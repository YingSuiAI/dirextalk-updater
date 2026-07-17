package updater

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const RuntimeStateSchemaVersion = 6

type DesiredState string

const (
	DesiredRunning       DesiredState = "running"
	DesiredUpgrading     DesiredState = "upgrading"
	DesiredMaintenance   DesiredState = "maintenance"
	DesiredDeprovisioned DesiredState = "deprovisioned"
)

func (state DesiredState) valid() bool {
	switch state {
	case DesiredRunning, DesiredUpgrading, DesiredMaintenance, DesiredDeprovisioned:
		return true
	default:
		return false
	}
}

type DiscoveryStatus string

const (
	DiscoveryUnknown     DiscoveryStatus = "unknown"
	DiscoveryFresh       DiscoveryStatus = "fresh"
	DiscoveryStale       DiscoveryStatus = "stale"
	DiscoveryUnavailable DiscoveryStatus = "unavailable"
)

type DiscoveryCache struct {
	Status         DiscoveryStatus `json:"status"`
	CheckedAt      time.Time       `json:"checked_at,omitempty"`
	Manifest       *Manifest       `json:"manifest,omitempty"`
	ManifestDigest string          `json:"manifest_digest,omitempty"`
	Index          *ReleaseIndex   `json:"release_index,omitempty"`
	IndexDigest    string          `json:"release_index_digest,omitempty"`
	ErrorCode      string          `json:"error_code,omitempty"`
}

type Plan struct {
	Manifest                  Manifest      `json:"manifest"`
	ManifestDigest            string        `json:"manifest_digest"`
	CurrentVersion            string        `json:"current_version"`
	ReleaseChain              []ReleaseStep `json:"release_chain,omitempty"`
	DirectContractVersion     int           `json:"direct_contract_version,omitempty"`
	ReleaseIndexDigest        string        `json:"release_index_digest,omitempty"`
	ClientVersion             string        `json:"client_version,omitempty"`
	SourceImageDigest         string        `json:"source_image_digest,omitempty"`
	SourceSchemaVersion       int           `json:"source_schema_version,omitempty"`
	SourceSchemaCompatVersion int           `json:"source_schema_compat_version,omitempty"`
	LegacyUnbound             bool          `json:"legacy_unbound,omitempty"`
	ExpiresAt                 time.Time     `json:"expires_at"`
}

func validatePlanReleaseChain(plan Plan) error {
	if len(plan.ReleaseChain) == 0 {
		return fmt.Errorf("release_chain is required")
	}
	current := plan.CurrentVersion
	for stepNumber, step := range plan.ReleaseChain {
		if err := step.Manifest.Validate(); err != nil {
			return fmt.Errorf("step %d manifest: %w", stepNumber, err)
		}
		if !digestPattern.MatchString(step.ManifestDigest) {
			return fmt.Errorf("step %d manifest_digest is invalid", stepNumber)
		}
		if canonicalManifestDigest(step.Manifest) != step.ManifestDigest {
			return fmt.Errorf("step %d manifest_digest mismatch", stepNumber)
		}
		if err := step.Manifest.ValidateUpgradeFrom(current); err != nil {
			return fmt.Errorf("step %d edge: %w", stepNumber, err)
		}
		if len(step.SourceImageDigests) == 0 && !plan.LegacyUnbound {
			return fmt.Errorf("step %d source image digests are required", stepNumber)
		}
		if len(step.SourceImageDigests) > 0 {
			if !sort.StringsAreSorted(step.SourceImageDigests) {
				return fmt.Errorf("step %d source digests are not sorted", stepNumber)
			}
			for digestNumber, digest := range step.SourceImageDigests {
				if !digestPattern.MatchString(digest) || (digestNumber > 0 && digest == step.SourceImageDigests[digestNumber-1]) {
					return fmt.Errorf("step %d source digest is invalid or duplicated", stepNumber)
				}
			}
		}
		current = step.Manifest.Version
	}
	target := plan.ReleaseChain[len(plan.ReleaseChain)-1]
	if !reflect.DeepEqual(target.Manifest, plan.Manifest) || target.ManifestDigest != plan.ManifestDigest {
		return fmt.Errorf("legacy target fields do not match the final chain step")
	}
	if plan.DirectContractVersion == 0 {
		return nil
	}
	if plan.DirectContractVersion != DirectContractVersion {
		return fmt.Errorf("direct_contract_version %d is not supported", plan.DirectContractVersion)
	}
	if len(plan.ReleaseChain) != 1 {
		return fmt.Errorf("direct contract requires exactly one release step")
	}
	if !digestPattern.MatchString(plan.ReleaseIndexDigest) {
		return fmt.Errorf("direct contract release_index_digest is invalid")
	}
	if !digestPattern.MatchString(plan.SourceImageDigest) || !digestAllowed(plan.SourceImageDigest, plan.ReleaseChain[0].SourceImageDigests) {
		return fmt.Errorf("direct contract source image digest is not bound to the release edge")
	}
	source := DirectSource{
		Version:             plan.CurrentVersion,
		ImageDigest:         plan.SourceImageDigest,
		SchemaVersion:       plan.SourceSchemaVersion,
		SchemaCompatVersion: plan.SourceSchemaCompatVersion,
	}
	if err := validateSchemaCompatibility(source, plan.Manifest); err != nil {
		return fmt.Errorf("direct contract schema: %w", err)
	}
	if err := validateClientCompatibility(plan.ClientVersion, plan.Manifest); err != nil {
		return fmt.Errorf("direct contract client: %w", err)
	}
	return nil
}

type JobStatus string

const (
	JobQueued      JobStatus = "queued"
	JobValidating  JobStatus = "validating"
	JobBackingUp   JobStatus = "backing_up"
	JobPulling     JobStatus = "pulling"
	JobStopping    JobStatus = "stopping"
	JobMigrating   JobStatus = "migrating"
	JobStarting    JobStatus = "starting"
	JobHealthCheck JobStatus = "health_check"
	JobRollingBack JobStatus = "rolling_back"
	JobRestarting  JobStatus = "restarting"
	JobSucceeded   JobStatus = "succeeded"
	JobFailed      JobStatus = "failed"
	JobRolledBack  JobStatus = "rolled_back"
)

type Job struct {
	ID                string          `json:"id"`
	Status            JobStatus       `json:"status"`
	PlanTokenHash     string          `json:"plan_token_hash,omitempty"`
	ManifestDigest    string          `json:"manifest_digest,omitempty"`
	DirectRelease     *DirectRelease  `json:"direct_release,omitempty"`
	BearerTokenHashes []string        `json:"bearer_token_hashes"`
	IdempotencyKey    string          `json:"idempotency_key"`
	CurrentVersion    string          `json:"current_version,omitempty"`
	TargetVersion     string          `json:"target_version"`
	CurrentStep       JobStep         `json:"current_step,omitempty"`
	CompletedSteps    int             `json:"completed_steps,omitempty"`
	TotalSteps        int             `json:"total_steps,omitempty"`
	CurrentHop        int             `json:"current_hop,omitempty"`
	TotalHops         int             `json:"total_hops,omitempty"`
	ServiceAvailable  bool            `json:"service_available"`
	LastSafeVersion   string          `json:"last_safe_version,omitempty"`
	ErrorCode         string          `json:"error_code,omitempty"`
	ErrorMessage      string          `json:"error_message,omitempty"`
	RecoveryPoint     *BackupMetadata `json:"recovery_point,omitempty"`
	RecoveryAttempts  int             `json:"recovery_attempts,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type WatchdogStatus string

const (
	WatchdogUnknown    WatchdogStatus = "unknown"
	WatchdogHealthy    WatchdogStatus = "healthy"
	WatchdogObserving  WatchdogStatus = "observing"
	WatchdogRepairing  WatchdogStatus = "repairing"
	WatchdogDegraded   WatchdogStatus = "degraded"
	WatchdogSuppressed WatchdogStatus = "suppressed"
)

func (status WatchdogStatus) valid() bool {
	switch status {
	case WatchdogUnknown, WatchdogHealthy, WatchdogObserving, WatchdogRepairing, WatchdogDegraded, WatchdogSuppressed:
		return true
	default:
		return false
	}
}

type WatchdogState struct {
	Status              WatchdogStatus `json:"status"`
	ConsecutiveFailures int            `json:"consecutive_failures,omitempty"`
	Attempts            []time.Time    `json:"attempts,omitempty"`
	CooldownUntil       time.Time      `json:"cooldown_until,omitempty"`
	LastObservedAt      time.Time      `json:"last_observed_at,omitempty"`
	LastRepairAt        time.Time      `json:"last_repair_at,omitempty"`
	ErrorCode           string         `json:"error_code,omitempty"`
}

type RuntimeState struct {
	SchemaVersion int               `json:"schema_version"`
	DesiredState  DesiredState      `json:"desired_state"`
	Discovery     DiscoveryCache    `json:"discovery"`
	Watchdog      WatchdogState     `json:"watchdog"`
	Plans         map[string]Plan   `json:"plans,omitempty"`
	Jobs          map[string]Job    `json:"jobs,omitempty"`
	Idempotency   map[string]string `json:"idempotency,omitempty"`
}

func NewRuntimeState() RuntimeState {
	return RuntimeState{
		SchemaVersion: RuntimeStateSchemaVersion,
		DesiredState:  DesiredRunning,
		Discovery:     DiscoveryCache{Status: DiscoveryUnknown},
		Watchdog:      WatchdogState{Status: WatchdogUnknown},
		Plans:         map[string]Plan{},
		Jobs:          map[string]Job{},
		Idempotency:   map[string]string{},
	}
}

func (state *RuntimeState) normalizeAndValidate() error {
	if state.SchemaVersion != RuntimeStateSchemaVersion {
		return fmt.Errorf("state schema_version %d is not supported", state.SchemaVersion)
	}
	if !state.DesiredState.valid() {
		return fmt.Errorf("desired_state %q is invalid", state.DesiredState)
	}
	if !state.Watchdog.Status.valid() {
		return fmt.Errorf("watchdog status %q is invalid", state.Watchdog.Status)
	}
	if state.Watchdog.ConsecutiveFailures < 0 || state.Watchdog.ConsecutiveFailures > watchdogObservationThreshold {
		return fmt.Errorf("watchdog consecutive_failures is invalid")
	}
	if len(state.Watchdog.Attempts) > watchdogMaxAttempts {
		return fmt.Errorf("watchdog attempts exceed the fixed budget")
	}
	for index, attempt := range state.Watchdog.Attempts {
		if attempt.IsZero() || (index > 0 && attempt.Before(state.Watchdog.Attempts[index-1])) {
			return fmt.Errorf("watchdog attempts are invalid")
		}
	}
	switch state.Watchdog.ErrorCode {
	case "", "observation_failed", "repair_failed":
	default:
		return fmt.Errorf("watchdog error_code is invalid")
	}
	if state.Watchdog.Status == WatchdogDegraded && state.Watchdog.CooldownUntil.IsZero() {
		return fmt.Errorf("degraded watchdog requires cooldown_until")
	}
	if state.Plans == nil {
		state.Plans = map[string]Plan{}
	}
	if state.Jobs == nil {
		state.Jobs = map[string]Job{}
	}
	if state.Idempotency == nil {
		state.Idempotency = map[string]string{}
	}
	switch state.Discovery.Status {
	case DiscoveryUnknown, DiscoveryFresh, DiscoveryStale, DiscoveryUnavailable:
	default:
		return fmt.Errorf("discovery status %q is invalid", state.Discovery.Status)
	}
	if state.Discovery.Manifest != nil {
		if err := state.Discovery.Manifest.Validate(); err != nil {
			return fmt.Errorf("discovery manifest is invalid: %w", err)
		}
		if !digestPattern.MatchString(state.Discovery.ManifestDigest) {
			return fmt.Errorf("discovery manifest_digest is invalid")
		}
		if state.Discovery.Index != nil {
			if err := state.Discovery.Index.Validate(); err != nil {
				return fmt.Errorf("discovery release index is invalid: %w", err)
			}
			if !digestPattern.MatchString(state.Discovery.IndexDigest) {
				return fmt.Errorf("discovery release_index_digest is invalid")
			}
			if canonicalReleaseIndexDigest(*state.Discovery.Index) != state.Discovery.IndexDigest {
				return fmt.Errorf("discovery release_index_digest mismatch")
			}
			latest := state.Discovery.Index.Releases[len(state.Discovery.Index.Releases)-1]
			if !reflect.DeepEqual(latest.Manifest, *state.Discovery.Manifest) || latest.ManifestDigest != state.Discovery.ManifestDigest {
				return fmt.Errorf("discovery latest manifest does not match release index")
			}
		}
	} else if state.Discovery.Status == DiscoveryFresh {
		return fmt.Errorf("fresh discovery requires a manifest")
	}
	if state.Discovery.Status == DiscoveryFresh && state.Discovery.CheckedAt.IsZero() {
		return fmt.Errorf("fresh discovery requires checked_at")
	}
	if state.Discovery.Status == DiscoveryFresh && state.Discovery.Index == nil {
		return fmt.Errorf("fresh discovery requires a release index")
	}
	for planHash, plan := range state.Plans {
		if !storedTokenHashValid(planHash) {
			return fmt.Errorf("plan token hash is invalid")
		}
		if err := plan.Manifest.Validate(); err != nil {
			return fmt.Errorf("plan manifest is invalid: %w", err)
		}
		if !digestPattern.MatchString(plan.ManifestDigest) {
			return fmt.Errorf("plan manifest_digest is invalid")
		}
		if err := validatePlanReleaseChain(plan); err != nil {
			return fmt.Errorf("plan release chain is invalid: %w", err)
		}
		if plan.ExpiresAt.IsZero() {
			return fmt.Errorf("plan expiry is required")
		}
	}
	for jobID, job := range state.Jobs {
		if job.ID == "" || job.ID != jobID {
			return fmt.Errorf("job id is invalid")
		}
		if !job.Status.valid() {
			return fmt.Errorf("job %s status %q is invalid", jobID, job.Status)
		}
		if job.DirectRelease != nil {
			if err := job.DirectRelease.Validate(); err != nil {
				return fmt.Errorf("job %s direct release is invalid: %w", jobID, err)
			}
			if job.PlanTokenHash != "" || job.ManifestDigest != "" {
				return fmt.Errorf("job %s direct release must not reference a plan", jobID)
			}
			if job.TargetVersion != job.DirectRelease.Version || job.TotalHops != 1 || job.CurrentHop < 0 || job.CurrentHop > 1 {
				return fmt.Errorf("job %s direct release progress is invalid", jobID)
			}
			current, currentErr := parseCanonicalVersion("current_version", job.CurrentVersion)
			target, targetErr := parseCanonicalVersion("target_version", job.TargetVersion)
			if currentErr != nil || targetErr != nil || current.GreaterThan(target) {
				return fmt.Errorf("job %s direct release versions are invalid", jobID)
			}
		} else {
			if _, ok := state.Plans[job.PlanTokenHash]; !ok {
				return fmt.Errorf("job %s references an unknown plan", jobID)
			}
			plan := state.Plans[job.PlanTokenHash]
			if job.ManifestDigest != plan.ManifestDigest || job.TargetVersion != plan.Manifest.Version || job.TotalHops != len(plan.ReleaseChain) {
				return fmt.Errorf("job %s target does not match its plan", jobID)
			}
			if job.CurrentHop < 0 || job.CurrentHop > len(plan.ReleaseChain) {
				return fmt.Errorf("job %s current hop is invalid", jobID)
			}
			expectedCurrent := plan.CurrentVersion
			if job.CurrentHop > 0 {
				expectedCurrent = plan.ReleaseChain[job.CurrentHop-1].Manifest.Version
			}
			if job.CurrentVersion != expectedCurrent {
				return fmt.Errorf("job %s current version does not match its completed hop", jobID)
			}
		}
		if job.IdempotencyKey == "" || state.Idempotency[job.IdempotencyKey] != jobID {
			return fmt.Errorf("job %s idempotency mapping is invalid", jobID)
		}
		if len(job.BearerTokenHashes) == 0 {
			return fmt.Errorf("job %s has no bearer token hash", jobID)
		}
		for _, bearerHash := range job.BearerTokenHashes {
			if !storedTokenHashValid(bearerHash) {
				return fmt.Errorf("job %s bearer token hash is invalid", jobID)
			}
		}
		if err := job.validateExecutionState(); err != nil {
			return fmt.Errorf("job %s execution state is invalid: %w", jobID, err)
		}
	}
	for idempotencyKey, jobID := range state.Idempotency {
		job, ok := state.Jobs[jobID]
		if idempotencyKey == "" || !ok || job.IdempotencyKey != idempotencyKey {
			return fmt.Errorf("idempotency mapping is invalid")
		}
	}
	activeJobs := countActiveJobs(*state)
	if activeJobs > 1 {
		return fmt.Errorf("more than one active job is not allowed")
	}
	activeJob := activeJobs == 1
	if state.DesiredState == DesiredUpgrading && !activeJob {
		return fmt.Errorf("desired_state upgrading requires an active job")
	}
	if activeJob && state.DesiredState != DesiredUpgrading {
		return fmt.Errorf("an active job requires desired_state upgrading")
	}
	return nil
}

func storedTokenHashValid(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

type StateStore struct {
	path          string
	syncDirectory func(string) error
	mu            sync.Mutex
}

func NewStateStore(path string) *StateStore {
	return &StateStore{path: path, syncDirectory: syncDirectory}
}

func (store *StateStore) Path() string {
	return store.path
}

func (store *StateStore) Load(ctx context.Context) (RuntimeState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.loadLocked(ctx)
}

func (store *StateStore) loadLocked(ctx context.Context) (RuntimeState, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeState{}, err
	}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return NewRuntimeState(), nil
	}
	if err != nil {
		return RuntimeState{}, fmt.Errorf("read updater state: %w", err)
	}
	var state RuntimeState
	if err := decodeStrict(data, &state, "updater state"); err != nil {
		return RuntimeState{}, err
	}
	if err := migrateRuntimeState(&state); err != nil {
		return RuntimeState{}, err
	}
	if err := state.normalizeAndValidate(); err != nil {
		return RuntimeState{}, err
	}
	return state, nil
}

func migrateRuntimeState(state *RuntimeState) error {
	switch state.SchemaVersion {
	case RuntimeStateSchemaVersion:
		return nil
	case 1:
		for jobID, job := range state.Jobs {
			plan, ok := state.Plans[job.PlanTokenHash]
			if !ok || plan.CurrentVersion == "" || plan.ManifestDigest == "" {
				return fmt.Errorf("cannot migrate schema 1 job %s without a bound version edge", jobID)
			}
			if job.CurrentVersion == "" {
				job.CurrentVersion = plan.CurrentVersion
			}
			if job.ManifestDigest == "" {
				job.ManifestDigest = plan.ManifestDigest
			}
			state.Jobs[jobID] = job
		}
		if hasActiveJob(*state) {
			state.DesiredState = DesiredUpgrading
		}
		state.SchemaVersion = 2
		fallthrough
	case 2:
		for jobID, job := range state.Jobs {
			if job.Status == JobQueued {
				job.CurrentStep = JobStepValidate
				job.TotalSteps = executionTotalSteps
				job.ServiceAvailable = true
				job.LastSafeVersion = job.CurrentVersion
				state.Jobs[jobID] = job
			}
		}
		state.SchemaVersion = 3
		fallthrough
	case 3:
		state.Watchdog = WatchdogState{Status: WatchdogUnknown}
		state.SchemaVersion = 4
		fallthrough
	case 4:
		for planHash, plan := range state.Plans {
			if len(plan.ReleaseChain) == 0 {
				plan.ManifestDigest = canonicalManifestDigest(plan.Manifest)
				plan.ReleaseChain = []ReleaseStep{{Manifest: plan.Manifest, ManifestDigest: plan.ManifestDigest}}
				plan.LegacyUnbound = true
				state.Plans[planHash] = plan
			}
		}
		for jobID, job := range state.Jobs {
			plan := state.Plans[job.PlanTokenHash]
			job.ManifestDigest = plan.ManifestDigest
			if job.TotalHops == 0 {
				job.TotalHops = len(plan.ReleaseChain)
			}
			if job.TotalSteps == executionTotalSteps && job.TotalHops > 1 {
				job.TotalSteps = executionTotalSteps * job.TotalHops
			}
			if job.Status == JobSucceeded {
				job.CurrentHop = job.TotalHops
				job.CurrentVersion = job.TargetVersion
				job.CompletedSteps = job.TotalSteps
			} else if job.Status == JobRolledBack {
				job.CurrentHop = completedHopForVersion(plan, job.LastSafeVersion)
				job.CurrentVersion = job.LastSafeVersion
				job.CompletedSteps = job.CurrentHop * executionTotalSteps
			}
			state.Jobs[jobID] = job
		}
		if state.Discovery.Status == DiscoveryFresh && state.Discovery.Index == nil {
			state.Discovery.Status = DiscoveryStale
			state.Discovery.ErrorCode = "release_index_refresh_required"
		}
		state.SchemaVersion = 5
		fallthrough
	case 5:
		// Discovery and unconsumed plans are no longer executable. Keep plans
		// referenced by persisted jobs so an interrupted legacy job can still
		// finish its automatic recovery after the updater binary is replaced.
		referencedPlans := make(map[string]struct{}, len(state.Jobs))
		for _, job := range state.Jobs {
			if job.PlanTokenHash != "" {
				referencedPlans[job.PlanTokenHash] = struct{}{}
			}
		}
		for planHash := range state.Plans {
			if _, referenced := referencedPlans[planHash]; !referenced {
				delete(state.Plans, planHash)
			}
		}
		state.Discovery = DiscoveryCache{Status: DiscoveryUnknown}
		state.SchemaVersion = RuntimeStateSchemaVersion
		return nil
	default:
		return fmt.Errorf("state schema_version %d is not supported", state.SchemaVersion)
	}
}

func (store *StateStore) Save(ctx context.Context, state RuntimeState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveLocked(ctx, state)
}

// Update serializes a complete read-modify-write transaction. Callers must not
// perform external I/O in mutate because the state lock remains held until the
// updated snapshot has been durably committed or the write has failed.
func (store *StateStore) Update(ctx context.Context, mutate func(*RuntimeState) error) error {
	if mutate == nil {
		return fmt.Errorf("state mutation is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.loadLocked(ctx)
	if err != nil {
		return err
	}
	if err := mutate(&state); err != nil {
		return err
	}
	return store.saveLocked(ctx, state)
}

func (store *StateStore) saveLocked(ctx context.Context, state RuntimeState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := state.normalizeAndValidate(); err != nil {
		return err
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("encode updater state: %w", err)
	}
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create updater state directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("protect updater state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create updater state temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	keepTemporary := false
	defer func() {
		if !keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect updater state temp file: %w", err)
	}
	if _, err := temporary.Write(buffer.Bytes()); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write updater state temp file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("fsync updater state temp file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close updater state temp file: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace updater state: %w", err)
	}
	keepTemporary = true
	if err := store.syncDirectory(directory); err != nil {
		return fmt.Errorf("fsync updater state directory: %w", err)
	}
	return nil
}
