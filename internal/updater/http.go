package updater

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"
)

const (
	apiPrefix               = "/_dirextalk/updater/v1/"
	controlJobsPath         = apiPrefix + "control/jobs"
	controlDiscoveryPath    = apiPrefix + "control/discovery"
	controlStatusPath       = apiPrefix + "control/status"
	controlDesiredStatePath = apiPrefix + "control/desired-state"
	publicJobsPrefix        = apiPrefix + "jobs/"
	controlTokenHeader      = "X-Dirextalk-Control-Token"
	applyConfirmation       = "apply_release_change"
	maxRequestBytes         = 64 * 1024
)

type Service struct {
	store            *StateStore
	controlTokenHash string
	now              func() time.Time
	releaseSource    ReleaseSource
	jobEngine        *JobEngine
	jobSignal        chan struct{}
	logf             func(string, ...any)
	hostGate         *HostOperationGate
}

type ServiceOption func(*Service)

func WithReleaseSource(source ReleaseSource) ServiceOption {
	return func(service *Service) {
		service.releaseSource = source
	}
}

func WithJobEngine(engine *JobEngine) ServiceOption {
	return func(service *Service) {
		service.jobEngine = engine
	}
}

func WithLogger(logf func(string, ...any)) ServiceOption {
	return func(service *Service) {
		service.logf = logf
	}
}

func WithHostOperationGate(gate *HostOperationGate) ServiceOption {
	return func(service *Service) {
		if gate != nil {
			service.hostGate = gate
		}
	}
}

func NewService(store *StateStore, controlToken string, options ...ServiceOption) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if strings.TrimSpace(controlToken) == "" {
		return nil, fmt.Errorf("control token is required")
	}
	if _, err := store.Load(context.Background()); err != nil {
		return nil, err
	}
	service := &Service{
		store:            store,
		controlTokenHash: tokenHash(controlToken),
		now:              time.Now,
		jobSignal:        make(chan struct{}, 1),
		logf:             log.Printf,
		hostGate:         NewHostOperationGate(),
	}
	for _, option := range options {
		option(service)
	}
	if service.jobEngine != nil && service.jobEngine.store != store {
		return nil, fmt.Errorf("job engine must use the service state store")
	}
	if service.logf == nil {
		return nil, fmt.Errorf("service logger is required")
	}
	return service, nil
}

func (service *Service) RunJobs(ctx context.Context) {
	if service == nil || service.jobEngine == nil {
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := service.jobEngine.RunActive(ctx); err != nil && ctx.Err() == nil {
			service.logf("dirextalk updater job execution attempt failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-service.jobSignal:
		case <-ticker.C:
		}
	}
}

func (service *Service) wakeJobRunner() {
	if service.jobEngine == nil {
		return
	}
	select {
	case service.jobSignal <- struct{}{}:
	default:
	}
}

func (service *Service) Handler() http.Handler {
	return http.HandlerFunc(service.serveHTTP)
}

func (service *Service) RegisterPlan(ctx context.Context, rawToken string, plan Plan) error {
	if strings.TrimSpace(rawToken) == "" {
		return fmt.Errorf("plan token is required")
	}
	if plan.LegacyUnbound {
		return fmt.Errorf("legacy unbound plans cannot be registered")
	}
	if err := plan.Manifest.Validate(); err != nil {
		return err
	}
	if !digestPattern.MatchString(plan.ManifestDigest) {
		return fmt.Errorf("plan manifest_digest is invalid")
	}
	if len(plan.ReleaseChain) > 0 {
		if err := validatePlanReleaseChain(plan); err != nil {
			return err
		}
	} else if _, err := parseCanonicalVersion("current_version", plan.CurrentVersion); err != nil {
		return err
	}
	if !plan.ExpiresAt.After(service.now()) {
		return fmt.Errorf("plan expiry must be in the future")
	}
	return service.store.Update(ctx, func(state *RuntimeState) error {
		if effectiveDiscoveryStatus(state.Discovery, service.now()) != DiscoveryFresh || state.Discovery.Manifest == nil || state.Discovery.Index == nil {
			return fmt.Errorf("a fresh discovered release is required")
		}
		if state.DesiredState != DesiredRunning {
			return fmt.Errorf("desired state must be running to register a plan")
		}
		if hasActiveJob(*state) {
			return fmt.Errorf("an operation is already in progress")
		}
		discoveredChain, pathErr := state.Discovery.Index.UpgradePath(plan.CurrentVersion)
		if len(plan.ReleaseChain) == 0 {
			plan.ReleaseChain = discoveredChain
		}
		if pathErr != nil || !reflect.DeepEqual(discoveredChain, plan.ReleaseChain) || state.Discovery.ManifestDigest != plan.ManifestDigest || !reflect.DeepEqual(*state.Discovery.Manifest, plan.Manifest) {
			return fmt.Errorf("plan release chain does not match the discovered release index")
		}
		if err := validatePlanReleaseChain(plan); err != nil {
			return err
		}
		for _, step := range plan.ReleaseChain {
			if len(step.SourceImageDigests) == 0 {
				return fmt.Errorf("plan source image digests are required")
			}
		}
		planHash := tokenHash(rawToken)
		if existing, ok := state.Plans[planHash]; ok {
			if reflect.DeepEqual(existing, plan) {
				return nil
			}
			return fmt.Errorf("plan token is already bound to a different plan")
		}
		state.Plans[planHash] = plan
		return nil
	})
}

func (service *Service) serveHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.URL.Path == controlDiscoveryPath:
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.refreshDiscovery(response, request)
	case request.URL.Path == controlJobsPath:
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.createJob(response, request)
	case request.URL.Path == controlStatusPath:
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.getStatus(response, request)
	case request.URL.Path == controlDesiredStatePath:
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.setDesiredState(response, request)
	case strings.HasPrefix(request.URL.Path, publicJobsPrefix):
		service.servePublicJob(response, request)
	default:
		writeAPIError(response, http.StatusNotFound, "not_found")
	}
}

func (service *Service) refreshDiscovery(response http.ResponseWriter, request *http.Request) {
	if !service.controlAuthorized(request) {
		writeAPIError(response, http.StatusUnauthorized, "control_token_required")
		return
	}
	if service.releaseSource == nil {
		writeAPIError(response, http.StatusServiceUnavailable, "release_source_unavailable")
		return
	}
	var input struct{}
	if err := decodeControlRequest(response, request, &input, "discovery request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request: "+err.Error())
		return
	}
	cache, err := RefreshDiscovery(request.Context(), service.store, service.releaseSource, service.now())
	if err != nil {
		writeAPIError(response, http.StatusBadGateway, "release_discovery_failed")
		return
	}
	writeJSON(response, http.StatusOK, cache)
}

type createJobRequest struct {
	PlanToken      string `json:"plan_token"`
	IdempotencyKey string `json:"idempotency_key"`
	Confirm        string `json:"confirm"`
}

type JobTicket struct {
	JobID     string    `json:"job_id"`
	JobToken  string    `json:"job_token"`
	StatusURL string    `json:"status_url"`
	Status    JobStatus `json:"status"`
}

type publicJob struct {
	JobID            string         `json:"job_id"`
	Status           JobStatus      `json:"status"`
	CurrentVersion   string         `json:"current_version,omitempty"`
	TargetVersion    string         `json:"target_version"`
	CurrentStep      JobStep        `json:"current_step,omitempty"`
	CompletedSteps   int            `json:"completed_steps"`
	TotalSteps       int            `json:"total_steps"`
	CompletedHops    int            `json:"completed_hops"`
	TotalHops        int            `json:"total_hops"`
	ServiceAvailable bool           `json:"service_available"`
	LastSafeVersion  string         `json:"last_safe_version,omitempty"`
	ErrorCode        string         `json:"error_code,omitempty"`
	ErrorMessage     string         `json:"error_message,omitempty"`
	Operations       []JobOperation `json:"operations"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

type JobOperation struct {
	Kind string `json:"kind"`
}

func (service *Service) createJob(response http.ResponseWriter, request *http.Request) {
	if !service.controlAuthorized(request) {
		writeAPIError(response, http.StatusUnauthorized, "control_token_required")
		return
	}
	var input createJobRequest
	if err := decodeControlRequest(response, request, &input, "job request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request: "+err.Error())
		return
	}
	input.PlanToken = strings.TrimSpace(input.PlanToken)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.PlanToken == "" || input.IdempotencyKey == "" || len(input.IdempotencyKey) > 128 || input.Confirm != applyConfirmation {
		writeAPIError(response, http.StatusBadRequest, "invalid_request")
		return
	}

	planHash := tokenHash(input.PlanToken)
	snapshot, err := service.store.Load(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "state_read_failed")
		return
	}
	if existingID, exists := snapshot.Idempotency[input.IdempotencyKey]; exists {
		service.replayJob(response, request, existingID, planHash)
		return
	}
	if rejection := preflightNewJob(snapshot, planHash, service.now()); rejection != nil {
		writeAPIError(response, rejection.status, rejection.code)
		return
	}
	if request.Context().Err() != nil {
		return
	}
	releaseHostGate := service.hostGate.BeginMutation()
	defer releaseHostGate()
	if request.Context().Err() != nil {
		return
	}

	var ticket JobTicket
	var rejection *mutationRejection
	err = service.store.Update(request.Context(), func(state *RuntimeState) error {
		if existingID, exists := state.Idempotency[input.IdempotencyKey]; exists {
			job, jobExists := state.Jobs[existingID]
			if !jobExists {
				return rejectMutation(http.StatusInternalServerError, "state_inconsistent")
			}
			if job.PlanTokenHash != planHash {
				return rejectMutation(http.StatusConflict, "idempotency_conflict")
			}
			rawBearer, tokenErr := randomToken(32)
			if tokenErr != nil {
				return rejectMutation(http.StatusInternalServerError, "token_generation_failed")
			}
			job.BearerTokenHashes = append(job.BearerTokenHashes, tokenHash(rawBearer))
			job.UpdatedAt = service.now().UTC()
			state.Jobs[job.ID] = job
			ticket = JobTicket{JobID: job.ID, JobToken: rawBearer, StatusURL: publicJobPath(job.ID), Status: job.Status}
			return nil
		}
		plan, ok := state.Plans[planHash]
		if !ok || !plan.ExpiresAt.After(service.now()) {
			return rejectMutation(http.StatusConflict, "plan_invalid_or_expired")
		}
		if hasActiveJob(*state) {
			return rejectMutation(http.StatusConflict, "operation_in_progress")
		}
		for _, existing := range state.Jobs {
			if existing.PlanTokenHash == planHash {
				return rejectMutation(http.StatusConflict, "plan_already_used")
			}
		}
		if state.DesiredState != DesiredRunning {
			return rejectMutation(http.StatusConflict, "desired_state_not_running")
		}

		jobIDToken, tokenErr := randomToken(18)
		if tokenErr != nil {
			return rejectMutation(http.StatusInternalServerError, "token_generation_failed")
		}
		rawBearer, tokenErr := randomToken(32)
		if tokenErr != nil {
			return rejectMutation(http.StatusInternalServerError, "token_generation_failed")
		}
		now := service.now().UTC()
		job := Job{
			ID:                "job_" + jobIDToken,
			Status:            JobQueued,
			PlanTokenHash:     planHash,
			ManifestDigest:    plan.ManifestDigest,
			BearerTokenHashes: []string{tokenHash(rawBearer)},
			IdempotencyKey:    input.IdempotencyKey,
			CurrentVersion:    plan.CurrentVersion,
			TargetVersion:     plan.Manifest.Version,
			CurrentStep:       JobStepValidate,
			TotalSteps:        executionTotalSteps * len(plan.ReleaseChain),
			TotalHops:         len(plan.ReleaseChain),
			ServiceAvailable:  true,
			LastSafeVersion:   plan.CurrentVersion,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		state.Jobs[job.ID] = job
		state.Idempotency[input.IdempotencyKey] = job.ID
		state.DesiredState = DesiredUpgrading
		suppressWatchdog(state)
		ticket = JobTicket{JobID: job.ID, JobToken: rawBearer, StatusURL: publicJobPath(job.ID), Status: job.Status}
		return nil
	})
	if err != nil {
		if errors.As(err, &rejection) {
			writeAPIError(response, rejection.status, rejection.code)
		} else {
			writeAPIError(response, http.StatusInternalServerError, "state_write_failed")
		}
		return
	}
	service.wakeJobRunner()
	writeJSON(response, http.StatusAccepted, ticket)
}

func (service *Service) replayJob(response http.ResponseWriter, request *http.Request, existingID, planHash string) {
	var ticket JobTicket
	var rejection *mutationRejection
	err := service.store.Update(request.Context(), func(state *RuntimeState) error {
		job, exists := state.Jobs[existingID]
		if !exists {
			return rejectMutation(http.StatusInternalServerError, "state_inconsistent")
		}
		if job.PlanTokenHash != planHash {
			return rejectMutation(http.StatusConflict, "idempotency_conflict")
		}
		rawBearer, tokenErr := randomToken(32)
		if tokenErr != nil {
			return rejectMutation(http.StatusInternalServerError, "token_generation_failed")
		}
		job.BearerTokenHashes = append(job.BearerTokenHashes, tokenHash(rawBearer))
		job.UpdatedAt = service.now().UTC()
		state.Jobs[job.ID] = job
		ticket = JobTicket{JobID: job.ID, JobToken: rawBearer, StatusURL: publicJobPath(job.ID), Status: job.Status}
		return nil
	})
	if err != nil {
		if errors.As(err, &rejection) {
			writeAPIError(response, rejection.status, rejection.code)
		} else {
			writeAPIError(response, http.StatusInternalServerError, "state_write_failed")
		}
		return
	}
	writeJSON(response, http.StatusAccepted, ticket)
}

func preflightNewJob(state RuntimeState, planHash string, now time.Time) *mutationRejection {
	plan, ok := state.Plans[planHash]
	if !ok || !plan.ExpiresAt.After(now) {
		return &mutationRejection{status: http.StatusConflict, code: "plan_invalid_or_expired"}
	}
	if hasActiveJob(state) {
		return &mutationRejection{status: http.StatusConflict, code: "operation_in_progress"}
	}
	for _, existing := range state.Jobs {
		if existing.PlanTokenHash == planHash {
			return &mutationRejection{status: http.StatusConflict, code: "plan_already_used"}
		}
	}
	if state.DesiredState != DesiredRunning {
		return &mutationRejection{status: http.StatusConflict, code: "desired_state_not_running"}
	}
	return nil
}

func (service *Service) controlAuthorized(request *http.Request) bool {
	return constantTokenEqual(service.controlTokenHash, request.Header.Get(controlTokenHeader))
}

func decodeControlRequest(response http.ResponseWriter, request *http.Request, target any, subject string) error {
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder, subject)
}

func (service *Service) getJob(response http.ResponseWriter, request *http.Request, jobID string) {
	state, err := service.store.Load(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "state_read_failed")
		return
	}
	job, ok := state.Jobs[jobID]
	if !ok {
		writeAPIError(response, http.StatusNotFound, "job_not_found")
		return
	}
	bearer := bearerToken(request.Header.Get("Authorization"))
	if !jobTokenAllowed(job, bearer) {
		writeAPIError(response, http.StatusUnauthorized, "job_token_required")
		return
	}
	writeJSON(response, http.StatusOK, publicJobView(job))
}

func publicJobView(job Job) publicJob {
	return publicJob{
		JobID:            job.ID,
		Status:           job.Status,
		CurrentVersion:   job.CurrentVersion,
		TargetVersion:    job.TargetVersion,
		CurrentStep:      job.CurrentStep,
		CompletedSteps:   job.CompletedSteps,
		TotalSteps:       job.TotalSteps,
		CompletedHops:    job.CurrentHop,
		TotalHops:        job.TotalHops,
		ServiceAvailable: job.ServiceAvailable,
		LastSafeVersion:  job.LastSafeVersion,
		ErrorCode:        job.ErrorCode,
		ErrorMessage:     job.ErrorMessage,
		Operations:       publicJobOperations(job),
		CreatedAt:        job.CreatedAt,
		UpdatedAt:        job.UpdatedAt,
	}
}

func publicJobOperations(job Job) []JobOperation {
	operations := []JobOperation{}
	if (job.Status == JobSucceeded || job.Status == JobFailed) && job.RecoveryPoint != nil {
		operations = append(operations, JobOperation{Kind: "rollback"})
	}
	if job.Status == JobFailed && !job.ServiceAvailable {
		operations = append(operations, JobOperation{Kind: "restart"})
	}
	return operations
}

func (service *Service) servePublicJob(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	response.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	response.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type")
	if request.Method == http.MethodOptions {
		response.WriteHeader(http.StatusNoContent)
		return
	}
	remainder := strings.TrimPrefix(request.URL.Path, publicJobsPrefix)
	parts := strings.Split(remainder, "/")
	if len(parts) == 1 && parts[0] != "" {
		if request.Method != http.MethodGet {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.getJob(response, request, parts[0])
		return
	}
	if len(parts) == 2 && parts[0] != "" && (parts[1] == "rollback" || parts[1] == "restart") {
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.startJobOperation(response, request, parts[0], parts[1])
		return
	}
	writeAPIError(response, http.StatusNotFound, "job_not_found")
}

func (service *Service) startJobOperation(response http.ResponseWriter, request *http.Request, jobID, kind string) {
	var input struct{}
	if err := decodeControlRequest(response, request, &input, "job operation request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	bearer := bearerToken(request.Header.Get("Authorization"))
	snapshot, err := service.store.Load(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "state_read_failed")
		return
	}
	snapshotJob, ok := snapshot.Jobs[jobID]
	if !ok {
		writeAPIError(response, http.StatusNotFound, "job_not_found")
		return
	}
	if !jobTokenAllowed(snapshotJob, bearer) {
		writeAPIError(response, http.StatusUnauthorized, "job_token_required")
		return
	}
	operationAllowed := false
	for _, operation := range publicJobOperations(snapshotJob) {
		if operation.Kind == kind {
			operationAllowed = true
			break
		}
	}
	if !operationAllowed {
		writeAPIError(response, http.StatusConflict, "operation_not_available")
		return
	}
	releaseHostGate := service.hostGate.BeginMutation()
	defer releaseHostGate()
	var updated Job
	var rejection *mutationRejection
	err = service.store.Update(request.Context(), func(state *RuntimeState) error {
		job, ok := state.Jobs[jobID]
		if !ok {
			return rejectMutation(http.StatusNotFound, "job_not_found")
		}
		if !jobTokenAllowed(job, bearer) {
			return rejectMutation(http.StatusUnauthorized, "job_token_required")
		}
		allowed := false
		for _, operation := range publicJobOperations(job) {
			if operation.Kind == kind {
				allowed = true
				break
			}
		}
		if !allowed {
			return rejectMutation(http.StatusConflict, "operation_not_available")
		}
		state.DesiredState = DesiredUpgrading
		suppressWatchdog(state)
		job.ServiceAvailable = false
		job.RecoveryAttempts = 0
		switch kind {
		case "rollback":
			job.Status = JobRollingBack
			job.CurrentStep = JobStepRestoreBackup
		case "restart":
			job.Status = JobRestarting
			job.CurrentStep = JobStepRestart
		}
		job.UpdatedAt = service.now().UTC()
		state.Jobs[jobID] = job
		updated = job
		return nil
	})
	if err != nil {
		if errors.As(err, &rejection) {
			writeAPIError(response, rejection.status, rejection.code)
		} else {
			writeAPIError(response, http.StatusInternalServerError, "state_write_failed")
		}
		return
	}
	service.wakeJobRunner()
	writeJSON(response, http.StatusAccepted, publicJobView(updated))
}

type mutationRejection struct {
	status int
	code   string
}

func (rejection *mutationRejection) Error() string { return rejection.code }

func rejectMutation(status int, code string) error {
	return &mutationRejection{status: status, code: code}
}

func publicJobPath(jobID string) string {
	return publicJobsPrefix + jobID
}

func tokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func constantTokenEqual(expectedHash, candidate string) bool {
	candidateHash := tokenHash(candidate)
	return subtle.ConstantTimeCompare([]byte(expectedHash), []byte(candidateHash)) == 1
}

func jobTokenAllowed(job Job, rawToken string) bool {
	if rawToken == "" {
		return false
	}
	candidateHash := tokenHash(rawToken)
	for _, expectedHash := range job.BearerTokenHashes {
		if subtle.ConstantTimeCompare([]byte(expectedHash), []byte(candidateHash)) == 1 {
			return true
		}
	}
	return false
}

func randomToken(size int) (string, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func bearerToken(header string) string {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func writeAPIError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
