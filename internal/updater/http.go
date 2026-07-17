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
	"regexp"
	"strings"
	"time"
)

const (
	apiPrefix               = "/_dirextalk/updater/v1/"
	controlJobsPath         = apiPrefix + "control/jobs"
	controlJobsReplayPath   = controlJobsPath + "/replay"
	controlStatusPath       = apiPrefix + "control/status"
	controlDesiredStatePath = apiPrefix + "control/desired-state"
	publicJobsPrefix        = apiPrefix + "jobs/"
	controlTokenHeader      = "X-Dirextalk-Control-Token"
	applyConfirmation       = "apply_release_change"
	maxRequestBytes         = 64 * 1024
	directPlanLifetime      = 15 * time.Minute
)

var canonicalUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type Service struct {
	store            *StateStore
	controlTokenHash string
	now              func() time.Time
	directRuntime    DirectJobRuntime
	releaseSource    ReleaseSource
	jobEngine        *JobEngine
	jobSignal        chan struct{}
	logf             func(string, ...any)
	hostGate         *HostOperationGate
}

type ServiceOption func(*Service)

// WithDirectJobRuntime connects host-owned source inspection to the control
// plane. Target trust is supplied separately by the fixed ReleaseSource.
func WithDirectJobRuntime(runtime DirectJobRuntime) ServiceOption {
	return func(service *Service) {
		service.directRuntime = runtime
	}
}

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

func (service *Service) serveHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.URL.Path == controlJobsReplayPath:
		if request.Method != http.MethodPost {
			writeAPIError(response, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		service.replayJobTicket(response, request)
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

type createJobRequest struct {
	TargetVersion  string `json:"target_version"`
	IdempotencyKey string `json:"idempotency_key"`
	ClientVersion  string `json:"client_version"`
	Confirm        string `json:"confirm"`
}

type replayJobRequest struct {
	TargetVersion  string `json:"target_version"`
	IdempotencyKey string `json:"idempotency_key"`
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
	input.TargetVersion = strings.TrimSpace(input.TargetVersion)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	clientVersion, clientErr := normalizeRequiredClientVersion(input.ClientVersion)
	if _, err := parseCanonicalVersion("target_version", input.TargetVersion); err != nil || !canonicalUUIDPattern.MatchString(input.IdempotencyKey) || clientErr != nil || input.Confirm != applyConfirmation {
		writeAPIError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	snapshot, err := service.store.Load(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "state_read_failed")
		return
	}
	if _, exists := snapshot.Idempotency[input.IdempotencyKey]; exists {
		service.rotateJobTicket(response, request, replayJobRequest{TargetVersion: input.TargetVersion, IdempotencyKey: input.IdempotencyKey}, http.StatusAccepted)
		return
	}
	if service.directRuntime == nil || service.releaseSource == nil {
		writeAPIError(response, http.StatusServiceUnavailable, "updater_not_ready")
		return
	}
	if rejection := preflightDirectJob(snapshot); rejection != nil {
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
	currentVersion, currentErr := service.directRuntime.CurrentVersion(request.Context())
	if currentErr != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "runtime_status_unavailable")
		return
	}
	current, currentParseErr := parseCanonicalVersion("current_version", currentVersion)
	target, _ := parseCanonicalVersion("target_version", input.TargetVersion)
	if currentParseErr != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "runtime_status_unavailable")
		return
	}
	if !current.LessThan(target) {
		writeAPIError(response, http.StatusConflict, "target_not_newer")
		return
	}
	indexData, resolveErr := service.releaseSource.Latest(request.Context())
	if resolveErr != nil {
		writeAPIError(response, http.StatusBadGateway, "release_resolution_failed")
		return
	}
	index, indexErr := ValidateReleaseIndex(indexData)
	if indexErr != nil {
		writeAPIError(response, http.StatusBadGateway, "release_index_invalid")
		return
	}
	step, edgeErr := index.DirectUpgradeStep(currentVersion, input.TargetVersion)
	if edgeErr != nil {
		writeAPIError(response, http.StatusConflict, "upgrade_edge_unsupported")
		return
	}
	source, sourceErr := service.directRuntime.InspectDirectSource(request.Context(), currentVersion, step)
	if sourceErr != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "source_verification_failed")
		return
	}
	if source.Validate() != nil || source.Version != currentVersion || !digestAllowed(source.ImageDigest, step.SourceImageDigests) {
		writeAPIError(response, http.StatusConflict, "source_image_digest_untrusted")
		return
	}
	if indexedSource, formal := index.releaseForVersion(currentVersion); formal {
		if indexedSource.Manifest.ImageDigest != source.ImageDigest || indexedSource.Manifest.SchemaVersion != source.SchemaVersion || indexedSource.Manifest.SchemaCompatVersion != source.SchemaCompatVersion {
			writeAPIError(response, http.StatusConflict, "source_contract_mismatch")
			return
		}
	}
	if err := validateSchemaCompatibility(source, step.Manifest); err != nil {
		writeAPIError(response, http.StatusConflict, "schema_incompatible")
		return
	}
	if err := validateClientCompatibility(clientVersion, step.Manifest); err != nil {
		writeAPIError(response, http.StatusConflict, "client_version_incompatible")
		return
	}
	now := service.now().UTC()
	plan := Plan{
		Manifest:                  step.Manifest,
		ManifestDigest:            step.ManifestDigest,
		CurrentVersion:            currentVersion,
		ReleaseChain:              []ReleaseStep{step},
		DirectContractVersion:     DirectContractVersion,
		ReleaseIndexDigest:        releaseIndexDigest(indexData),
		ClientVersion:             clientVersion,
		SourceImageDigest:         source.ImageDigest,
		SourceSchemaVersion:       source.SchemaVersion,
		SourceSchemaCompatVersion: source.SchemaCompatVersion,
		ExpiresAt:                 now.Add(directPlanLifetime),
	}
	if err := validatePlanReleaseChain(plan); err != nil {
		writeAPIError(response, http.StatusConflict, "release_contract_invalid")
		return
	}
	rawPlanToken, tokenErr := randomToken(32)
	if tokenErr != nil {
		writeAPIError(response, http.StatusInternalServerError, "token_generation_failed")
		return
	}
	planHash := tokenHash(rawPlanToken)

	var ticket JobTicket
	var rejection *mutationRejection
	err = service.store.Update(request.Context(), func(state *RuntimeState) error {
		if _, exists := state.Idempotency[input.IdempotencyKey]; exists {
			var rotateErr error
			ticket, rotateErr = rotatePersistedJobTicket(state, input.IdempotencyKey, input.TargetVersion, service.now().UTC())
			return rotateErr
		}
		if hasActiveJob(*state) {
			return rejectMutation(http.StatusConflict, "operation_in_progress")
		}
		if state.DesiredState != DesiredRunning {
			return rejectMutation(http.StatusConflict, "desired_state_not_running")
		}
		if _, exists := state.Plans[planHash]; exists {
			return rejectMutation(http.StatusInternalServerError, "token_generation_failed")
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
			CurrentVersion:    currentVersion,
			TargetVersion:     plan.Manifest.Version,
			CurrentStep:       JobStepValidate,
			TotalSteps:        executionTotalSteps,
			TotalHops:         1,
			ServiceAvailable:  true,
			LastSafeVersion:   currentVersion,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		state.Plans[planHash] = plan
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

func (service *Service) replayJobTicket(response http.ResponseWriter, request *http.Request) {
	if !service.controlAuthorized(request) {
		writeAPIError(response, http.StatusUnauthorized, "control_token_required")
		return
	}
	var input replayJobRequest
	if err := decodeControlRequest(response, request, &input, "job replay request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request: "+err.Error())
		return
	}
	input.TargetVersion = strings.TrimSpace(input.TargetVersion)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if _, err := parseCanonicalVersion("target_version", input.TargetVersion); err != nil || !canonicalUUIDPattern.MatchString(input.IdempotencyKey) {
		writeAPIError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	service.rotateJobTicket(response, request, input, http.StatusOK)
}

func (service *Service) rotateJobTicket(response http.ResponseWriter, request *http.Request, input replayJobRequest, successStatus int) {
	var ticket JobTicket
	var rejection *mutationRejection
	err := service.store.Update(request.Context(), func(state *RuntimeState) error {
		var rotateErr error
		ticket, rotateErr = rotatePersistedJobTicket(state, input.IdempotencyKey, input.TargetVersion, service.now().UTC())
		return rotateErr
	})
	if err != nil {
		if errors.As(err, &rejection) {
			writeAPIError(response, rejection.status, rejection.code)
		} else {
			writeAPIError(response, http.StatusInternalServerError, "state_write_failed")
		}
		return
	}
	writeJSON(response, successStatus, ticket)
}

func rotatePersistedJobTicket(state *RuntimeState, idempotencyKey, targetVersion string, now time.Time) (JobTicket, error) {
	existingID, exists := state.Idempotency[idempotencyKey]
	if !exists {
		return JobTicket{}, rejectMutation(http.StatusNotFound, "idempotency_not_found")
	}
	job, exists := state.Jobs[existingID]
	if !exists {
		return JobTicket{}, rejectMutation(http.StatusInternalServerError, "state_inconsistent")
	}
	if job.TargetVersion != targetVersion {
		return JobTicket{}, rejectMutation(http.StatusConflict, "idempotency_conflict")
	}
	rawBearer, err := randomToken(32)
	if err != nil {
		return JobTicket{}, rejectMutation(http.StatusInternalServerError, "token_generation_failed")
	}
	job.BearerTokenHashes = append(job.BearerTokenHashes, tokenHash(rawBearer))
	job.UpdatedAt = now
	state.Jobs[job.ID] = job
	return JobTicket{JobID: job.ID, JobToken: rawBearer, StatusURL: publicJobPath(job.ID), Status: job.Status}, nil
}

func preflightDirectJob(state RuntimeState) *mutationRejection {
	if hasActiveJob(state) {
		return &mutationRejection{status: http.StatusConflict, code: "operation_in_progress"}
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
	if len(parts) == 2 && parts[0] != "" && parts[1] == "restart" {
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
		job.Status = JobRestarting
		job.CurrentStep = JobStepRestart
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
