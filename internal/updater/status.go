package updater

import (
	"net/http"
	"time"
)

// statusRequest is deliberately empty. The resident updater reads the pinned
// host version itself; callers cannot claim a source version or schema.
type statusRequest struct{}

type ActiveJobStatus struct {
	JobID            string    `json:"job_id"`
	Status           JobStatus `json:"status"`
	CurrentVersion   string    `json:"current_version"`
	TargetVersion    string    `json:"target_version"`
	ServiceAvailable bool      `json:"service_available"`
}

type StatusResponse struct {
	Available      bool               `json:"available"`
	UpdaterReady   bool               `json:"updater_ready"`
	CurrentVersion string             `json:"current_version"`
	DesiredState   DesiredState       `json:"desired_state"`
	ActiveJob      *ActiveJobStatus   `json:"active_job,omitempty"`
	Watchdog       WatchdogStatusView `json:"watchdog"`
}

type WatchdogStatusView struct {
	Status         WatchdogStatus `json:"status"`
	Degraded       bool           `json:"degraded"`
	CooldownUntil  *time.Time     `json:"cooldown_until"`
	LastObservedAt *time.Time     `json:"last_observed_at"`
	ErrorCode      string         `json:"error_code"`
}

func (service *Service) getStatus(response http.ResponseWriter, request *http.Request) {
	if !service.controlAuthorized(request) {
		writeAPIError(response, http.StatusUnauthorized, "control_token_required")
		return
	}
	var input statusRequest
	if err := decodeControlRequest(response, request, &input, "status request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request: "+err.Error())
		return
	}
	if service.directRuntime == nil {
		writeAPIError(response, http.StatusServiceUnavailable, "updater_not_ready")
		return
	}
	currentVersion, err := service.directRuntime.CurrentVersion(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "runtime_status_unavailable")
		return
	}
	if _, err := parseCanonicalVersion("current_version", currentVersion); err != nil {
		writeAPIError(response, http.StatusServiceUnavailable, "runtime_status_unavailable")
		return
	}
	state, err := service.store.Load(request.Context())
	if err != nil {
		writeAPIError(response, http.StatusInternalServerError, "state_read_failed")
		return
	}
	status := StatusResponse{
		Available:      true,
		CurrentVersion: currentVersion,
		DesiredState:   state.DesiredState,
		Watchdog:       publicWatchdogStatus(state.Watchdog),
	}
	for _, job := range state.Jobs {
		if !job.Status.active() {
			continue
		}
		if status.ActiveJob != nil {
			writeAPIError(response, http.StatusInternalServerError, "state_inconsistent")
			return
		}
		status.ActiveJob = &ActiveJobStatus{
			JobID:            job.ID,
			Status:           job.Status,
			CurrentVersion:   job.CurrentVersion,
			TargetVersion:    job.TargetVersion,
			ServiceAvailable: job.ServiceAvailable,
		}
	}
	status.UpdaterReady = status.ActiveJob == nil && state.DesiredState == DesiredRunning
	writeJSON(response, http.StatusOK, status)
}

func publicWatchdogStatus(state WatchdogState) WatchdogStatusView {
	view := WatchdogStatusView{
		Status:    state.Status,
		Degraded:  state.Status == WatchdogDegraded,
		ErrorCode: state.ErrorCode,
	}
	if !state.CooldownUntil.IsZero() {
		cooldown := state.CooldownUntil.UTC()
		view.CooldownUntil = &cooldown
	}
	if !state.LastObservedAt.IsZero() {
		observed := state.LastObservedAt.UTC()
		view.LastObservedAt = &observed
	}
	return view
}

func hasActiveJob(state RuntimeState) bool {
	for _, job := range state.Jobs {
		if job.Status.active() {
			return true
		}
	}
	return false
}
