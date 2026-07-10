package updater

import (
	"errors"
	"net/http"
)

type desiredStateRequest struct {
	DesiredState DesiredState `json:"desired_state"`
}

type desiredStateResponse struct {
	DesiredState DesiredState `json:"desired_state"`
}

func (service *Service) setDesiredState(response http.ResponseWriter, request *http.Request) {
	if !service.controlAuthorized(request) {
		writeAPIError(response, http.StatusUnauthorized, "control_token_required")
		return
	}
	var input desiredStateRequest
	if err := decodeControlRequest(response, request, &input, "desired-state request"); err != nil {
		writeAPIError(response, http.StatusBadRequest, "invalid_request: "+err.Error())
		return
	}
	if !input.DesiredState.valid() {
		writeAPIError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	var rejection *mutationRejection
	if err := service.store.Update(request.Context(), func(state *RuntimeState) error {
		if hasActiveJob(*state) {
			return rejectMutation(http.StatusConflict, "operation_in_progress")
		}
		if input.DesiredState == DesiredUpgrading {
			return rejectMutation(http.StatusConflict, "desired_state_reserved")
		}
		state.DesiredState = input.DesiredState
		return nil
	}); err != nil {
		if errors.As(err, &rejection) {
			writeAPIError(response, rejection.status, rejection.code)
		} else {
			writeAPIError(response, http.StatusInternalServerError, "state_write_failed")
		}
		return
	}
	writeJSON(response, http.StatusOK, desiredStateResponse{DesiredState: input.DesiredState})
}
