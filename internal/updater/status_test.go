package updater

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
)

func TestControlStatusReadsPinnedRuntimeAndExposesReadiness(t *testing.T) {
	service, _ := newTestService(t)
	unauthorized := postJSON(t, service.Handler(), controlStatusPath, `{}`, "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("status without control token: %d %s", unauthorized.Code, unauthorized.Body.String())
	}
	unknown := postJSON(t, service.Handler(), controlStatusPath, `{"current_version":"v9.9.9"}`, testControlToken, "")
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("status accepted caller version: %d %s", unknown.Code, unknown.Body.String())
	}
	response := postJSON(t, service.Handler(), controlStatusPath, `{}`, testControlToken, "")
	if response.Code != http.StatusOK {
		t.Fatalf("status: %d %s", response.Code, response.Body.String())
	}
	var status StatusResponse
	decodeResponse(t, response, &status)
	if !status.Available || !status.UpdaterReady || status.DirectContractVersion != DirectContractVersion || status.CurrentVersion != "v1.0.0" || status.DesiredState != DesiredRunning || status.ActiveJob != nil {
		t.Fatalf("unexpected direct status: %#v", status)
	}
}

func TestControlStatusReportsActiveJobAsNotReady(t *testing.T) {
	service, _ := newTestService(t)
	created := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", "7e4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
	var ticket JobTicket
	decodeResponse(t, created, &ticket)
	response := postJSON(t, service.Handler(), controlStatusPath, `{}`, testControlToken, "")
	var status StatusResponse
	decodeResponse(t, response, &status)
	if status.UpdaterReady || status.ActiveJob == nil || status.ActiveJob.JobID != ticket.JobID || status.ActiveJob.TargetVersion != "v1.0.3" {
		t.Fatalf("active job was not surfaced: %#v", status)
	}
}

func TestControlStatusDoesNotReportReadyWithoutTrustedReleaseSource(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(newTestDirectRuntime()))
	if err != nil {
		t.Fatal(err)
	}
	response := postJSON(t, service.Handler(), controlStatusPath, `{}`, testControlToken, "")
	var status StatusResponse
	decodeResponse(t, response, &status)
	if status.UpdaterReady || status.DirectContractVersion != DirectContractVersion {
		t.Fatalf("missing trusted release source reported ready: %#v", status)
	}
}

type unavailableDirectRuntime struct{}

func (unavailableDirectRuntime) CurrentVersion(context.Context) (string, error) {
	return "", errors.New("unavailable")
}

func (unavailableDirectRuntime) InspectDirectSource(context.Context, string, ReleaseStep) (DirectSource, error) {
	return DirectSource{}, errors.New("unavailable")
}

func TestControlStatusFailsClosedWhenRuntimeVersionCannotBeRead(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(unavailableDirectRuntime{}))
	if err != nil {
		t.Fatal(err)
	}
	response := postJSON(t, service.Handler(), controlStatusPath, `{}`, testControlToken, "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable runtime reported success: %d %s", response.Code, response.Body.String())
	}
}
