package updater

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testControlToken = "test-control-token"

func TestControlJobRequiresAuthAndStrictDirectFields(t *testing.T) {
	service, _ := newTestService(t)
	valid := directJobRequest("v1.0.3", "2d4d8444-2b3d-4f8f-8503-910f58b5b1df")
	unauthorized := postJSON(t, service.Handler(), controlJobsPath, valid, "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected control auth rejection, got %d: %s", unauthorized.Code, unauthorized.Body.String())
	}

	for _, field := range []string{"plan_token", "shell", "compose_path", "service", "image", "digest", "url", "caddy_mode"} {
		body := strings.TrimSuffix(valid, "}") + `,"` + field + `":"attacker"}`
		response := postJSON(t, service.Handler(), controlJobsPath, body, testControlToken, "")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unknown field") {
			t.Fatalf("expected %s rejection, got %d: %s", field, response.Code, response.Body.String())
		}
	}
	for _, body := range []string{
		`{"target_version":"1.0.3","idempotency_key":"2d4d8444-2b3d-4f8f-8503-910f58b5b1df","confirm":"apply_release_change"}`,
		`{"target_version":"v1.0.3","idempotency_key":"request-1","confirm":"apply_release_change"}`,
		`{"target_version":"v1.0.3","idempotency_key":"2d4d8444-2b3d-4f8f-8503-910f58b5b1df","confirm":"wrong"}`,
	} {
		response := postJSON(t, service.Handler(), controlJobsPath, body, testControlToken, "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid direct request accepted: %s -> %d %s", body, response.Code, response.Body.String())
		}
	}
}

func TestControlDiscoveryEndpointIsNoLongerActive(t *testing.T) {
	service, _ := newTestService(t)
	response := postJSON(t, service.Handler(), apiPrefix+"control/discovery", `{}`, testControlToken, "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("legacy discovery remains reachable: %d %s", response.Code, response.Body.String())
	}
}

func TestDirectJobBearerIsHashedAndAuthorizesPublicStatus(t *testing.T) {
	service, store := newTestService(t)
	response := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", "3e4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("create job: %d %s", response.Code, response.Body.String())
	}
	var ticket JobTicket
	decodeResponse(t, response, &ticket)
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(ticket.JobToken)) {
		t.Fatalf("raw job token leaked into persisted state: %s", data)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[ticket.JobID]
	if job.DirectRelease == nil || job.DirectRelease.Version != "v1.0.3" || job.TotalHops != 1 || job.TotalSteps != executionTotalSteps {
		t.Fatalf("direct job did not preserve the immutable target: %#v", job)
	}

	request := httptest.NewRequest(http.MethodGet, publicJobPath(ticket.JobID), nil)
	request.Header.Set("Authorization", "Bearer "+ticket.JobToken)
	status := httptest.NewRecorder()
	service.Handler().ServeHTTP(status, request)
	if status.Code != http.StatusOK {
		t.Fatalf("authorized job status: %d %s", status.Code, status.Body.String())
	}
	request.Header.Set("Authorization", "Bearer wrong")
	status = httptest.NewRecorder()
	service.Handler().ServeHTTP(status, request)
	if status.Code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer must fail, got %d", status.Code)
	}
}

func TestDirectJobIdempotencyBindsTargetVersion(t *testing.T) {
	service, _ := newTestService(t)
	key := "4e4d8444-2b3d-4f8f-8503-910f58b5b1df"
	firstResponse := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", key), testControlToken, "")
	var first JobTicket
	decodeResponse(t, firstResponse, &first)
	secondResponse := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", key), testControlToken, "")
	var second JobTicket
	decodeResponse(t, secondResponse, &second)
	if first.JobID != second.JobID || first.JobToken == second.JobToken {
		t.Fatalf("idempotent replay did not preserve the job: first=%#v second=%#v", first, second)
	}
	conflict := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.4", key), testControlToken, "")
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "idempotency_conflict") {
		t.Fatalf("same key bound to another target: %d %s", conflict.Code, conflict.Body.String())
	}
}

func TestDirectJobRejectsDowngradeOrNoop(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	runtime := &directControlRuntime{
		currentVersion: "v1.0.3",
		resolved:       DirectRelease{Version: "v1.0.3", ImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"v1.0.3", "v1.0.2"} {
		response := postJSON(t, service.Handler(), controlJobsPath, directJobRequest(target, "5e4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "target_not_newer") {
			t.Fatalf("target %s was accepted: %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestPublicRestartRemainsButRollbackIsNotExposed(t *testing.T) {
	service, store := newTestService(t)
	created := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", "6e4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
	var ticket JobTicket
	decodeResponse(t, created, &ticket)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[ticket.JobID]
		job.Status = JobFailed
		job.CurrentStep = JobStepComplete
		job.ServiceAvailable = false
		job.ErrorCode = "backup_failed"
		job.ErrorMessage = "safe failure"
		state.Jobs[job.ID] = job
		state.DesiredState = DesiredRunning
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rollback := postJSON(t, service.Handler(), publicJobPath(ticket.JobID)+"/rollback", `{}`, "", ticket.JobToken)
	if rollback.Code != http.StatusNotFound {
		t.Fatalf("manual rollback remains public: %d %s", rollback.Code, rollback.Body.String())
	}
	restart := postJSON(t, service.Handler(), publicJobPath(ticket.JobID)+"/restart", `{}`, "", ticket.JobToken)
	if restart.Code != http.StatusAccepted {
		t.Fatalf("restart was not available: %d %s", restart.Code, restart.Body.String())
	}
}

func TestPublicJobSupportsBrowserCORSPreflight(t *testing.T) {
	service, _ := newTestService(t)
	request := httptest.NewRequest(http.MethodOptions, publicJobsPrefix+"job_browser", nil)
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("unexpected CORS preflight: %d %#v", response.Code, response.Header())
	}
}

func newTestService(t *testing.T) (*Service, *StateStore) {
	t.Helper()
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	runtime := newTestDirectRuntime()
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service, store
}

func newTestDirectRuntime() *directControlRuntime {
	return &directControlRuntime{
		currentVersion: "v1.0.0",
		resolved:       DirectRelease{Version: "v1.0.3", ImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
}

func directJobRequest(target, idempotencyKey string) string {
	return `{"target_version":"` + target + `","idempotency_key":"` + idempotencyKey + `","confirm":"apply_release_change"}`
}

func postJSON(t *testing.T, handler http.Handler, path, body, controlToken, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if controlToken != "" {
		request.Header.Set(controlTokenHeader, controlToken)
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if response.Code != http.StatusAccepted && response.Code != http.StatusOK {
		t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
	}
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(target); err != nil && err != io.EOF {
		t.Fatalf("decode response: %v", err)
	}
}
