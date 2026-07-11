package updater

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testControlToken = "test-control-token"
	testPlanToken    = "test-plan-token"
)

func TestControlJobCreationRequiresControlTokenAndRejectsInfrastructureFields(t *testing.T) {
	service, _ := newTestService(t)

	response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, "", "")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected control auth rejection, got %d: %s", response.Code, response.Body.String())
	}

	for _, field := range []string{"shell", "compose_path", "service", "image", "digest", "caddy_mode"} {
		body := strings.TrimSuffix(`{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, "}") + `,"` + field + `":"attacker"}`
		response = postJSON(t, service.Handler(), controlJobsPath, body, testControlToken, "")
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unknown field") {
			t.Fatalf("expected %s rejection, got %d: %s", field, response.Code, response.Body.String())
		}
	}
}

func TestControlDiscoveryRequiresTokenAndUsesTheResidentStateOwner(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	calls := 0
	service, err := NewService(store, testControlToken, WithReleaseSource(releaseSourceFunc(func(context.Context) ([]byte, error) {
		calls++
		return []byte(validSingleReleaseIndexJSON(t)), nil
	})))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	unauthorized := postJSON(t, service.Handler(), controlDiscoveryPath, `{}`, "", "")
	if unauthorized.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("unauthorized discovery reached release source: status=%d calls=%d", unauthorized.Code, calls)
	}
	authorized := postJSON(t, service.Handler(), controlDiscoveryPath, `{}`, testControlToken, "")
	if authorized.Code != http.StatusOK || calls != 1 {
		t.Fatalf("resident discovery failed: status=%d calls=%d body=%s", authorized.Code, calls, authorized.Body.String())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Discovery.Status != DiscoveryFresh || state.Discovery.Manifest == nil {
		t.Fatalf("discovery was not persisted by the resident service: %#v", state.Discovery)
	}
}

func TestJobBearerIsHashedAndAuthorizesPublicStatus(t *testing.T) {
	service, store := newTestService(t)
	response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("create job: %d %s", response.Code, response.Body.String())
	}
	var ticket JobTicket
	decodeResponse(t, response, &ticket)
	if ticket.JobID == "" || ticket.JobToken == "" {
		t.Fatalf("missing job ticket fields: %#v", ticket)
	}
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(ticket.JobToken)) || bytes.Contains(data, []byte(testPlanToken)) {
		t.Fatalf("raw tokens leaked into persisted state: %s", data)
	}

	request := httptest.NewRequest(http.MethodGet, publicJobPath(ticket.JobID), nil)
	request.Header.Set("Authorization", "Bearer "+ticket.JobToken)
	status := httptest.NewRecorder()
	service.Handler().ServeHTTP(status, request)
	if status.Code != http.StatusOK {
		t.Fatalf("authorized job status: %d %s", status.Code, status.Body.String())
	}
	var publicStatus publicJob
	decodeResponse(t, status, &publicStatus)
	if publicStatus.CurrentVersion != "v1.0.0" || publicStatus.TargetVersion != "v1.1.0" {
		t.Fatalf("job status lost its immutable version edge: %#v", publicStatus)
	}
	if publicStatus.CompletedHops != 0 || publicStatus.TotalHops != 1 || publicStatus.TotalSteps != executionTotalSteps {
		t.Fatalf("job status lost overall release progress: %#v", publicStatus)
	}
	request = httptest.NewRequest(http.MethodGet, publicJobPath(ticket.JobID), nil)
	request.Header.Set("Authorization", "Bearer wrong")
	status = httptest.NewRecorder()
	service.Handler().ServeHTTP(status, request)
	if status.Code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer must fail, got %d", status.Code)
	}
}

func TestPublicRestartOperationRequiresOfferedOperationAndJobBearer(t *testing.T) {
	service, store := newTestService(t)
	created := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"restart-request","confirm":"apply_release_change"}`, testControlToken, "")
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
	path := publicJobPath(ticket.JobID) + "/restart"
	unauthorized := postJSON(t, service.Handler(), path, `{}`, "", "wrong-token")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized restart status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	response := postJSON(t, service.Handler(), path, `{}`, "", ticket.JobToken)
	if response.Code != http.StatusAccepted {
		t.Fatalf("restart status=%d body=%s", response.Code, response.Body.String())
	}
	var job publicJob
	decodeResponse(t, response, &job)
	if job.Status != JobRestarting || job.CurrentStep != JobStepRestart || job.ServiceAvailable {
		t.Fatalf("restart was not durably queued: %#v", job)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.DesiredState != DesiredUpgrading {
		t.Fatalf("restart did not enter protected desired state: %q", state.DesiredState)
	}
}

func TestPublicRollbackOperationRequiresCommittedRecoveryMetadata(t *testing.T) {
	service, store := newTestService(t)
	created := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"rollback-request","confirm":"apply_release_change"}`, testControlToken, "")
	var ticket JobTicket
	decodeResponse(t, created, &ticket)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[ticket.JobID]
	recovery, err := (&fakeUpgradeRuntime{}).PrepareBackup(context.Background(), job, state.Plans[job.PlanTokenHash], ignoreProgress)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[ticket.JobID]
		job.Status = JobFailed
		job.CurrentStep = JobStepComplete
		job.ServiceAvailable = false
		job.ErrorCode = "rollback_required"
		job.ErrorMessage = "safe failure"
		job.RecoveryPoint = &recovery
		state.Jobs[job.ID] = job
		state.DesiredState = DesiredRunning
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	response := postJSON(t, service.Handler(), publicJobPath(ticket.JobID)+"/rollback", `{}`, "", ticket.JobToken)
	if response.Code != http.StatusAccepted {
		t.Fatalf("rollback status=%d body=%s", response.Code, response.Body.String())
	}
	var public publicJob
	decodeResponse(t, response, &public)
	if public.Status != JobRollingBack || public.CurrentStep != JobStepRestoreBackup {
		t.Fatalf("rollback was not durably queued: %#v", public)
	}
}

func TestSuccessfulJobCanQueueCommittedRollback(t *testing.T) {
	service, store := newTestService(t)
	created := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"successful-rollback","confirm":"apply_release_change"}`, testControlToken, "")
	var ticket JobTicket
	decodeResponse(t, created, &ticket)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[ticket.JobID]
	recovery, err := (&fakeUpgradeRuntime{}).PrepareBackup(context.Background(), job, state.Plans[job.PlanTokenHash], ignoreProgress)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[ticket.JobID]
		job.Status = JobSucceeded
		job.CurrentStep = JobStepComplete
		job.CompletedSteps = job.TotalSteps
		job.CurrentHop = job.TotalHops
		job.CurrentVersion = job.TargetVersion
		job.ServiceAvailable = true
		job.LastSafeVersion = job.TargetVersion
		job.RecoveryPoint = &recovery
		state.Jobs[job.ID] = job
		state.DesiredState = DesiredRunning
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rollback := postJSON(t, service.Handler(), publicJobPath(ticket.JobID)+"/rollback", `{}`, "", ticket.JobToken)
	if rollback.Code != http.StatusAccepted {
		t.Fatalf("successful job rollback status=%d body=%s", rollback.Code, rollback.Body.String())
	}
}

func TestNewJobRevokesOlderTerminalRollbackAuthority(t *testing.T) {
	service, store := newTestService(t)
	firstResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"first-edge","confirm":"apply_release_change"}`, testControlToken, "")
	var first JobTicket
	decodeResponse(t, firstResponse, &first)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstJob := state.Jobs[first.JobID]
	recovery, err := (&fakeUpgradeRuntime{}).PrepareBackup(context.Background(), firstJob, state.Plans[firstJob.PlanTokenHash], ignoreProgress)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[first.JobID]
		job.Status = JobSucceeded
		job.CurrentStep = JobStepComplete
		job.CompletedSteps = executionTotalSteps
		job.CurrentHop = job.TotalHops
		job.CurrentVersion = job.TargetVersion
		job.ServiceAvailable = true
		job.LastSafeVersion = job.TargetVersion
		job.RecoveryPoint = &recovery
		state.Jobs[job.ID] = job
		state.DesiredState = DesiredRunning
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reused := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"reused-edge","confirm":"apply_release_change"}`, testControlToken, "")
	if reused.Code != http.StatusConflict || !strings.Contains(reused.Body.String(), "plan_already_used") {
		t.Fatalf("consumed plan was reused: status=%d body=%s", reused.Code, reused.Body.String())
	}
	nextManifest := *state.Discovery.Manifest
	nextManifest.Version = "v1.2.0"
	nextManifest.Image = AllowedImageRepository + ":v1.2.0"
	nextManifest.UpgradeFrom = []string{"=v1.1.0"}
	nextManifest.ReleaseNotesURL = "https://github.com/YingSuiAI/dirextalk-message-server/releases/tag/v1.2.0"
	nextDigest := canonicalManifestDigest(nextManifest)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		index := *state.Discovery.Index
		index.LatestVersion = nextManifest.Version
		index.Releases = append(append([]IndexedRelease(nil), index.Releases...), IndexedRelease{Manifest: nextManifest, ManifestDigest: nextDigest})
		index.Edges = append(append([]UpgradeEdge(nil), index.Edges...), UpgradeEdge{FromVersion: "v1.1.0", FromImageDigests: []string{"sha256:" + strings.Repeat("a", 64)}, ToVersion: nextManifest.Version})
		state.Discovery.Manifest = &nextManifest
		state.Discovery.ManifestDigest = nextDigest
		state.Discovery.Index = &index
		state.Discovery.IndexDigest = canonicalReleaseIndexDigest(index)
		state.Discovery.CheckedAt = time.Now().UTC()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.RegisterPlan(context.Background(), "second-plan", Plan{Manifest: nextManifest, ManifestDigest: nextDigest, CurrentVersion: "v1.1.0", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	second := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"second-plan","idempotency_key":"second-edge","confirm":"apply_release_change"}`, testControlToken, "")
	if second.Code != http.StatusAccepted {
		t.Fatalf("second job status=%d body=%s", second.Code, second.Body.String())
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Jobs[first.JobID].RecoveryPoint == nil {
		t.Fatal("new job revoked the valid rollback before replacing the single backup slot")
	}
	if err := NewJobEngine(store, &fakeUpgradeRuntime{digestByVersion: map[string]string{"v1.1.0": "sha256:" + strings.Repeat("a", 64)}}).RunActive(context.Background()); err != nil {
		t.Fatalf("run second version edge: %v", err)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Jobs[first.JobID].RecoveryPoint != nil || len(publicJobOperations(state.Jobs[first.JobID])) != 0 {
		t.Fatalf("older job retained stale rollback authority: %#v", state.Jobs[first.JobID])
	}
}

func TestIdempotentReplayReturnsSameJobAndSurvivesRestart(t *testing.T) {
	service, store := newTestService(t)
	firstResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var first JobTicket
	decodeResponse(t, firstResponse, &first)
	secondResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var second JobTicket
	decodeResponse(t, secondResponse, &second)
	if first.JobID != second.JobID || first.JobToken == second.JobToken {
		t.Fatalf("idempotent replay must reuse job and mint an additional bearer: first=%#v second=%#v", first, second)
	}

	restarted, err := NewService(store, testControlToken)
	if err != nil {
		t.Fatalf("restart service: %v", err)
	}
	for _, token := range []string{first.JobToken, second.JobToken} {
		request := httptest.NewRequest(http.MethodGet, publicJobPath(first.JobID), nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		restarted.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("persisted bearer rejected after restart: %d %s", response.Code, response.Body.String())
		}
	}
	replay := postJSON(t, restarted.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var replayTicket JobTicket
	decodeResponse(t, replay, &replayTicket)
	if replayTicket.JobID != first.JobID {
		t.Fatalf("restart lost idempotency mapping: %#v", replayTicket)
	}
}

func TestFirstJobTransitionsDesiredStateAndBlocksDifferentKey(t *testing.T) {
	service, store := newTestService(t)
	first := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	if first.Code != http.StatusAccepted {
		t.Fatalf("create first job: %d %s", first.Code, first.Body.String())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.DesiredState != DesiredUpgrading || len(state.Jobs) != 1 {
		t.Fatalf("job and desired state were not committed together: %#v", state)
	}
	blocked := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-2","confirm":"apply_release_change"}`, testControlToken, "")
	if blocked.Code != http.StatusConflict || !strings.Contains(blocked.Body.String(), "operation_in_progress") {
		t.Fatalf("second key was not blocked: %d %s", blocked.Code, blocked.Body.String())
	}
}

func TestNewJobIsRejectedForEveryNonRunningDesiredState(t *testing.T) {
	for _, desired := range []DesiredState{DesiredMaintenance, DesiredDeprovisioned} {
		t.Run(string(desired), func(t *testing.T) {
			service, store := newTestService(t)
			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			state.DesiredState = desired
			if err := store.Save(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "desired_state_not_running") {
				t.Fatalf("desired state %s accepted job: %d %s", desired, response.Code, response.Body.String())
			}
		})
	}
}

func TestConcurrentDifferentKeysCreateOnlyOneJob(t *testing.T) {
	service, store := newTestService(t)
	bodies := []string{
		`{"plan_token":"test-plan-token","idempotency_key":"concurrent-1","confirm":"apply_release_change"}`,
		`{"plan_token":"test-plan-token","idempotency_key":"concurrent-2","confirm":"apply_release_change"}`,
	}
	responses := make([]*httptest.ResponseRecorder, len(bodies))
	var wait sync.WaitGroup
	for index := range bodies {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			responses[index] = postJSON(t, service.Handler(), controlJobsPath, bodies[index], testControlToken, "")
		}(index)
	}
	wait.Wait()
	accepted := 0
	conflicts := 0
	for _, response := range responses {
		switch response.Code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			if !strings.Contains(response.Body.String(), "operation_in_progress") {
				t.Fatalf("unexpected conflict: %s", response.Body.String())
			}
			conflicts++
		default:
			t.Fatalf("unexpected concurrent response: %d %s", response.Code, response.Body.String())
		}
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || conflicts != 1 || len(state.Jobs) != 1 || state.DesiredState != DesiredUpgrading {
		t.Fatalf("concurrent apply invariant failed: accepted=%d conflicts=%d state=%#v", accepted, conflicts, state)
	}
}

func TestIdempotentReplaySucceedsAfterPlanExpiryButDifferentKeyFails(t *testing.T) {
	service, store := newTestService(t)
	firstResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var first JobTicket
	decodeResponse(t, firstResponse, &first)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	planHash := tokenHash(testPlanToken)
	plan := state.Plans[planHash]
	plan.ExpiresAt = time.Now().Add(-time.Minute)
	state.Plans[planHash] = plan
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}

	replayResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var replay JobTicket
	decodeResponse(t, replayResponse, &replay)
	if replay.JobID != first.JobID || replay.JobToken == first.JobToken {
		t.Fatalf("expired-plan replay did not recover job: first=%#v replay=%#v", first, replay)
	}
	differentKey := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-2","confirm":"apply_release_change"}`, testControlToken, "")
	if differentKey.Code != http.StatusConflict || !strings.Contains(differentKey.Body.String(), "plan_invalid_or_expired") {
		t.Fatalf("expired plan authorized a new key: %d %s", differentKey.Code, differentKey.Body.String())
	}
}

func TestIdempotencyKeyCannotBeReusedForDifferentPlan(t *testing.T) {
	service, store := newTestService(t)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.RegisterPlan(context.Background(), "other-plan", Plan{Manifest: manifest, ManifestDigest: state.Discovery.ManifestDigest, CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	first := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	if first.Code != http.StatusAccepted {
		t.Fatalf("create first job: %d %s", first.Code, first.Body.String())
	}
	conflict := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"other-plan","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected idempotency conflict, got %d %s", conflict.Code, conflict.Body.String())
	}
}

func TestRegisterPlanRequiresTheDiscoveredManifestAndDoesNotLeakFailedWrites(t *testing.T) {
	service, store := newTestService(t)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	drifted := *state.Discovery.Manifest
	drifted.ImageDigest = "sha256:" + strings.Repeat("b", 64)
	err = service.RegisterPlan(context.Background(), "drifted-plan", Plan{
		Manifest:       drifted,
		ManifestDigest: state.Discovery.ManifestDigest,
		CurrentVersion: "v1.0.0",
		ExpiresAt:      time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected manifest drift to be rejected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = service.RegisterPlan(ctx, "failed-plan", Plan{
		Manifest:       *state.Discovery.Manifest,
		ManifestDigest: state.Discovery.ManifestDigest,
		CurrentVersion: "v1.0.0",
		ExpiresAt:      time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected cancelled write to fail")
	}
	response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"failed-plan","idempotency_key":"failed-request","confirm":"apply_release_change"}`, testControlToken, "")
	if response.Code != http.StatusConflict {
		t.Fatalf("failed plan write leaked into live state: %d %s", response.Code, response.Body.String())
	}
}

func TestRegisterPlanRejectsUnsupportedEdgeAndTokenRebinding(t *testing.T) {
	service, store := newTestService(t)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	unsupported := Plan{
		Manifest:       *state.Discovery.Manifest,
		ManifestDigest: state.Discovery.ManifestDigest,
		CurrentVersion: "v9.0.0",
		ExpiresAt:      time.Now().Add(time.Hour),
	}
	if err := service.RegisterPlan(context.Background(), "unsupported-edge", unsupported); err == nil {
		t.Fatal("expected unsupported upgrade edge to be rejected")
	}

	rebound := Plan{
		Manifest:       *state.Discovery.Manifest,
		ManifestDigest: state.Discovery.ManifestDigest,
		CurrentVersion: "v1.0.0",
		ExpiresAt:      time.Now().Add(2 * time.Hour),
	}
	if err := service.RegisterPlan(context.Background(), testPlanToken, rebound); err == nil {
		t.Fatal("expected an existing plan token to be immutable")
	}
}

func TestRegisterPlanAtomicallyRechecksFreshnessAndEligibility(t *testing.T) {
	service, store := newTestService(t)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkedAt := state.Discovery.CheckedAt
	plan := Plan{Manifest: *state.Discovery.Manifest, ManifestDigest: state.Discovery.ManifestDigest, CurrentVersion: "v1.0.0", ExpiresAt: checkedAt.Add(48 * time.Hour)}
	service.now = func() time.Time { return checkedAt.Add(DiscoveryMaximumAge + time.Nanosecond) }
	if err := service.RegisterPlan(context.Background(), "expired-discovery", plan); err == nil || !strings.Contains(err.Error(), "fresh discovered release") {
		t.Fatalf("expired discovery registered a plan: %v", err)
	}

	service.now = func() time.Time { return checkedAt.Add(time.Hour) }
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.DesiredState = DesiredMaintenance
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if err := service.RegisterPlan(context.Background(), "maintenance-plan", plan); err == nil || !strings.Contains(err.Error(), "desired state") {
		t.Fatalf("maintenance registered a plan: %v", err)
	}
}

func TestPostRenameSyncFailureIsReconciledFromPersistedState(t *testing.T) {
	service, store := newTestService(t)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		Manifest:       *state.Discovery.Manifest,
		ManifestDigest: state.Discovery.ManifestDigest,
		CurrentVersion: "v1.0.0",
		ExpiresAt:      time.Now().Add(time.Hour),
	}
	originalSync := store.syncDirectory
	store.syncDirectory = func(string) error { return errors.New("injected directory fsync failure") }
	if err := service.RegisterPlan(context.Background(), "ambiguous-plan", plan); err == nil {
		t.Fatal("expected the post-rename fsync failure to be reported")
	}
	store.syncDirectory = originalSync

	response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"ambiguous-plan","idempotency_key":"ambiguous-request","confirm":"apply_release_change"}`, testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("service did not reconcile the renamed state file: %d %s", response.Code, response.Body.String())
	}
}

func TestIdempotentReplayRecoversAfterPostRenameSyncFailure(t *testing.T) {
	service, store := newTestService(t)
	firstResponse := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var first JobTicket
	decodeResponse(t, firstResponse, &first)

	originalSync := store.syncDirectory
	store.syncDirectory = func(string) error { return errors.New("injected directory fsync failure") }
	failedReplay := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	if failedReplay.Code != http.StatusInternalServerError {
		t.Fatalf("expected state write failure, got %d %s", failedReplay.Code, failedReplay.Body.String())
	}
	store.syncDirectory = originalSync

	replay := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"request-1","confirm":"apply_release_change"}`, testControlToken, "")
	var recovered JobTicket
	decodeResponse(t, replay, &recovered)
	if recovered.JobID != first.JobID {
		t.Fatalf("idempotent retry created a different job after ambiguous commit: first=%s recovered=%s", first.JobID, recovered.JobID)
	}
}

func newTestService(t *testing.T) (*Service, *StateStore) {
	t.Helper()
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	indexData := []byte(validSingleReleaseIndexJSON(t))
	state := NewRuntimeState()
	state.Discovery = testDiscoveryCache(t, indexData, DiscoveryFresh, time.Now())
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatalf("seed discovery: %v", err)
	}
	service, err := NewService(store, testControlToken)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	chain := mustUpgradePath(t, *state.Discovery.Index, "v1.0.0")
	if err := service.RegisterPlan(context.Background(), testPlanToken, Plan{Manifest: *state.Discovery.Manifest, ManifestDigest: state.Discovery.ManifestDigest, CurrentVersion: "v1.0.0", ReleaseChain: chain, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("RegisterPlan: %v", err)
	}
	return service, store
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
