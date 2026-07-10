package updater

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWatchdogRepairsOnlyAfterThreeFailedObservations(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	runtime := &fakeWatchdogRuntime{observeErr: errors.New("message-server down"), repairErr: errors.New("repair failed")}
	watchdog := NewWatchdog(store, runtime)
	watchdog.now = func() time.Time { return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC) }

	for observation := 1; observation <= 2; observation++ {
		if err := watchdog.Reconcile(context.Background()); err == nil {
			t.Fatalf("observation %d did not report the unhealthy service", observation)
		}
		if runtime.repairCalls != 0 {
			t.Fatalf("repair ran after only %d observations", observation)
		}
	}
	if err := watchdog.Reconcile(context.Background()); err == nil {
		t.Fatal("failed repair was not reported")
	}
	if runtime.repairCalls != 1 {
		t.Fatalf("repair calls = %d, want 1", runtime.repairCalls)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Watchdog.Status != WatchdogObserving || state.Watchdog.ConsecutiveFailures != 0 || len(state.Watchdog.Attempts) != 1 {
		t.Fatalf("unexpected persisted watchdog state: %#v", state.Watchdog)
	}
}

func TestWatchdogEnforcesAttemptBudgetAndCooldown(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	runtime := &fakeWatchdogRuntime{observeErr: errors.New("down"), repairErr: errors.New("still down")}
	watchdog := NewWatchdog(store, runtime)
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	watchdog.now = func() time.Time { return now }

	for attempt := 0; attempt < 3; attempt++ {
		for observation := 0; observation < 3; observation++ {
			_ = watchdog.Reconcile(context.Background())
		}
		now = now.Add(time.Minute)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.repairCalls != 3 || state.Watchdog.Status != WatchdogDegraded || !state.Watchdog.CooldownUntil.Equal(time.Date(2026, 7, 10, 12, 17, 0, 0, time.UTC)) {
		t.Fatalf("restart budget was not enforced: calls=%d state=%#v", runtime.repairCalls, state.Watchdog)
	}
	for observation := 0; observation < 5; observation++ {
		_ = watchdog.Reconcile(context.Background())
	}
	if runtime.repairCalls != 3 {
		t.Fatalf("repair ran during cooldown: %d", runtime.repairCalls)
	}

	now = state.Watchdog.CooldownUntil.Add(time.Second)
	for observation := 0; observation < 3; observation++ {
		_ = watchdog.Reconcile(context.Background())
	}
	if runtime.repairCalls != 4 {
		t.Fatalf("repair did not resume after cooldown: %d", runtime.repairCalls)
	}
}

func TestWatchdogNeverResurrectsIntentionalDesiredStates(t *testing.T) {
	t.Parallel()
	for _, desired := range []DesiredState{DesiredMaintenance, DesiredDeprovisioned, DesiredUpgrading} {
		t.Run(string(desired), func(t *testing.T) {
			var store *StateStore
			if desired == DesiredUpgrading {
				store, _ = seedQueuedExecutionJob(t)
			} else {
				store = NewStateStore(t.TempDir() + "/runtime.json")
				state := NewRuntimeState()
				state.DesiredState = desired
				if err := store.Save(context.Background(), state); err != nil {
					t.Fatal(err)
				}
			}
			runtime := &fakeWatchdogRuntime{observeErr: errors.New("stopped")}
			if err := NewWatchdog(store, runtime).Reconcile(context.Background()); err != nil {
				t.Fatalf("suppressed reconcile: %v", err)
			}
			if runtime.observeCalls != 0 || runtime.repairCalls != 0 {
				t.Fatalf("desired %s touched host: observe=%d repair=%d", desired, runtime.observeCalls, runtime.repairCalls)
			}
			state, _ := store.Load(context.Background())
			if state.Watchdog.Status != WatchdogSuppressed {
				t.Fatalf("desired %s status = %q", desired, state.Watchdog.Status)
			}
		})
	}
}

func TestWatchdogRechecksDesiredStateAfterObservationBeforeRepair(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	runtime := &fakeWatchdogRuntime{observeErr: errors.New("down")}
	runtime.afterObserve = func(observations int) {
		if observations == watchdogObservationThreshold {
			_ = store.Update(context.Background(), func(state *RuntimeState) error {
				state.DesiredState = DesiredMaintenance
				return nil
			})
		}
	}
	watchdog := NewWatchdog(store, runtime)
	for observation := 0; observation < watchdogObservationThreshold; observation++ {
		err := watchdog.Reconcile(context.Background())
		if observation+1 == watchdogObservationThreshold && err != nil {
			t.Fatalf("desired-state suppression returned an error: %v", err)
		}
	}
	if runtime.repairCalls != 0 {
		t.Fatalf("watchdog repaired after desired state changed: %d", runtime.repairCalls)
	}
}

func TestDesiredStateMutationCancelsInFlightWatchdogRepair(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	state := NewRuntimeState()
	state.Watchdog.Status = WatchdogObserving
	state.Watchdog.ConsecutiveFailures = watchdogObservationThreshold - 1
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	gate := NewHostOperationGate()
	repairStarted := make(chan struct{})
	runtime := &fakeWatchdogRuntime{
		observeErr:    errors.New("down"),
		blockRepair:   true,
		repairStarted: repairStarted,
	}
	watchdog := NewWatchdog(store, runtime, gate)
	result := make(chan error, 1)
	go func() { result <- watchdog.Reconcile(context.Background()) }()
	select {
	case <-repairStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog repair did not start")
	}

	service, err := NewService(store, testControlToken, WithHostOperationGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	rejected := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"unknown-plan","idempotency_key":"rejected-during-repair","confirm":"apply_release_change"}`, testControlToken, "")
	if rejected.Code != http.StatusConflict {
		t.Fatalf("invalid job request returned %d: %s", rejected.Code, rejected.Body.String())
	}
	select {
	case <-result:
		t.Fatal("rejected job request cancelled the active repair")
	default:
	}
	request := httptest.NewRequest(http.MethodPost, controlDesiredStatePath, strings.NewReader(`{"desired_state":"maintenance"}`))
	request.Header.Set(controlTokenHeader, testControlToken)
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("desired-state mutation failed: %d %s", response.Code, response.Body.String())
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled watchdog repair did not exit")
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.DesiredState != DesiredMaintenance || state.Watchdog.Status != WatchdogSuppressed {
		t.Fatalf("desired state did not win the repair race: %#v", state)
	}
}

func TestIdempotentJobReplayDoesNotCancelInFlightWatchdogRepair(t *testing.T) {
	t.Parallel()
	initialService, store := newTestService(t)
	const idempotencyKey = "watchdog-idempotent-replay"
	first := postJSON(t, initialService.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"`+idempotencyKey+`","confirm":"apply_release_change"}`, testControlToken, "")
	var firstTicket JobTicket
	decodeResponse(t, first, &firstTicket)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[firstTicket.JobID]
		job.Status = JobSucceeded
		job.CurrentStep = JobStepComplete
		job.CompletedSteps = executionTotalSteps
		job.ServiceAvailable = true
		job.LastSafeVersion = job.TargetVersion
		state.Jobs[job.ID] = job
		state.DesiredState = DesiredRunning
		state.Watchdog.Status = WatchdogObserving
		state.Watchdog.ConsecutiveFailures = watchdogObservationThreshold - 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	gate := NewHostOperationGate()
	repairStarted := make(chan struct{})
	runtime := &fakeWatchdogRuntime{observeErr: errors.New("down"), blockRepair: true, repairStarted: repairStarted}
	watchdog := NewWatchdog(store, runtime, gate)
	result := make(chan error, 1)
	go func() { result <- watchdog.Reconcile(context.Background()) }()
	select {
	case <-repairStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog repair did not start")
	}
	service, err := NewService(store, testControlToken, WithHostOperationGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	replay := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"`+idempotencyKey+`","confirm":"apply_release_change"}`, testControlToken, "")
	var replayTicket JobTicket
	decodeResponse(t, replay, &replayTicket)
	if replayTicket.JobID != firstTicket.JobID {
		t.Fatalf("idempotent replay changed job: first=%s replay=%s", firstTicket.JobID, replayTicket.JobID)
	}
	select {
	case <-result:
		t.Fatal("idempotent replay cancelled the active repair")
	default:
	}

	request := httptest.NewRequest(http.MethodPost, controlDesiredStatePath, strings.NewReader(`{"desired_state":"maintenance"}`))
	request.Header.Set(controlTokenHeader, testControlToken)
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("cleanup desired-state mutation failed: %d %s", response.Code, response.Body.String())
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled watchdog repair did not exit")
	}
}

func TestUpgradeJobCreationCancelsInFlightWatchdogRepair(t *testing.T) {
	t.Parallel()
	_, store := newTestService(t)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		state.Watchdog.Status = WatchdogObserving
		state.Watchdog.ConsecutiveFailures = watchdogObservationThreshold - 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	gate := NewHostOperationGate()
	repairStarted := make(chan struct{})
	runtime := &fakeWatchdogRuntime{
		observeErr:    errors.New("down"),
		blockRepair:   true,
		repairStarted: repairStarted,
	}
	watchdog := NewWatchdog(store, runtime, gate)
	result := make(chan error, 1)
	go func() { result <- watchdog.Reconcile(context.Background()) }()
	select {
	case <-repairStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog repair did not start")
	}
	service, err := NewService(store, testControlToken, WithHostOperationGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	response := postJSON(t, service.Handler(), controlJobsPath, `{"plan_token":"test-plan-token","idempotency_key":"watchdog-gate-job","confirm":"apply_release_change"}`, testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("upgrade job was not accepted after cancelling repair: %d %s", response.Code, response.Body.String())
	}
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled watchdog repair did not exit")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.DesiredState != DesiredUpgrading || state.Watchdog.Status != WatchdogSuppressed || countActiveJobs(state) != 1 {
		t.Fatalf("upgrade job did not win the repair race: %#v", state)
	}
}

func TestAbandonedMutationRestoresInterruptedRepairBudget(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	state := NewRuntimeState()
	state.Watchdog.Status = WatchdogObserving
	state.Watchdog.ConsecutiveFailures = watchdogObservationThreshold - 1
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	gate := NewHostOperationGate()
	repairStarted := make(chan struct{})
	runtime := &fakeWatchdogRuntime{observeErr: errors.New("down"), blockRepair: true, repairStarted: repairStarted}
	watchdog := NewWatchdog(store, runtime, gate)
	result := make(chan error, 1)
	go func() { result <- watchdog.Reconcile(context.Background()) }()
	select {
	case <-repairStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog repair did not start")
	}
	releaseMutation := gate.BeginMutation()
	releaseMutation()
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted repair did not exit")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.DesiredState != DesiredRunning || state.Watchdog.Status != WatchdogObserving || len(state.Watchdog.Attempts) != 0 || !state.Watchdog.LastRepairAt.IsZero() {
		t.Fatalf("abandoned mutation consumed repair budget: %#v", state.Watchdog)
	}
}

func TestWatchdogHealthyObservationClearsDegradedState(t *testing.T) {
	t.Parallel()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	state := NewRuntimeState()
	state.Watchdog = WatchdogState{
		Status:              WatchdogDegraded,
		ConsecutiveFailures: 3,
		Attempts:            []time.Time{time.Date(2026, 7, 10, 11, 59, 0, 0, time.UTC)},
		CooldownUntil:       time.Date(2026, 7, 10, 12, 15, 0, 0, time.UTC),
		ErrorCode:           "repair_failed",
	}
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	runtime := &fakeWatchdogRuntime{}
	if err := NewWatchdog(store, runtime).Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Load(context.Background())
	if state.Watchdog.Status != WatchdogHealthy || state.Watchdog.ConsecutiveFailures != 0 || !state.Watchdog.CooldownUntil.IsZero() || state.Watchdog.ErrorCode != "" {
		t.Fatalf("healthy observation did not clear degraded state: %#v", state.Watchdog)
	}
}

func TestWatchdogDockerEventTriggersReconciliation(t *testing.T) {
	store := NewStateStore(t.TempDir() + "/runtime.json")
	ready := make(chan struct{})
	runtime := &fakeWatchdogRuntime{eventReady: ready}
	watchdog := NewWatchdog(store, runtime)
	watchdog.reconcileInterval = time.Hour
	watchdog.eventRetryInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchdog.Run(ctx)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not start Docker event stream")
	}
	runtime.emitEvent()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.observations() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Docker event did not trigger reconciliation")
}

type fakeWatchdogRuntime struct {
	mu            sync.Mutex
	observeErr    error
	repairErr     error
	observeCalls  int
	repairCalls   int
	eventReady    chan struct{}
	events        chan struct{}
	afterObserve  func(int)
	blockRepair   bool
	repairStarted chan struct{}
}

func (runtime *fakeWatchdogRuntime) ObserveWatchdog(context.Context) error {
	runtime.mu.Lock()
	runtime.observeCalls++
	observations := runtime.observeCalls
	err := runtime.observeErr
	afterObserve := runtime.afterObserve
	runtime.mu.Unlock()
	if afterObserve != nil {
		afterObserve(observations)
	}
	return err
}

func (runtime *fakeWatchdogRuntime) RepairWatchdog(ctx context.Context) error {
	runtime.mu.Lock()
	runtime.repairCalls++
	block := runtime.blockRepair
	started := runtime.repairStarted
	err := runtime.repairErr
	if started != nil {
		close(started)
		runtime.repairStarted = nil
	}
	runtime.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func (runtime *fakeWatchdogRuntime) StreamWatchdogEvents(ctx context.Context, notify func()) error {
	runtime.mu.Lock()
	if runtime.events == nil {
		runtime.events = make(chan struct{}, 1)
	}
	events := runtime.events
	ready := runtime.eventReady
	runtime.eventReady = nil
	runtime.mu.Unlock()
	if ready != nil {
		close(ready)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-events:
			notify()
		}
	}
}

func (runtime *fakeWatchdogRuntime) emitEvent() {
	runtime.mu.Lock()
	events := runtime.events
	runtime.mu.Unlock()
	events <- struct{}{}
}

func (runtime *fakeWatchdogRuntime) observations() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.observeCalls
}
