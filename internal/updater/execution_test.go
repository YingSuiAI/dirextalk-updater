package updater

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobEngineRunsBackupActivationAndHealthInOrder(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{}
	engine := NewJobEngine(store, runtime)

	if err := engine.RunActive(context.Background()); err != nil {
		t.Fatalf("run active job: %v", err)
	}
	if want := []string{"prepare_backup", "activate_target", "check_target"}; !reflect.DeepEqual(runtime.calls, want) {
		t.Fatalf("runtime call order = %#v, want %#v", runtime.calls, want)
	}
	if want := []JobStatus{JobValidating, JobBackingUp, JobPulling, JobStopping, JobMigrating, JobStarting}; !reflect.DeepEqual(runtime.phases, want) {
		t.Fatalf("persisted phase sequence = %#v, want %#v", runtime.phases, want)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobSucceeded || job.CurrentStep != JobStepComplete || job.CompletedSteps != executionTotalSteps || !job.ServiceAvailable {
		t.Fatalf("unexpected completed job: %#v", job)
	}
	if state.DesiredState != DesiredRunning {
		t.Fatalf("desired state = %q, want running", state.DesiredState)
	}
}

func TestJobEngineExecutesEveryPersistedReleaseHopAndRotatesBackup(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedMultiHopExecutionJob(t)
	runtime := &fakeUpgradeRuntime{digestByVersion: map[string]string{
		"v1.0.0": "sha256:" + strings.Repeat("0", 64),
		"v1.1.0": "sha256:" + strings.Repeat("a", 64),
	}}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err != nil {
		t.Fatalf("run multi-hop job: %v", err)
	}
	wantCalls := []string{"prepare_backup", "activate_target", "check_target", "prepare_backup", "activate_target", "check_target"}
	if !reflect.DeepEqual(runtime.calls, wantCalls) {
		t.Fatalf("multi-hop calls = %#v, want %#v", runtime.calls, wantCalls)
	}
	if !reflect.DeepEqual(runtime.backupVersions, []string{"v1.0.0", "v1.1.0"}) || !reflect.DeepEqual(runtime.targetVersions, []string{"v1.1.0", "v1.2.0"}) {
		t.Fatalf("unexpected hop identities: backups=%#v targets=%#v", runtime.backupVersions, runtime.targetVersions)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobSucceeded || job.CurrentVersion != "v1.2.0" || job.LastSafeVersion != "v1.2.0" || job.CurrentHop != 2 || job.TotalHops != 2 || job.CompletedSteps != 2*executionTotalSteps {
		t.Fatalf("unexpected multi-hop completion: %#v", job)
	}
	if job.RecoveryPoint == nil || job.RecoveryPoint.Version != "v1.1.0" {
		t.Fatalf("final rollback slot must restore most recent safe hop: %#v", job.RecoveryPoint)
	}
}

func TestJobEngineSecondHopFailureRollsBackToMostRecentSafeHop(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedMultiHopExecutionJob(t)
	runtime := &fakeUpgradeRuntime{
		digestByVersion:   map[string]string{"v1.0.0": "sha256:" + strings.Repeat("0", 64), "v1.1.0": "sha256:" + strings.Repeat("a", 64)},
		checkTargetErrors: map[string]error{"v1.2.0": errors.New("second hop unhealthy")},
	}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected second-hop failure")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobRolledBack || job.LastSafeVersion != "v1.1.0" || job.CurrentVersion != "v1.1.0" || job.RecoveryPoint == nil || job.RecoveryPoint.Version != "v1.1.0" {
		t.Fatalf("second hop did not roll back to latest safe release: %#v", job)
	}
}

func TestJobEngineRejectsObservedSourceDigestBeforeTargetMutation(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{digestByVersion: map[string]string{"v1.0.0": "sha256:" + strings.Repeat("f", 64)}}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected untrusted source digest rejection")
	}
	if !reflect.DeepEqual(runtime.calls, []string{"prepare_backup"}) {
		t.Fatalf("target mutation was attempted: %#v", runtime.calls)
	}
	state, _ := store.Load(context.Background())
	job := state.Jobs[jobID]
	if job.Status != JobFailed || job.ErrorCode != "source_image_digest_untrusted" || !job.ServiceAvailable {
		t.Fatalf("source digest mismatch did not fail closed: %#v", job)
	}
}

func TestJobEngineFailsClosedWhenLegacyPersistedPlanResumesAfterBackup(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[jobID]
		plan := state.Plans[job.PlanTokenHash]
		plan.ReleaseChain[0].SourceImageDigests = nil
		plan.LegacyUnbound = true
		state.Plans[job.PlanTokenHash] = plan
		job.Status = JobPulling
		job.CurrentStep = JobStepPulling
		job.CompletedSteps = 2
		job.RecoveryPoint = &BackupMetadata{
			SchemaVersion: BackupMetadataSchemaVersion, JobID: job.ID, Version: job.CurrentVersion,
			ImageDigest: "sha256:" + strings.Repeat("1", 64), ImageRef: AllowedImageRepository + ":v1.0.0@sha256:" + strings.Repeat("1", 64),
			DatabaseSchema: 1, SchemaCompatVersion: 1, CreatedAt: time.Now().UTC(),
			Artifacts: []BackupArtifact{{Name: "message-config.tar", Size: 1, SHA256: strings.Repeat("a", 64)}, {Name: "message-data.tar", Size: 1, SHA256: strings.Repeat("b", 64)}, {Name: "p2p.tar", Size: 1, SHA256: strings.Repeat("c", 64)}, {Name: "postgres.dump", Size: 1, SHA256: strings.Repeat("d", 64)}},
		}
		state.Jobs[jobID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &fakeUpgradeRuntime{}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected legacy unbound release path to fail closed")
	}
	if len(runtime.calls) != 0 {
		t.Fatalf("legacy unbound path reached host mutation: %#v", runtime.calls)
	}
	state, _ := store.Load(context.Background())
	if job := state.Jobs[jobID]; job.Status != JobFailed || job.ErrorCode != "source_image_digest_untrusted" || !job.ServiceAvailable {
		t.Fatalf("legacy path was not stopped safely: %#v", job)
	}
}

func TestJobProgressReportsPlannedDowntimeDuringBackup(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	engine := NewJobEngine(store, &fakeUpgradeRuntime{})
	progress := engine.progress(context.Background(), jobID)
	if err := progress(JobBackingUp); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Load(context.Background())
	if state.Jobs[jobID].ServiceAvailable {
		t.Fatal("backing_up reported message-server available while snapshot may have stopped it")
	}
	if err := progress(JobPulling); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Load(context.Background())
	if !state.Jobs[jobID].ServiceAvailable {
		t.Fatal("pulling did not report the health-confirmed source service available")
	}
}

func TestJobEngineAutomaticallyRollsBackAfterTargetHealthFailure(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{checkTargetErr: errors.New("new service unhealthy")}
	engine := NewJobEngine(store, runtime)

	if err := engine.RunActive(context.Background()); err == nil {
		t.Fatal("expected target health failure to be reported")
	}
	wantCalls := []string{"prepare_backup", "activate_target", "check_target", "restore_backup", "check_restored"}
	if !reflect.DeepEqual(runtime.calls, wantCalls) {
		t.Fatalf("runtime calls = %#v, want %#v", runtime.calls, wantCalls)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobRolledBack || job.CurrentStep != JobStepComplete || !job.ServiceAvailable {
		t.Fatalf("unexpected rolled back job: %#v", job)
	}
	if job.ErrorCode != "target_health_failed" || job.ErrorMessage == "new service unhealthy" {
		t.Fatalf("job must expose a stable safe error, got %#v", job)
	}
	if job.LastSafeVersion != "v1.0.0" || state.DesiredState != DesiredRunning {
		t.Fatalf("rollback did not restore safe state: %#v / %q", job, state.DesiredState)
	}
}

func TestJobEnginePersistsRollbackCheckpointAndResumesAfterRestart(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	firstRuntime := &fakeUpgradeRuntime{
		checkTargetErr: errors.New("unhealthy"),
		restoreErr:     errors.New("host restarted while restoring"),
	}
	if err := NewJobEngine(store, firstRuntime).RunActive(context.Background()); err == nil {
		t.Fatal("expected interrupted rollback")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobRollingBack || job.CurrentStep != JobStepRestoreBackup || job.RecoveryPoint == nil {
		t.Fatalf("rollback checkpoint was not persisted: %#v", job)
	}

	secondRuntime := &fakeUpgradeRuntime{}
	if err := NewJobEngine(store, secondRuntime).RunActive(context.Background()); err != nil {
		t.Fatalf("resume rollback: %v", err)
	}
	if want := []string{"restore_backup", "check_restored"}; !reflect.DeepEqual(secondRuntime.calls, want) {
		t.Fatalf("resume calls = %#v, want %#v", secondRuntime.calls, want)
	}
	state, _ = store.Load(context.Background())
	if state.Jobs[jobID].Status != JobRolledBack || state.DesiredState != DesiredRunning {
		t.Fatalf("rollback resume did not finish: %#v", state.Jobs[jobID])
	}
}

func TestJobEngineStopsAutomaticRollbackAfterThreeFailuresAndOffersManualRecovery(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{
		checkTargetErr: errors.New("unhealthy"),
		restoreErr:     errors.New("restore failed"),
	}
	engine := NewJobEngine(store, runtime)
	for attempt := 0; attempt < maxRecoveryAttempts; attempt++ {
		if err := engine.RunActive(context.Background()); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	job := state.Jobs[jobID]
	if job.Status != JobFailed || job.RecoveryAttempts != maxRecoveryAttempts || job.ServiceAvailable || state.DesiredState != DesiredMaintenance {
		t.Fatalf("automatic rollback did not fail closed: %#v / %q", job, state.DesiredState)
	}
	if want := []JobOperation{{Kind: "rollback"}, {Kind: "restart"}}; !reflect.DeepEqual(publicJobOperations(job), want) {
		t.Fatalf("manual recovery operations = %#v, want %#v", publicJobOperations(job), want)
	}
}

func TestJobEngineBackupFailureLeavesServiceRunningWithoutRollback(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{prepareErr: errors.New("pg dump invalid")}

	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected backup failure")
	}
	if want := []string{"prepare_backup"}; !reflect.DeepEqual(runtime.calls, want) {
		t.Fatalf("runtime calls = %#v, want %#v", runtime.calls, want)
	}
	state, _ := store.Load(context.Background())
	job := state.Jobs[jobID]
	if job.Status != JobFailed || !job.ServiceAvailable || job.ErrorCode != "backup_failed" || state.DesiredState != DesiredRunning {
		t.Fatalf("unsafe backup failure state: %#v / %q", job, state.DesiredState)
	}
}

func TestJobEnginePreservesUntrustedSourceDigestErrorCodeFromRuntime(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{prepareErr: errUntrustedSourceImageDigest}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected source digest precheck failure")
	}
	state, _ := store.Load(context.Background())
	if job := state.Jobs[jobID]; job.Status != JobFailed || job.ErrorCode != "source_image_digest_untrusted" || !job.ServiceAvailable {
		t.Fatalf("source digest error code was lost: %#v", job)
	}
}

func TestJobEngineExposesRestartWhenSourceRecoveryFailsDuringBackup(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{prepareErr: serviceUnavailableError{cause: errors.New("source stayed down")}}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected source recovery failure")
	}
	state, _ := store.Load(context.Background())
	job := state.Jobs[jobID]
	if job.Status != JobFailed || job.ServiceAvailable || state.DesiredState != DesiredRunning {
		t.Fatalf("source recovery failure was misreported: %#v / %q", job, state.DesiredState)
	}
	if want := []JobOperation{{Kind: "restart"}}; !reflect.DeepEqual(publicJobOperations(job), want) {
		t.Fatalf("source recovery did not offer restart: %#v", publicJobOperations(job))
	}
}

func TestJobEngineDoesNotRollbackWhenTargetPullFailsBeforeHostMutation(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{activateErr: errors.New("registry unavailable")}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err == nil {
		t.Fatal("expected target pull failure")
	}
	if want := []string{"prepare_backup", "activate_target"}; !reflect.DeepEqual(runtime.calls, want) {
		t.Fatalf("pre-mutation failure ran rollback: calls=%#v", runtime.calls)
	}
	state, _ := store.Load(context.Background())
	job := state.Jobs[jobID]
	if job.Status != JobFailed || !job.ServiceAvailable || state.DesiredState != DesiredRunning {
		t.Fatalf("pre-mutation failure did not preserve running source: %#v / %q", job, state.DesiredState)
	}
}

func TestPublicJobOnlyOffersRecoveryOperationsSupportedByPersistedState(t *testing.T) {
	t.Parallel()
	withoutBackup := Job{Status: JobFailed, ServiceAvailable: true}
	if operations := publicJobOperations(withoutBackup); len(operations) != 0 {
		t.Fatalf("unsafe operations without recovery point: %#v", operations)
	}
	withBackup := Job{
		Status:           JobFailed,
		ServiceAvailable: false,
		RecoveryPoint:    &BackupMetadata{},
	}
	operations := publicJobOperations(withBackup)
	if want := []JobOperation{{Kind: "rollback"}, {Kind: "restart"}}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("operations = %#v, want %#v", operations, want)
	}
	succeeded := Job{Status: JobSucceeded, ServiceAvailable: true, RecoveryPoint: &BackupMetadata{}}
	if want := []JobOperation{{Kind: "rollback"}}; !reflect.DeepEqual(publicJobOperations(succeeded), want) {
		t.Fatalf("successful upgrade did not retain one-step rollback: %#v", publicJobOperations(succeeded))
	}
}

func TestServiceJobWorkerResumesPersistedJobOnStartup(t *testing.T) {
	store, jobID := seedQueuedExecutionJob(t)
	runtime := &fakeUpgradeRuntime{}
	engine := NewJobEngine(store, runtime)
	service, err := NewService(store, "control-token", WithJobEngine(engine))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go service.RunJobs(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if state.Jobs[jobID].Status == JobSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("persisted job was not resumed by the service worker")
}

func TestServiceJobWorkerRecordsExecutionFailuresForOperators(t *testing.T) {
	store, jobID := seedQueuedExecutionJob(t)
	engine := NewJobEngine(store, &fakeUpgradeRuntime{prepareErr: errors.New("runner failed")})
	var mutex sync.Mutex
	logs := []string{}
	service, err := NewService(store, "control-token", WithJobEngine(engine), WithLogger(func(format string, args ...any) {
		mutex.Lock()
		defer mutex.Unlock()
		logs = append(logs, fmt.Sprintf(format, args...))
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go service.RunJobs(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		mutex.Lock()
		logged := len(logs) > 0
		mutex.Unlock()
		if state.Jobs[jobID].Status == JobFailed && logged {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job failure was not persisted and logged")
}

func TestJobEngineCompletesPersistedRestartOperation(t *testing.T) {
	t.Parallel()
	store, jobID := seedQueuedExecutionJob(t)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[jobID]
		job.Status = JobRestarting
		job.CurrentStep = JobStepRestart
		job.ServiceAvailable = false
		job.ErrorCode = "backup_failed"
		job.ErrorMessage = "safe failure"
		state.Jobs[jobID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &fakeUpgradeRuntime{}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err != nil {
		t.Fatalf("resume restart: %v", err)
	}
	if want := []string{"restart_current"}; !reflect.DeepEqual(runtime.calls, want) {
		t.Fatalf("restart calls = %#v, want %#v", runtime.calls, want)
	}
	state, _ := store.Load(context.Background())
	job := state.Jobs[jobID]
	if job.Status != JobFailed || !job.ServiceAvailable || state.DesiredState != DesiredRunning {
		t.Fatalf("restart completion was not persisted: %#v / %q", job, state.DesiredState)
	}
}

type fakeUpgradeRuntime struct {
	calls             []string
	phases            []JobStatus
	prepareErr        error
	activateErr       error
	activateMutated   bool
	checkTargetErr    error
	restoreErr        error
	checkRestoreErr   error
	digestByVersion   map[string]string
	checkTargetErrors map[string]error
	backupVersions    []string
	targetVersions    []string
}

func (runtime *fakeUpgradeRuntime) PrepareBackup(_ context.Context, job Job, _ Plan, progress func(JobStatus) error) (BackupMetadata, error) {
	runtime.calls = append(runtime.calls, "prepare_backup")
	runtime.backupVersions = append(runtime.backupVersions, job.CurrentVersion)
	if err := progress(JobValidating); err != nil {
		return BackupMetadata{}, err
	}
	runtime.phases = append(runtime.phases, JobValidating)
	if err := progress(JobBackingUp); err != nil {
		return BackupMetadata{}, err
	}
	runtime.phases = append(runtime.phases, JobBackingUp)
	if runtime.prepareErr != nil {
		return BackupMetadata{}, runtime.prepareErr
	}
	digest := "sha256:" + strings.Repeat("1", 64)
	if runtime.digestByVersion != nil && runtime.digestByVersion[job.CurrentVersion] != "" {
		digest = runtime.digestByVersion[job.CurrentVersion]
	}
	return BackupMetadata{
		SchemaVersion:       BackupMetadataSchemaVersion,
		JobID:               job.ID,
		Version:             job.CurrentVersion,
		ImageDigest:         digest,
		ImageRef:            AllowedImageRepository + ":" + job.CurrentVersion + "@" + digest,
		DatabaseSchema:      1,
		SchemaCompatVersion: 1,
		CreatedAt:           time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		Artifacts: []BackupArtifact{
			{Name: "message-config.tar", Size: 1, SHA256: strings.Repeat("a", 64)},
			{Name: "message-data.tar", Size: 1, SHA256: strings.Repeat("b", 64)},
			{Name: "p2p.tar", Size: 1, SHA256: strings.Repeat("c", 64)},
			{Name: "postgres.dump", Size: 1, SHA256: strings.Repeat("d", 64)},
		},
	}, nil
}

func (runtime *fakeUpgradeRuntime) ActivateTarget(_ context.Context, manifest Manifest, progress func(JobStatus) error) error {
	runtime.calls = append(runtime.calls, "activate_target")
	runtime.targetVersions = append(runtime.targetVersions, manifest.Version)
	if runtime.activateErr != nil && !runtime.activateMutated {
		return runtime.activateErr
	}
	for _, phase := range []JobStatus{JobPulling, JobStopping, JobMigrating, JobStarting} {
		if err := progress(phase); err != nil {
			return err
		}
		runtime.phases = append(runtime.phases, phase)
	}
	if runtime.activateErr != nil {
		return hostMutationError{cause: runtime.activateErr, mutated: true}
	}
	return nil
}

func (runtime *fakeUpgradeRuntime) CheckTarget(_ context.Context, manifest Manifest) error {
	runtime.calls = append(runtime.calls, "check_target")
	if runtime.checkTargetErrors != nil && runtime.checkTargetErrors[manifest.Version] != nil {
		return runtime.checkTargetErrors[manifest.Version]
	}
	return runtime.checkTargetErr
}

func (runtime *fakeUpgradeRuntime) RestoreBackup(_ context.Context, _ BackupMetadata) error {
	runtime.calls = append(runtime.calls, "restore_backup")
	return runtime.restoreErr
}

func (runtime *fakeUpgradeRuntime) CheckRestored(_ context.Context, _ BackupMetadata) error {
	runtime.calls = append(runtime.calls, "check_restored")
	return runtime.checkRestoreErr
}

func (runtime *fakeUpgradeRuntime) RestartCurrent(_ context.Context, _ Job) error {
	runtime.calls = append(runtime.calls, "restart_current")
	return nil
}

func seedQueuedExecutionJob(t *testing.T) (*StateStore, string) {
	t.Helper()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	state := NewRuntimeState()
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	planHash := strings.Repeat("a", 64)
	jobID := "job_execution"
	manifestDigest := canonicalManifestDigest(manifest)
	state.Plans[planHash] = Plan{
		Manifest:       manifest,
		ManifestDigest: manifestDigest,
		CurrentVersion: "v1.0.0",
		ReleaseChain:   []ReleaseStep{{Manifest: manifest, ManifestDigest: manifestDigest, SourceImageDigests: []string{"sha256:" + strings.Repeat("1", 64)}}},
		ExpiresAt:      time.Now().Add(time.Hour),
	}
	state.Jobs[jobID] = Job{
		ID:                jobID,
		Status:            JobQueued,
		PlanTokenHash:     planHash,
		ManifestDigest:    manifestDigest,
		BearerTokenHashes: []string{strings.Repeat("c", 64)},
		IdempotencyKey:    "execution-test",
		CurrentVersion:    "v1.0.0",
		TargetVersion:     "v1.1.0",
		CurrentStep:       JobStepValidate,
		TotalSteps:        executionTotalSteps,
		TotalHops:         1,
		ServiceAvailable:  true,
		LastSafeVersion:   "v1.0.0",
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	state.Idempotency["execution-test"] = jobID
	state.DesiredState = DesiredUpgrading
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	return store, jobID
}

func seedQueuedMultiHopExecutionJob(t *testing.T) (*StateStore, string) {
	t.Helper()
	store := NewStateStore(t.TempDir() + "/runtime.json")
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	chain := mustUpgradePath(t, index, "v1.0.0")
	state := NewRuntimeState()
	planHash := strings.Repeat("d", 64)
	jobID := "job_multihop"
	target := chain[len(chain)-1]
	state.Plans[planHash] = Plan{Manifest: target.Manifest, ManifestDigest: target.ManifestDigest, CurrentVersion: "v1.0.0", ReleaseChain: chain, ExpiresAt: time.Now().Add(time.Hour)}
	state.Jobs[jobID] = Job{
		ID: jobID, Status: JobQueued, PlanTokenHash: planHash, ManifestDigest: target.ManifestDigest,
		BearerTokenHashes: []string{strings.Repeat("e", 64)}, IdempotencyKey: "execution-multihop",
		CurrentVersion: "v1.0.0", TargetVersion: target.Manifest.Version, CurrentStep: JobStepValidate,
		TotalSteps: len(chain) * executionTotalSteps, TotalHops: len(chain), ServiceAvailable: true,
		LastSafeVersion: "v1.0.0", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	state.Idempotency["execution-multihop"] = jobID
	state.DesiredState = DesiredUpgrading
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	return store, jobID
}
