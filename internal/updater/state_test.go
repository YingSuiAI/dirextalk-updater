package updater

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStateStoreAtomicallyReplacesPrivateState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "runtime.json")
	store := NewStateStore(path)
	first := NewRuntimeState()
	first.DesiredState = DesiredMaintenance
	if err := store.Save(context.Background(), first); err != nil {
		t.Fatalf("save first state: %v", err)
	}
	second := NewRuntimeState()
	second.DesiredState = DesiredRunning
	if err := store.Save(context.Background(), second); err != nil {
		t.Fatalf("save second state: %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loaded.DesiredState != DesiredRunning {
		t.Fatalf("expected replacement state, got %#v", loaded)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("state must be private, mode=%#o", info.Mode().Perm())
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".runtime.json.tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary state files remain: %v err=%v", matches, err)
	}
}

func TestRuntimeStateRejectsUnknownFieldsAndInvalidDesiredState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"desired_state":"running","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStateStore(path).Load(context.Background()); err == nil {
		t.Fatal("expected unknown state field rejection")
	}
	state := NewRuntimeState()
	state.DesiredState = DesiredState("restart-everything")
	if err := NewStateStore(path).Save(context.Background(), state); err == nil {
		t.Fatal("expected invalid desired state rejection")
	}
}

func TestRuntimeStateRejectsFreshDiscoveryWithoutCheckedAt(t *testing.T) {
	manifestData := []byte(validManifestJSON())
	manifest, err := ValidateManifest(manifestData)
	if err != nil {
		t.Fatal(err)
	}
	state := NewRuntimeState()
	state.Discovery = DiscoveryCache{Status: DiscoveryFresh, Manifest: &manifest, ManifestDigest: manifestDigest(manifestData)}
	if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
		t.Fatal("fresh discovery without checked_at was accepted")
	}
}

func TestRuntimeStateRejectsInvalidUpgradingAndActiveJobCombinations(t *testing.T) {
	state := NewRuntimeState()
	state.DesiredState = DesiredUpgrading
	if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
		t.Fatal("upgrading without an active job was accepted")
	}
}

func TestRuntimeStateStillReadsLegacyDigestBoundDirectJob(t *testing.T) {
	store, jobID := seedQueuedDirectExecutionJob(t)
	if err := store.Update(context.Background(), func(state *RuntimeState) error {
		job := state.Jobs[jobID]
		job.DirectRelease.ImageDigest = "sha256:" + strings.Repeat("a", 64)
		state.Jobs[jobID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load legacy digest-bound direct job: %v", err)
	}
	if got := loaded.Jobs[jobID].DirectRelease; got == nil || got.ImageDigest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("legacy direct target was not preserved: %#v", got)
	}
}

func TestRuntimeStateRejectsInvalidPersistedPlanAndJobState(t *testing.T) {
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("manifest", func(t *testing.T) {
		state := NewRuntimeState()
		invalidManifest := manifest
		invalidManifest.ImageDigest = "sha256:attacker"
		state.Plans[tokenHash("plan")] = Plan{Manifest: invalidManifest, ManifestDigest: manifestDigest([]byte(validManifestJSON())), CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
		if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
			t.Fatal("expected invalid persisted manifest to be rejected")
		}
	})
	t.Run("job status", func(t *testing.T) {
		state := NewRuntimeState()
		planHash := tokenHash("plan")
		state.Plans[planHash] = Plan{Manifest: manifest, ManifestDigest: manifestDigest([]byte(validManifestJSON())), CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
		state.Jobs["job_1"] = Job{ID: "job_1", Status: JobStatus("run_shell"), PlanTokenHash: planHash, ManifestDigest: manifestDigest([]byte(validManifestJSON())), BearerTokenHashes: []string{tokenHash("bearer")}, IdempotencyKey: "request-1", TargetVersion: manifest.Version}
		state.Idempotency["request-1"] = "job_1"
		if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
			t.Fatal("expected invalid persisted job state to be rejected")
		}
	})
	t.Run("job target", func(t *testing.T) {
		state := NewRuntimeState()
		planHash := tokenHash("plan")
		digest := manifestDigest([]byte(validManifestJSON()))
		state.Plans[planHash] = Plan{Manifest: manifest, ManifestDigest: digest, CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
		state.Jobs["job_1"] = Job{ID: "job_1", Status: JobQueued, PlanTokenHash: planHash, ManifestDigest: digest, BearerTokenHashes: []string{tokenHash("bearer")}, IdempotencyKey: "request-1", TargetVersion: "v9.0.0"}
		state.Idempotency["request-1"] = "job_1"
		if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
			t.Fatal("expected a job target inconsistent with its plan to be rejected")
		}
	})
	t.Run("job current version", func(t *testing.T) {
		state := NewRuntimeState()
		planHash := tokenHash("plan")
		digest := manifestDigest([]byte(validManifestJSON()))
		state.Plans[planHash] = Plan{Manifest: manifest, ManifestDigest: digest, CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
		state.Jobs["job_1"] = Job{ID: "job_1", Status: JobQueued, PlanTokenHash: planHash, ManifestDigest: digest, BearerTokenHashes: []string{tokenHash("bearer")}, IdempotencyKey: "request-1", CurrentVersion: "v0.9.0", TargetVersion: manifest.Version}
		state.Idempotency["request-1"] = "job_1"
		if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
			t.Fatal("expected a job current version inconsistent with its plan to be rejected")
		}
	})
}

func TestRuntimeStateRejectsIntermediateManifestDigestDrift(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	chain := mustUpgradePath(t, index, "v1.0.0")
	chain[0].Manifest.SchemaVersion++
	target := chain[len(chain)-1]
	state := NewRuntimeState()
	state.Plans[tokenHash("drifted-chain")] = Plan{
		Manifest: target.Manifest, ManifestDigest: target.ManifestDigest, CurrentVersion: "v1.0.0",
		ReleaseChain: chain, ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := NewStateStore(filepath.Join(t.TempDir(), "state.json")).Save(context.Background(), state); err == nil {
		t.Fatal("intermediate manifest drift was accepted without matching its bound digest")
	}
}

func TestStateStoreMigratesSchemaOneQueuedJobCurrentVersion(t *testing.T) {
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	digest := manifestDigest([]byte(validManifestJSON()))
	planHash := tokenHash("plan")
	legacy := NewRuntimeState()
	legacy.SchemaVersion = 1
	legacy.Plans[planHash] = Plan{Manifest: manifest, ManifestDigest: digest, CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
	legacy.Jobs["job_1"] = Job{ID: "job_1", Status: JobQueued, PlanTokenHash: planHash, ManifestDigest: digest, BearerTokenHashes: []string{tokenHash("bearer")}, IdempotencyKey: "request-1", TargetVersion: manifest.Version}
	legacy.Idempotency["request-1"] = "job_1"
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "runtime.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	migrated, err := NewStateStore(path).Load(context.Background())
	if err != nil {
		t.Fatalf("load schema one state: %v", err)
	}
	if migrated.SchemaVersion != RuntimeStateSchemaVersion || RuntimeStateSchemaVersion != 6 {
		t.Fatalf("state schema was not upgraded: %d", migrated.SchemaVersion)
	}
	if migrated.Watchdog.Status != WatchdogUnknown {
		t.Fatalf("watchdog state was not initialized during migration: %#v", migrated.Watchdog)
	}
	job := migrated.Jobs["job_1"]
	if job.CurrentVersion != "v1.0.0" || job.CurrentStep != JobStepValidate || job.TotalSteps != executionTotalSteps || job.TotalHops != 1 {
		t.Fatalf("queued job version edge was not restored: %#v", migrated.Jobs["job_1"])
	}
	migratedPlan := migrated.Plans[planHash]
	if !migratedPlan.LegacyUnbound || len(migratedPlan.ReleaseChain) != 1 || migratedPlan.ReleaseChain[0].ManifestDigest != canonicalManifestDigest(manifest) || len(migratedPlan.ReleaseChain[0].SourceImageDigests) != 0 {
		t.Fatalf("legacy single-manifest plan was not explicitly migrated fail-closed: %#v", migratedPlan)
	}
}

func TestStateStoreMigratesSchemaFourSucceededJobProgress(t *testing.T) {
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	digest := canonicalManifestDigest(manifest)
	planHash := tokenHash("schema-four-plan")
	legacy := NewRuntimeState()
	legacy.SchemaVersion = 4
	legacy.Plans[planHash] = Plan{Manifest: manifest, ManifestDigest: digest, CurrentVersion: "v1.0.0", ExpiresAt: time.Now().Add(time.Hour)}
	legacy.Jobs["job_schema4"] = Job{
		ID: "job_schema4", Status: JobSucceeded, PlanTokenHash: planHash, ManifestDigest: digest,
		BearerTokenHashes: []string{tokenHash("bearer")}, IdempotencyKey: "schema-four-request",
		CurrentVersion: "v1.0.0", TargetVersion: manifest.Version, CurrentStep: JobStepComplete,
		CompletedSteps: executionTotalSteps, TotalSteps: executionTotalSteps, ServiceAvailable: true,
		LastSafeVersion: manifest.Version, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	legacy.Idempotency["schema-four-request"] = "job_schema4"
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "runtime.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	migrated, err := NewStateStore(path).Load(context.Background())
	if err != nil {
		t.Fatalf("load schema four state: %v", err)
	}
	job := migrated.Jobs["job_schema4"]
	if job.CurrentHop != 1 || job.TotalHops != 1 || job.CurrentVersion != manifest.Version || job.CompletedSteps != job.TotalSteps {
		t.Fatalf("succeeded job progress was not migrated: %#v", job)
	}
}

func TestStateStoreMigratesLegacyActiveJobWithoutDiscoveryOrUnreferencedPlans(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validSingleReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	chain := mustUpgradePath(t, index, "v1.0.0")
	planHash := tokenHash("legacy-active-plan")
	dropHash := tokenHash("unreferenced-plan")
	target := chain[len(chain)-1]
	now := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	legacy := NewRuntimeState()
	legacy.SchemaVersion = 5
	legacy.DesiredState = DesiredUpgrading
	legacy.Discovery = testDiscoveryCache(t, []byte(validSingleReleaseIndexJSON(t)), DiscoveryFresh, now)
	legacy.Plans[planHash] = Plan{
		Manifest: target.Manifest, ManifestDigest: target.ManifestDigest, CurrentVersion: "v1.0.0",
		ReleaseChain: chain, ExpiresAt: now.Add(time.Hour),
	}
	legacy.Plans[dropHash] = legacy.Plans[planHash]
	recovery := BackupMetadata{
		SchemaVersion:       BackupMetadataSchemaVersion,
		JobID:               "job_legacy",
		Version:             "v1.0.0",
		ImageDigest:         "sha256:" + strings.Repeat("1", 64),
		ImageRef:            AllowedImageRepository + ":v1.0.0@sha256:" + strings.Repeat("1", 64),
		DatabaseSchema:      1,
		SchemaCompatVersion: 1,
		CreatedAt:           now,
		Artifacts: []BackupArtifact{
			{Name: "message-config.tar", Size: 1, SHA256: strings.Repeat("a", 64)},
			{Name: "message-data.tar", Size: 1, SHA256: strings.Repeat("b", 64)},
			{Name: "p2p.tar", Size: 1, SHA256: strings.Repeat("c", 64)},
			{Name: "postgres.dump", Size: 1, SHA256: strings.Repeat("d", 64)},
		},
	}
	legacy.Jobs["job_legacy"] = Job{
		ID:                "job_legacy",
		Status:            JobRollingBack,
		PlanTokenHash:     planHash,
		ManifestDigest:    target.ManifestDigest,
		BearerTokenHashes: []string{tokenHash("legacy-job-token")},
		IdempotencyKey:    "legacy-job-request",
		CurrentVersion:    "v1.0.0",
		TargetVersion:     target.Manifest.Version,
		CurrentStep:       JobStepRestoreBackup,
		TotalSteps:        executionTotalSteps,
		TotalHops:         1,
		ServiceAvailable:  false,
		LastSafeVersion:   "v1.0.0",
		RecoveryPoint:     &recovery,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	legacy.Idempotency["legacy-job-request"] = "job_legacy"
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	store := NewStateStore(filepath.Join(t.TempDir(), "runtime.json"))
	if err := os.WriteFile(store.Path(), data, 0o600); err != nil {
		t.Fatal(err)
	}

	migrated, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("migrate schema five state: %v", err)
	}
	job := migrated.Jobs["job_legacy"]
	if migrated.SchemaVersion != RuntimeStateSchemaVersion || migrated.Discovery.Status != DiscoveryUnknown || migrated.Discovery.Manifest != nil || len(migrated.Plans) != 1 {
		t.Fatalf("migration retained executable discovery data: %#v", migrated)
	}
	if _, exists := migrated.Plans[dropHash]; exists {
		t.Fatalf("migration retained an unreferenced plan: %#v", migrated.Plans)
	}
	if _, exists := migrated.Plans[planHash]; !exists || job.BearerTokenHashes[0] != tokenHash("legacy-job-token") || !reflect.DeepEqual(job.RecoveryPoint, &recovery) {
		t.Fatalf("migration did not preserve the active legacy job boundary: %#v", job)
	}

	runtime := &fakeUpgradeRuntime{}
	if err := NewJobEngine(store, runtime).RunActive(context.Background()); err != nil {
		t.Fatalf("resume migrated legacy recovery: %v", err)
	}
	if want := []string{"restore_backup", "check_restored"}; !reflect.DeepEqual(runtime.calls, want) {
		t.Fatalf("legacy recovery calls = %#v, want %#v", runtime.calls, want)
	}
}
