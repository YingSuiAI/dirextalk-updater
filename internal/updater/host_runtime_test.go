package updater

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestComposeRuntimePreparesAndCommitsCompleteRecoveryPoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "job_backup", CurrentVersion: "v1.0.0"}
	plan := testBackupPlan(manifest, job.CurrentVersion, "sha256:"+strings.Repeat("1", 64))

	metadata, err := runtime.PrepareBackup(context.Background(), job, plan, ignoreProgress)
	if err != nil {
		t.Fatalf("prepare backup: %v", err)
	}
	if metadata.Version != "v1.0.0" || metadata.ImageDigest != "sha256:"+strings.Repeat("1", 64) {
		t.Fatalf("unexpected recovery metadata: %#v", metadata)
	}
	if _, err := runtime.backups.Current(context.Background()); err != nil {
		t.Fatalf("committed backup is not readable: %v", err)
	}
	assertCallSequence(t, runner.calls, []string{
		" up -d --no-deps message-server",
		"{{.State.Status}}",
		" stop message-server",
		" pg_dump ",
		" pg_restore --list",
		"/etc/dirextalk-message-server -cf",
		"/var/dirextalk-message-server --exclude=./p2p -cf",
		" up -d --no-deps message-server",
		"{{.State.Status}}",
	})
}

func TestComposeRuntimeRejectsUntrustedSourceDigestBeforeStoppingOrRotatingBackup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	old := stageCompleteBackup(t, runtime.backups, "job_old", "v0.9.0")
	if err := runtime.backups.Commit(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	plan := testBackupPlan(manifest, "v1.0.0", "sha256:"+strings.Repeat("f", 64))
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_untrusted", CurrentVersion: "v1.0.0"}, plan, ignoreProgress); err == nil {
		t.Fatal("expected source digest mismatch")
	}
	for _, call := range runner.calls {
		if strings.Contains(call, " stop message-server") || strings.Contains(call, " pg_dump ") {
			t.Fatalf("untrusted source reached disruptive backup commands: %#v", runner.calls)
		}
	}
	current, err := runtime.backups.Current(context.Background())
	if err != nil || current.Version != "v0.9.0" {
		t.Fatalf("untrusted source replaced prior recovery point: current=%#v err=%v", current, err)
	}
}

func TestComposeRuntimeAcceptsMinimalHealthOnlyForTrustedLegacyBootstrapPlan(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	digest := "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
	if err := os.WriteFile(paths.envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE="+pinnedImageRef(legacyInitialVersion, digest)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &fakeHostCommandRunner{
		imageRef:   pinnedImageRef(legacyInitialVersion, digest),
		healthJSON: `{"status":"ok"}`,
	}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(manifestJSONFor(firstFormalVersion, strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0")))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_legacy", CurrentVersion: legacyInitialVersion}, testBackupPlan(manifest, legacyInitialVersion, digest), ignoreProgress)
	if err != nil {
		t.Fatalf("trusted legacy minimal health was rejected: %v", err)
	}
	if metadata.Version != legacyInitialVersion || metadata.ImageDigest != digest || metadata.DatabaseSchema != 1 || metadata.SchemaCompatVersion != 1 {
		t.Fatalf("legacy bootstrap assumption was not recorded explicitly: %#v", metadata)
	}
	if healthVersionMatches("v1.0.0", "1.0.0") {
		t.Fatal("bare version normalization escaped the one legacy version")
	}
}

func TestComposeRuntimeRejectsMinimalHealthOutsideTrustedLegacyBootstrap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		version    string
		healthJSON string
	}{
		{name: "formal source", version: "v1.0.0", healthJSON: `{"status":"ok"}`},
		{name: "legacy non ok", version: legacyInitialVersion, healthJSON: `{"status":"down"}`},
		{name: "legacy partial", version: legacyInitialVersion, healthJSON: `{"status":"ok","version":"0.15.2"}`},
		{name: "legacy full bare", version: legacyInitialVersion, healthJSON: `{"status":"ok","version":"0.15.2","schema_version":1,"schema_compat_version":1}`},
		{name: "legacy unknown field", version: legacyInitialVersion, healthJSON: `{"status":"ok","extra":true}`},
		{name: "legacy malformed", version: legacyInitialVersion, healthJSON: `{"status":"ok"} trailing`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			paths := testHostPaths(t, root)
			digest := "sha256:" + strings.Repeat("1", 64)
			if test.version == legacyInitialVersion {
				digest = "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
			}
			if err := os.WriteFile(paths.envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE="+pinnedImageRef(test.version, digest)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runner := &fakeHostCommandRunner{imageRef: pinnedImageRef(test.version, digest), healthJSON: test.healthJSON}
			runtime := newTestComposeRuntime(paths, runner)
			if err := runtime.ObserveWatchdog(context.Background()); err == nil {
				t.Fatal("minimal or malformed health escaped strict watchdog validation")
			}
		})
	}
}

func TestComposeRuntimeAllowsMinimalHealthOnlyForExplicitLegacyRestore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	digest := "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
	if err := os.WriteFile(paths.envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE="+pinnedImageRef(legacyInitialVersion, digest)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &fakeHostCommandRunner{imageRef: pinnedImageRef(legacyInitialVersion, digest), healthJSON: `{"status":"ok"}`}
	runtime := newTestComposeRuntime(paths, runner)
	recovery := BackupMetadata{Version: legacyInitialVersion, ImageDigest: digest, DatabaseSchema: 1, SchemaCompatVersion: 1, LegacyBootstrapAssumption: true}
	if err := runtime.CheckRestored(context.Background(), recovery); err != nil {
		t.Fatalf("explicit legacy restore rejected minimal health: %v", err)
	}

	formalDigest := "sha256:" + strings.Repeat("1", 64)
	if err := os.WriteFile(paths.envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE="+pinnedImageRef("v1.0.0", formalDigest)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner.imageRef = pinnedImageRef("v1.0.0", formalDigest)
	formal := BackupMetadata{Version: "v1.0.0", ImageDigest: formalDigest, DatabaseSchema: 1, SchemaCompatVersion: 1}
	if err := runtime.CheckRestored(context.Background(), formal); err == nil {
		t.Fatal("formal restore accepted minimal health")
	}
}

func TestComposeRuntimeTargetHealthNeverUsesLegacyMinimalException(t *testing.T) {
	t.Parallel()
	paths := testHostPaths(t, t.TempDir())
	runner := &fakeHostCommandRunner{
		imageRef:   "dirextalk/message-server:v1.1.0@sha256:" + strings.Repeat("a", 64),
		healthJSON: `{"status":"ok"}`,
	}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.CheckTarget(context.Background(), manifest); err == nil {
		t.Fatal("formal target accepted legacy minimal health exception")
	}
}

func TestComposeRuntimeLegacyBootstrapRequiresExactPersistedSourceDigestBeforeStart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	observed := "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
	if err := os.WriteFile(paths.envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE="+pinnedImageRef(legacyInitialVersion, observed)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &fakeHostCommandRunner{imageRef: pinnedImageRef(legacyInitialVersion, observed), healthJSON: `{"status":"ok"}`}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(manifestJSONFor(firstFormalVersion, strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0")))
	if err != nil {
		t.Fatal(err)
	}
	plan := testBackupPlan(manifest, legacyInitialVersion, "sha256:"+strings.Repeat("f", 64))
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_legacy_mismatch", CurrentVersion: legacyInitialVersion}, plan, ignoreProgress); err == nil {
		t.Fatal("legacy source digest mismatch was accepted")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("legacy digest mismatch started Compose before rejection: %#v", runner.calls)
	}
}

func TestComposeRuntimeActivatesOnlyManifestDigestAndAtomicallyUpdatesEnv(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.ActivateTarget(context.Background(), manifest, ignoreProgress); err != nil {
		t.Fatalf("activate target: %v", err)
	}
	data, err := os.ReadFile(paths.envFile)
	if err != nil {
		t.Fatal(err)
	}
	targetRef := manifest.Image + "@" + manifest.ImageDigest
	if !strings.Contains(string(data), "MESSAGE_SERVER_IMAGE="+targetRef+"\n") || strings.Contains(string(data), ":latest") {
		t.Fatalf("environment was not pinned to target digest: %q", data)
	}
	want := []string{
		"docker pull " + targetRef,
		"docker image inspect --format {{join .RepoDigests \"\\n\"}} " + targetRef,
		"docker compose --project-name dirextalk-p2p --file " + paths.composeFile + " up -d --no-deps --force-recreate message-server",
	}
	if !reflect.DeepEqual(runner.calls, want) {
		t.Fatalf("commands = %#v, want %#v", runner.calls, want)
	}
}

func TestWritePinnedImagePreservesOldEnvironmentOnRenameFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	envPath := filepath.Join(root, ".env")
	original := []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE=dirextalk/message-server:v1.0.0@sha256:" + strings.Repeat("1", 64) + "\n")
	if err := os.WriteFile(envPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	replace := func(_, _ string) error { return fmt.Errorf("simulated rename failure") }
	err := writePinnedImage(envPath, "dirextalk/message-server:v1.1.0@sha256:"+strings.Repeat("2", 64), replace, syncDirectory)
	if err == nil {
		t.Fatal("expected atomic replace failure")
	}
	got, readErr := os.ReadFile(envPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("failed replace changed environment: %q", got)
	}
}

func TestComposeRuntimeRequiresConsecutiveInternalDatabaseAndCaddyHealth(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	paths.healthAttempts = 3
	paths.healthConsecutive = 2
	paths.healthInterval = time.Millisecond
	paths.sleep = func(context.Context, time.Duration) error { return nil }
	runner := &fakeHostCommandRunner{
		imageRef:   "dirextalk/message-server:v1.1.0@sha256:" + strings.Repeat("a", 64),
		healthJSON: `{"status":"ok","version":"v1.1.0","schema_version":2,"schema_compat_version":1}`,
	}
	runtime := newTestComposeRuntime(paths, runner)
	runtime.publicHealth = func(context.Context, string) (runtimeHealth, error) {
		return runtimeHealth{Status: "ok", Version: "v1.1.0", SchemaVersion: 2, SchemaCompatVersion: 1}, nil
	}
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.CheckTarget(context.Background(), manifest); err != nil {
		t.Fatalf("check target confirmation window: %v", err)
	}
	inspectCount := 0
	for _, call := range runner.calls {
		if strings.Contains(call, "{{.Config.Image}}") {
			inspectCount++
		}
	}
	if inspectCount != 2 {
		t.Fatalf("expected two consecutive health samples, inspect count=%d", inspectCount)
	}
	if !strings.Contains(strings.Join(runner.calls, "\n"), "CREATE TEMP TABLE dirextalk_updater_probe") {
		t.Fatal("health confirmation omitted the PostgreSQL read/write probe")
	}
}

func TestComposeRuntimeRestoresOnlyTheCommittedRecoveryPoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "job_restore", CurrentVersion: "v1.0.0"}
	recovery, err := runtime.PrepareBackup(context.Background(), job, testBackupPlan(manifest, job.CurrentVersion, "sha256:"+strings.Repeat("1", 64)), ignoreProgress)
	if err != nil {
		t.Fatal(err)
	}
	targetRef := manifest.Image + "@" + manifest.ImageDigest
	if err := writePinnedImage(paths.envFile, targetRef, os.Rename, syncDirectory); err != nil {
		t.Fatal(err)
	}
	runner.calls = nil
	if err := runtime.RestoreBackup(context.Background(), recovery); err != nil {
		t.Fatalf("restore committed recovery point: %v", err)
	}
	environment, err := os.ReadFile(paths.envFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(environment), "MESSAGE_SERVER_IMAGE="+recovery.ImageRef+"\n") {
		t.Fatalf("restore did not repin source digest: %q", environment)
	}
	joined := strings.Join(runner.calls, "\n")
	for _, required := range []string{"docker image inspect", " stop message-server", " pg_restore ", "CHECKPOINT;", "sync", " up -d --no-deps --force-recreate message-server"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("restore command %q missing from:\n%s", required, joined)
		}
	}
	if strings.Contains(joined, "docker pull "+recovery.ImageRef) {
		t.Fatal("rollback fetched the recovery image even though the exact local digest was available")
	}

	tampered := recovery
	tampered.ImageDigest = "sha256:" + strings.Repeat("f", 64)
	tampered.ImageRef = pinnedImageRef(tampered.Version, tampered.ImageDigest)
	runner.calls = nil
	if err := runtime.RestoreBackup(context.Background(), tampered); err == nil {
		t.Fatal("expected non-committed recovery metadata to be rejected")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("tampered recovery reached Docker: %#v", runner.calls)
	}
}

func TestComposeRuntimeBackupFailureRestartsSourceAndPreservesCommittedBackup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{failContains: "--exclude=./p2p"}
	runtime := newTestComposeRuntime(paths, runner)
	old := stageCompleteBackup(t, runtime.backups, "job_old", "v0.9.0")
	if err := runtime.backups.Commit(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.PrepareBackup(context.Background(), Job{ID: "job_new", CurrentVersion: "v1.0.0"}, testBackupPlan(manifest, "v1.0.0", "sha256:"+strings.Repeat("1", 64)), ignoreProgress)
	if err == nil {
		t.Fatal("expected staged archive failure")
	}
	restarts := 0
	for _, call := range runner.calls {
		if strings.Contains(call, " up -d --no-deps message-server") {
			restarts++
		}
	}
	if restarts < 2 {
		t.Fatalf("source service was not restarted and health-checked after backup failure: %#v", runner.calls)
	}
	current, err := runtime.backups.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if current.JobID != "job_old" {
		t.Fatalf("failed staging replaced committed backup: %#v", current)
	}
}

func TestComposeRuntimeResumesBackupByRecoveringAStoppedSourceFirst(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runner := &fakeHostCommandRunner{serviceStopped: true}
	runtime := newTestComposeRuntime(paths, runner)
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_resume", CurrentVersion: "v1.0.0"}, testBackupPlan(manifest, "v1.0.0", "sha256:"+strings.Repeat("1", 64)), ignoreProgress); err != nil {
		t.Fatalf("resume backup from stopped source: %v", err)
	}
	if len(runner.calls) == 0 || !strings.Contains(runner.calls[0], " up -d --no-deps message-server") {
		t.Fatalf("backup resume checked health before recovering source: %#v", runner.calls)
	}
}

func TestComposeRuntimeDoesNotFsyncDiscardedStagingAfterCommit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := testHostPaths(t, root)
	runtime := newTestComposeRuntime(paths, &fakeHostCommandRunner{})
	syncCalls := 0
	runtime.backups.syncDirectory = func(string) error {
		syncCalls++
		if syncCalls >= 4 {
			return fmt.Errorf("unexpected post-commit staging fsync")
		}
		return nil
	}
	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_no_defer", CurrentVersion: "v1.0.0"}, testBackupPlan(manifest, "v1.0.0", "sha256:"+strings.Repeat("1", 64)), ignoreProgress); err != nil {
		t.Fatalf("committed backup was turned into an ambiguous error: %v", err)
	}
	if syncCalls != 3 {
		t.Fatalf("unexpected backup root sync count: %d", syncCalls)
	}
}

func TestComposeRuntimeWatchdogRepairUsesPinnedLocalImageAndFixedOrder(t *testing.T) {
	t.Parallel()
	paths := testHostPaths(t, t.TempDir())
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)

	if err := runtime.RepairWatchdog(context.Background()); err != nil {
		t.Fatalf("repair watchdog: %v", err)
	}
	assertCallSequence(t, runner.calls, []string{
		"systemctl start docker.service",
		"docker image inspect",
		" up -d --no-deps --pull never postgres",
		" pg_isready ",
		" up -d --no-deps --pull never message-server",
		" up -d --no-deps --pull never caddy",
		"{{.State.Status}}",
		"CREATE TEMP TABLE dirextalk_updater_probe",
	})
	joined := strings.Join(runner.calls, "\n")
	for _, forbidden := range []string{"docker pull", ":latest", "pg_dump", "pg_restore", "backup", "migration", "release"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("watchdog repair used forbidden operation %q: %s", forbidden, joined)
		}
	}
}

func TestComposeRuntimeWatchdogFailsClosedWhenPinnedImageIsNotLocal(t *testing.T) {
	t.Parallel()
	paths := testHostPaths(t, t.TempDir())
	runner := &fakeHostCommandRunner{failContains: "image inspect"}
	runtime := newTestComposeRuntime(paths, runner)

	if err := runtime.RepairWatchdog(context.Background()); err == nil {
		t.Fatal("watchdog pulled or started services without the pinned local image")
	}
	joined := strings.Join(runner.calls, "\n")
	if strings.Contains(joined, "docker pull") || strings.Contains(joined, " up -d --no-deps --pull never postgres") {
		t.Fatalf("watchdog mutated Compose after local image failure: %s", joined)
	}
}

func TestComposeRuntimeWatchdogObservesDockerAndCurrentPinnedHealth(t *testing.T) {
	t.Parallel()
	paths := testHostPaths(t, t.TempDir())
	runner := &fakeHostCommandRunner{}
	runtime := newTestComposeRuntime(paths, runner)

	if err := runtime.ObserveWatchdog(context.Background()); err != nil {
		t.Fatalf("observe watchdog: %v", err)
	}
	assertCallSequence(t, runner.calls, []string{
		"systemctl is-active --quiet docker.service",
		"{{.State.Status}}",
		"CREATE TEMP TABLE dirextalk_updater_probe",
	})
}

func TestComposeRuntimeStreamsOnlyFixedProjectFailureEvents(t *testing.T) {
	t.Parallel()
	paths := testHostPaths(t, t.TempDir())
	runner := &fakeHostCommandRunner{eventOutput: "container-one\ncontainer-two\n"}
	runtime := newTestComposeRuntime(paths, runner)
	events := 0

	if err := runtime.StreamWatchdogEvents(context.Background(), func() { events++ }); err != nil {
		t.Fatalf("stream watchdog events: %v", err)
	}
	if events != 2 {
		t.Fatalf("event notifications = %d, want 2", events)
	}
	joined := strings.Join(runner.calls, "\n")
	for _, required := range []string{"docker events", "label=com.docker.compose.project=dirextalk-p2p", "event=die", "event=stop", "event=kill"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("event stream omitted %q: %s", required, joined)
		}
	}
}

type fakeHostCommandRunner struct {
	calls          []string
	imageRef       string
	healthJSON     string
	failContains   string
	serviceStopped bool
	eventOutput    string
}

func (runner *fakeHostCommandRunner) Run(_ context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
	runner.calls = append(runner.calls, strings.Join(append([]string{name}, args...), " "))
	joined := strings.Join(args, " ")
	if runner.failContains != "" && strings.Contains(joined, runner.failContains) {
		return fmt.Errorf("simulated command failure")
	}
	if strings.Contains(joined, " stop message-server") {
		runner.serviceStopped = true
	}
	if strings.Contains(joined, " up -d --no-deps") && strings.HasSuffix(joined, " message-server") {
		runner.serviceStopped = false
	}
	if runner.serviceStopped && (strings.Contains(joined, "{{.State.Status}}") || strings.Contains(joined, "wget -qO-")) {
		return fmt.Errorf("message-server is stopped")
	}
	switch {
	case name == "docker" && len(args) > 0 && args[0] == "events":
		_, _ = io.WriteString(stdout, runner.eventOutput)
	case strings.Contains(joined, "{{.State.Status}}"):
		_, _ = io.WriteString(stdout, "running healthy\n")
	case strings.Contains(joined, "{{.Config.Image}}"):
		imageRef := runner.imageRef
		if imageRef == "" {
			imageRef = "dirextalk/message-server:v1.0.0@sha256:" + strings.Repeat("1", 64)
		}
		_, _ = io.WriteString(stdout, imageRef+"\n")
	case strings.Contains(joined, "wget -qO-"):
		health := runner.healthJSON
		if health == "" {
			health = `{"status":"ok","version":"v1.0.0","schema_version":1,"schema_compat_version":1}`
		}
		_, _ = io.WriteString(stdout, health)
	case strings.Contains(joined, "pg_dump"):
		_, _ = io.WriteString(stdout, "postgres-custom-dump")
	case strings.Contains(joined, "pg_restore --list"):
		if stdin == nil {
			return fmt.Errorf("pg_restore validation requires dump input")
		}
		_, _ = io.Copy(io.Discard, stdin)
	case strings.Contains(joined, "-C /etc/dirextalk-message-server -cf"):
		return writeFakeTar(stdout, "config")
	case strings.Contains(joined, "-C /var/dirextalk-message-server --exclude=./p2p -cf"):
		return writeFakeTar(stdout, "data")
	case name == "docker" && len(args) > 2 && args[0] == "image" && args[1] == "inspect":
		digest := "sha256:" + strings.Repeat("a", 64)
		if at := strings.LastIndex(args[len(args)-1], "@sha256:"); at >= 0 {
			digest = strings.TrimPrefix(args[len(args)-1][at:], "@")
		}
		_, _ = io.WriteString(stdout, AllowedImageRepository+"@"+digest+"\n")
	}
	return nil
}

func testHostPaths(t *testing.T, root string) composeRuntimePaths {
	t.Helper()
	composeDir := filepath.Join(root, "compose")
	p2pDir := filepath.Join(composeDir, "p2p")
	if err := os.MkdirAll(p2pDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p2pDir, "bootstrap.json"), []byte(`{"test":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	envFile := filepath.Join(composeDir, ".env")
	if err := os.WriteFile(envFile, []byte("DOMAIN=d1.example.test\nMESSAGE_SERVER_IMAGE=dirextalk/message-server:v1.0.0@sha256:"+strings.Repeat("1", 64)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return composeRuntimePaths{
		composeFile:       filepath.Join(composeDir, "docker-compose.yml"),
		envFile:           envFile,
		p2pDir:            p2pDir,
		backupRoot:        filepath.Join(root, "backups"),
		now:               func() time.Time { return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC) },
		healthAttempts:    1,
		healthConsecutive: 1,
		healthInterval:    time.Millisecond,
		sleep:             func(context.Context, time.Duration) error { return nil },
	}
}

func ignoreProgress(JobStatus) error { return nil }

func testBackupPlan(manifest Manifest, currentVersion, sourceDigest string) Plan {
	step := ReleaseStep{Manifest: manifest, ManifestDigest: canonicalManifestDigest(manifest), SourceImageDigests: []string{sourceDigest}}
	return Plan{Manifest: manifest, ManifestDigest: step.ManifestDigest, CurrentVersion: currentVersion, ReleaseChain: []ReleaseStep{step}}
}

func newTestComposeRuntime(paths composeRuntimePaths, runner *fakeHostCommandRunner) *ComposeRuntime {
	runtime := newComposeRuntime(paths, runner, nil)
	runtime.publicHealth = func(context.Context, string) (runtimeHealth, error) {
		healthJSON := runner.healthJSON
		if healthJSON == "" {
			healthJSON = `{"status":"ok","version":"v1.0.0","schema_version":1,"schema_compat_version":1}`
		}
		var health runtimeHealth
		if err := json.Unmarshal([]byte(healthJSON), &health); err != nil {
			return runtimeHealth{}, err
		}
		return health, nil
	}
	return runtime
}

func writeFakeTar(output io.Writer, value string) error {
	writer := tar.NewWriter(output)
	if err := writer.WriteHeader(&tar.Header{Name: "fixture", Mode: 0o600, Size: int64(len(value)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, value); err != nil {
		return err
	}
	return writer.Close()
}

func assertCallSequence(t *testing.T, calls, fragments []string) {
	t.Helper()
	cursor := 0
	for _, fragment := range fragments {
		found := false
		for cursor < len(calls) {
			if strings.Contains(calls[cursor], fragment) {
				found = true
				cursor++
				break
			}
			cursor++
		}
		if !found {
			t.Fatalf("command fragment %q missing in order from %#v", fragment, calls)
		}
	}
}
