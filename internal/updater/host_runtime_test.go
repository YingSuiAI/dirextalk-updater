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
	plan := Plan{Manifest: manifest, CurrentVersion: job.CurrentVersion}

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
	recovery, err := runtime.PrepareBackup(context.Background(), job, Plan{Manifest: manifest, CurrentVersion: job.CurrentVersion}, ignoreProgress)
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
	_, err = runtime.PrepareBackup(context.Background(), Job{ID: "job_new", CurrentVersion: "v1.0.0"}, Plan{Manifest: manifest, CurrentVersion: "v1.0.0"}, ignoreProgress)
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
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_resume", CurrentVersion: "v1.0.0"}, Plan{Manifest: manifest, CurrentVersion: "v1.0.0"}, ignoreProgress); err != nil {
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
	if _, err := runtime.PrepareBackup(context.Background(), Job{ID: "job_no_defer", CurrentVersion: "v1.0.0"}, Plan{Manifest: manifest, CurrentVersion: "v1.0.0"}, ignoreProgress); err != nil {
		t.Fatalf("committed backup was turned into an ambiguous error: %v", err)
	}
	if syncCalls != 3 {
		t.Fatalf("unexpected backup root sync count: %d", syncCalls)
	}
}

type fakeHostCommandRunner struct {
	calls          []string
	imageRef       string
	healthJSON     string
	failContains   string
	serviceStopped bool
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
