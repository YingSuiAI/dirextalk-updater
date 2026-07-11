//go:build integration

package updater

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUbuntuComposeBackupUpgradeAndRollback(t *testing.T) {
	if os.Getenv("DIREXTALK_UPDATER_COMPOSE_INTEGRATION") != "1" {
		t.Skip("set DIREXTALK_UPDATER_COMPOSE_INTEGRATION=1 on Ubuntu 22.04 or 24.04 with Docker Compose")
	}
	if err := CheckSupportedHost(); err != nil {
		t.Fatalf("integration host: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root := t.TempDir()
	fixtureImage := "dirextalk-updater-fixture:" + strings.ToLower(randomFixtureID(t))
	writeComposeFixture(t, root, fixtureImage)
	realRunner := execHostCommandRunner{}
	projectLabel := "label=com.docker.compose.project=" + AllowedComposeProject
	for _, resource := range []struct {
		name string
		args []string
	}{
		{name: "container", args: []string{"ps", "-a", "--filter", projectLabel, "-q"}},
		{name: "volume", args: []string{"volume", "ls", "--filter", projectLabel, "-q"}},
		{name: "network", args: []string{"network", "ls", "--filter", projectLabel, "-q"}},
	} {
		var existing bytes.Buffer
		if err := realRunner.Run(ctx, nil, &existing, "docker", resource.args...); err != nil {
			t.Fatalf("inspect existing Compose %s resources: %v", resource.name, err)
		}
		if strings.TrimSpace(existing.String()) != "" {
			t.Skipf("refusing to reuse existing %s %s resources", AllowedComposeProject, resource.name)
		}
	}
	if err := realRunner.Run(ctx, nil, io.Discard, "docker", "build", "-t", fixtureImage, filepath.Join(root, "image")); err != nil {
		t.Fatalf("build fixture image: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if err := realRunner.Run(cleanupCtx, nil, io.Discard, "docker", "compose", "--project-name", AllowedComposeProject, "--file", filepath.Join(root, "docker-compose.yml"), "down", "--volumes", "--remove-orphans"); err != nil {
			t.Errorf("clean up fixture Compose project: %v", err)
		}
		if err := realRunner.Run(cleanupCtx, nil, io.Discard, "docker", "image", "rm", "-f", fixtureImage); err != nil {
			t.Errorf("clean up fixture image: %v", err)
		}
	})
	runner := &integrationCommandRunner{delegate: realRunner, envFile: filepath.Join(root, ".env"), healthFile: filepath.Join(root, "health.json")}
	paths := composeRuntimePaths{
		composeFile:       filepath.Join(root, "docker-compose.yml"),
		envFile:           filepath.Join(root, ".env"),
		p2pDir:            filepath.Join(root, "p2p"),
		backupRoot:        filepath.Join(root, "backup"),
		now:               time.Now,
		healthAttempts:    12,
		healthConsecutive: 2,
		healthInterval:    time.Second,
		sleep:             sleepContext,
	}
	runtime := newComposeRuntime(paths, runner, nil, CaddyModeCompose)
	runtime.publicHealth = func(ctx context.Context, _ string) (runtimeHealth, error) {
		return runtime.inspectHealth(ctx)
	}
	if err := realRunner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "postgres", "message-server")...); err != nil {
		t.Fatalf("start fixture compose: %v", err)
	}
	waitForFixtureHealth(t, ctx, runtime, "v1.0.0")
	seedFixtureState(t, ctx, runtime)

	manifest, err := ValidateManifest([]byte(validManifestJSON()))
	if err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "job_integration", CurrentVersion: "v1.0.0"}
	recovery, err := runtime.PrepareBackup(ctx, job, testBackupPlan(manifest, job.CurrentVersion, "sha256:"+strings.Repeat("1", 64)), ignoreProgress)
	if err != nil {
		t.Fatalf("real Compose backup: %v", err)
	}
	mutateFixtureState(t, ctx, runtime)
	if err := runtime.ActivateTarget(ctx, manifest, ignoreProgress); err != nil {
		t.Fatalf("real Compose activation: %v", err)
	}
	if err := runtime.CheckTarget(ctx, manifest); err != nil {
		t.Fatalf("real Compose target health: %v", err)
	}
	if err := runtime.RestoreBackup(ctx, recovery); err != nil {
		t.Fatalf("real Compose rollback: %v", err)
	}
	if err := runtime.CheckRestored(ctx, recovery); err != nil {
		t.Fatalf("real Compose restored health: %v", err)
	}
	verifyFixtureRestored(t, ctx, runtime)

	store, jobID := seedQueuedExecutionJob(t)
	failTargetHealth := true
	runtime.publicHealth = func(ctx context.Context, _ string) (runtimeHealth, error) {
		health, healthErr := runtime.inspectHealth(ctx)
		if healthErr == nil && failTargetHealth && health.Version == "v1.1.0" {
			return runtimeHealth{}, errors.New("integration target health failure")
		}
		return health, healthErr
	}
	runner.afterTargetActivation = func() error {
		mutateFixtureState(t, ctx, runtime)
		return nil
	}
	if err := NewJobEngine(store, runtime).RunActive(ctx); err == nil {
		t.Fatal("real Compose target failure did not propagate after automatic rollback")
	}
	failTargetHealth = false
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job = state.Jobs[jobID]
	if job.Status != JobRolledBack || !job.ServiceAvailable || state.DesiredState != DesiredRunning || job.RecoveryPoint == nil {
		t.Fatalf("real Compose automatic rollback state is unsafe: job=%#v desired=%q", job, state.DesiredState)
	}
	currentRecovery, err := runtime.backups.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !sameRecoveryPoint(currentRecovery, *job.RecoveryPoint) {
		t.Fatal("automatic rollback job does not own the committed recovery point")
	}
	verifyFixtureRestored(t, ctx, runtime)

	if err := realRunner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("stop", "message-server")...); err != nil {
		t.Fatalf("stop fixture before restart recovery: %v", err)
	}
	if err := store.Update(ctx, func(state *RuntimeState) error {
		job := state.Jobs[jobID]
		job.Status = JobRestarting
		job.CurrentStep = JobStepRestart
		job.ServiceAvailable = false
		state.Jobs[jobID] = job
		state.DesiredState = DesiredUpgrading
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := NewJobEngine(store, runtime).RunActive(ctx); err != nil {
		t.Fatalf("real Compose restart recovery: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job = state.Jobs[jobID]
	if job.Status != JobFailed || !job.ServiceAvailable || state.DesiredState != DesiredRunning {
		t.Fatalf("real Compose restart recovery state is unsafe: job=%#v desired=%q", job, state.DesiredState)
	}
	if err := runtime.CheckRestored(ctx, *job.RecoveryPoint); err != nil {
		t.Fatalf("restarted source release did not pass health confirmation: %v", err)
	}

	if err := realRunner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("stop", "message-server")...); err != nil {
		t.Fatalf("stop fixture before watchdog suppression checks: %v", err)
	}
	for _, desired := range []DesiredState{DesiredMaintenance, DesiredDeprovisioned, DesiredUpgrading} {
		var watchdogStore *StateStore
		if desired == DesiredUpgrading {
			watchdogStore, _ = seedQueuedExecutionJob(t)
		} else {
			watchdogStore = NewStateStore(filepath.Join(t.TempDir(), "runtime.json"))
			watchdogState := NewRuntimeState()
			watchdogState.DesiredState = desired
			if err := watchdogStore.Save(ctx, watchdogState); err != nil {
				t.Fatal(err)
			}
		}
		if err := NewWatchdog(watchdogStore, runtime).Reconcile(ctx); err != nil {
			t.Fatalf("watchdog suppression for %s: %v", desired, err)
		}
		assertFixtureMessageServerStopped(t, ctx, realRunner)
	}
	watchdogStore := NewStateStore(filepath.Join(t.TempDir(), "runtime.json"))
	watchdog := NewWatchdog(watchdogStore, runtime)
	for observation := 0; observation < watchdogObservationThreshold; observation++ {
		_ = watchdog.Reconcile(ctx)
	}
	watchdogState, err := watchdogStore.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if watchdogState.Watchdog.Status != WatchdogHealthy {
		t.Fatalf("real Compose watchdog did not repair the stopped service: %#v", watchdogState.Watchdog)
	}
	if err := runtime.ObserveWatchdog(ctx); err != nil {
		t.Fatalf("watchdog-repaired fixture is unhealthy: %v", err)
	}
}

type integrationCommandRunner struct {
	delegate              hostCommandRunner
	envFile               string
	healthFile            string
	afterTargetActivation func() error
}

func (runner *integrationCommandRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
	joined := strings.Join(args, " ")
	targetActivation := false
	if name == "systemctl" {
		return nil
	}
	if name == "docker" && strings.Contains(joined, "inspect --format {{.Config.Image}} ") {
		ref, err := readEnvironmentValue(runner.envFile, "MESSAGE_SERVER_IMAGE")
		if err != nil {
			return err
		}
		_, err = io.WriteString(stdout, ref+"\n")
		return err
	}
	if name == "docker" && len(args) >= 2 && args[0] == "pull" && strings.HasPrefix(args[1], AllowedImageRepository+":") {
		return nil
	}
	if name == "docker" && len(args) >= 2 && args[0] == "image" && args[1] == "inspect" && strings.Contains(joined, ".RepoDigests") {
		ref := args[len(args)-1]
		_, digest, err := parsePinnedImageRef(ref)
		if err != nil {
			return err
		}
		_, err = io.WriteString(stdout, AllowedImageRepository+"@"+digest+"\n")
		return err
	}
	if name == "docker" && strings.Contains(joined, " up -d --no-deps --force-recreate message-server") {
		ref, err := readEnvironmentValue(runner.envFile, "MESSAGE_SERVER_IMAGE")
		if err != nil {
			return err
		}
		version, _, err := parsePinnedImageRef(ref)
		if err != nil {
			return err
		}
		schema := 1
		if version == "v1.1.0" {
			schema = 2
		}
		health := fmt.Sprintf(`{"status":"ok","version":%q,"schema_version":%d,"schema_compat_version":1}`+"\n", version, schema)
		if err := os.WriteFile(runner.healthFile, []byte(health), 0o600); err != nil {
			return err
		}
		targetActivation = version == "v1.1.0"
	}
	if err := runner.delegate.Run(ctx, stdin, stdout, name, args...); err != nil {
		return err
	}
	if targetActivation && runner.afterTargetActivation != nil {
		afterTargetActivation := runner.afterTargetActivation
		runner.afterTargetActivation = nil
		return afterTargetActivation()
	}
	return nil
}

func writeComposeFixture(t *testing.T, root, image string) {
	t.Helper()
	for _, directory := range []string{"image", "p2p"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(relative, content string, mode os.FileMode) {
		if err := os.WriteFile(filepath.Join(root, relative), []byte(content), mode); err != nil {
			t.Fatal(err)
		}
	}
	write("image/Dockerfile", "FROM alpine:3.20\nRUN apk add --no-cache busybox-extras wget && mkdir -p /www/_p2p\nCMD [\"httpd\",\"-f\",\"-p\",\"8008\",\"-h\",\"/www\"]\n", 0o600)
	write("health.json", `{"status":"ok","version":"v1.0.0","schema_version":1,"schema_compat_version":1}`+"\n", 0o600)
	write(".env", "DOMAIN=integration.example.test\nFIXTURE_IMAGE="+image+"\nMESSAGE_SERVER_IMAGE="+pinnedImageRef("v1.0.0", "sha256:"+strings.Repeat("1", 64))+"\n", 0o600)
	compose := `services:
  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_USER: dirextalk_message_server
      POSTGRES_PASSWORD: dirextalk_message_server
      POSTGRES_DB: dirextalk_message_server
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dirextalk_message_server -d dirextalk_message_server"]
      interval: 1s
      timeout: 2s
      retries: 30
    volumes: [postgres-data:/var/lib/postgresql]
  message-server:
    image: ${FIXTURE_IMAGE}
    depends_on:
      postgres: {condition: service_healthy}
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8008/_p2p/health >/dev/null"]
      interval: 1s
      timeout: 2s
      retries: 30
    volumes:
      - message-config:/etc/dirextalk-message-server
      - message-data:/var/dirextalk-message-server
      - ./p2p:/var/dirextalk-message-server/p2p
      - plugin-data:/var/dirextalk-message-server/plugins
      - agent-data:/var/dirextalk-message-server/agent
      - ./health.json:/www/_p2p/health:ro
  caddy:
    image: ${FIXTURE_IMAGE}
volumes:
  postgres-data:
  message-config:
  message-data:
  plugin-data:
  agent-data:
`
	write("docker-compose.yml", compose, 0o600)
}

func assertFixtureMessageServerStopped(t *testing.T, ctx context.Context, runner hostCommandRunner) {
	t.Helper()
	var status bytes.Buffer
	if err := runner.Run(ctx, nil, &status, "docker", "inspect", "--format", "{{.State.Status}}", composeContainerName("message-server")); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(status.String()) != "exited" {
		t.Fatalf("intentionally stopped message-server was resurrected: %q", status.String())
	}
}

func seedFixtureState(t *testing.T, ctx context.Context, runtime *ComposeRuntime) {
	t.Helper()
	commands := [][]string{
		runtime.composeArgs("exec", "-T", "postgres", "psql", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE retained(value text); INSERT INTO retained VALUES ('before');"),
		runtime.composeArgs("exec", "-T", "message-server", "sh", "-c", "printf before > /etc/dirextalk-message-server/key; printf before > /var/dirextalk-message-server/media"),
	}
	for _, args := range commands {
		if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(runtime.paths.p2pDir, "bootstrap.json"), []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mutateFixtureState(t *testing.T, ctx context.Context, runtime *ComposeRuntime) {
	t.Helper()
	commands := [][]string{
		runtime.composeArgs("exec", "-T", "postgres", "psql", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-v", "ON_ERROR_STOP=1", "-c", "UPDATE retained SET value='after';"),
		runtime.composeArgs("exec", "-T", "message-server", "sh", "-c", "printf after > /etc/dirextalk-message-server/key; printf after > /var/dirextalk-message-server/media"),
	}
	for _, args := range commands {
		if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(runtime.paths.p2pDir, "bootstrap.json"), []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func verifyFixtureRestored(t *testing.T, ctx context.Context, runtime *ComposeRuntime) {
	t.Helper()
	var database bytes.Buffer
	if err := runtime.runner.Run(ctx, nil, &database, "docker", runtime.composeArgs("exec", "-T", "postgres", "psql", "-At", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-c", "SELECT value FROM retained;")...); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(database.String()) != "before" {
		t.Fatalf("database was not restored: %q", database.String())
	}
	var files bytes.Buffer
	if err := runtime.runner.Run(ctx, nil, &files, "docker", runtime.composeArgs("exec", "-T", "message-server", "sh", "-c", "cat /etc/dirextalk-message-server/key; printf ' '; cat /var/dirextalk-message-server/media")...); err != nil {
		t.Fatal(err)
	}
	p2p, err := os.ReadFile(filepath.Join(runtime.paths.p2pDir, "bootstrap.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(files.String()) != "before before" || string(p2p) != "before" {
		t.Fatalf("persistent files were not restored: volume=%q p2p=%q", files.String(), p2p)
	}
}

func waitForFixtureHealth(t *testing.T, ctx context.Context, runtime *ComposeRuntime, version string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		health, err := runtime.inspectHealth(ctx)
		if err == nil && health.Status == "ok" && health.Version == version {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("fixture message-server did not become healthy")
}

func randomFixtureID(t *testing.T) string {
	t.Helper()
	token, err := randomToken(6)
	if err != nil {
		t.Fatal(err)
	}
	return strings.NewReplacer("_", "", "-", "").Replace(token)
}
