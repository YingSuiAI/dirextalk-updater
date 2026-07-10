package updater

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	fixedComposeDir  = "/var/dirextalk-message-server"
	fixedP2PDir      = fixedComposeDir + "/p2p"
	fixedBackupRoot  = "/var/lib/dirextalk-updater/backup"
	maxCommandOutput = 1024 * 1024
	backupTimeout    = 15 * time.Minute
	mutationTimeout  = 10 * time.Minute
	restoreTimeout   = 15 * time.Minute
	healthTimeout    = 5 * time.Minute
)

type hostCommandRunner interface {
	Run(context.Context, io.Reader, io.Writer, string, ...string) error
}

type execHostCommandRunner struct{}

func (execHostCommandRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Stdin = stdin
	command.Stdout = stdout
	var stderr bytes.Buffer
	command.Stderr = &limitedWriter{writer: &stderr, remaining: 8 * 1024}
	if err := command.Run(); err != nil {
		// Docker stderr can contain environment-derived values. Keep the public
		// error bounded and opaque; operators can inspect systemd/Docker logs.
		return fmt.Errorf("host command %s failed: %w", name, err)
	}
	return nil
}

type limitedWriter struct {
	writer    io.Writer
	remaining int64
	exceeded  bool
}

func (writer *limitedWriter) Write(data []byte) (int, error) {
	original := len(data)
	accepted := 0
	if writer.remaining > 0 {
		part := data
		if int64(len(part)) > writer.remaining {
			part = part[:writer.remaining]
		}
		if _, err := writer.writer.Write(part); err != nil {
			return 0, err
		}
		accepted = len(part)
		writer.remaining -= int64(len(part))
	}
	if accepted < original {
		writer.exceeded = true
	}
	return original, nil
}

type composeRuntimePaths struct {
	composeFile       string
	envFile           string
	p2pDir            string
	backupRoot        string
	now               func() time.Time
	healthAttempts    int
	healthConsecutive int
	healthInterval    time.Duration
	sleep             func(context.Context, time.Duration) error
}

type ComposeRuntime struct {
	paths        composeRuntimePaths
	runner       hostCommandRunner
	backups      *BackupStore
	httpClient   *http.Client
	publicHealth func(context.Context, string) (runtimeHealth, error)
	caddyMode    CaddyMode
}

func NewComposeRuntime(caddyMode CaddyMode) (*ComposeRuntime, error) {
	if !caddyMode.valid() {
		return nil, fmt.Errorf("Caddy mode %q is not supported", caddyMode)
	}
	paths := composeRuntimePaths{
		composeFile:       fixedComposeDir + "/docker-compose.yml",
		envFile:           fixedComposeDir + "/.env",
		p2pDir:            fixedP2PDir,
		backupRoot:        fixedBackupRoot,
		now:               time.Now,
		healthAttempts:    60,
		healthConsecutive: 3,
		healthInterval:    5 * time.Second,
	}
	return newComposeRuntime(paths, execHostCommandRunner{}, &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, caddyMode), nil
}

func newComposeRuntime(paths composeRuntimePaths, runner hostCommandRunner, httpClient *http.Client, caddyMode CaddyMode) *ComposeRuntime {
	if paths.now == nil {
		paths.now = time.Now
	}
	if paths.healthAttempts <= 0 {
		paths.healthAttempts = 60
	}
	if paths.healthConsecutive <= 0 {
		paths.healthConsecutive = 3
	}
	if paths.healthInterval <= 0 {
		paths.healthInterval = 5 * time.Second
	}
	if paths.sleep == nil {
		paths.sleep = sleepContext
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	runtime := &ComposeRuntime{
		paths:      paths,
		runner:     runner,
		backups:    NewBackupStore(paths.backupRoot),
		httpClient: httpClient,
		caddyMode:  caddyMode,
	}
	runtime.publicHealth = runtime.fetchPublicHealth
	return runtime
}

func (runtime *ComposeRuntime) Recover(ctx context.Context) error {
	return runtime.backups.Recover(ctx)
}

func (runtime *ComposeRuntime) PrepareBackup(ctx context.Context, job Job, plan Plan, progress func(JobStatus) error) (metadata BackupMetadata, returnErr error) {
	ctx, cancel := context.WithTimeout(ctx, backupTimeout)
	defer cancel()
	if job.ID == "" || job.CurrentVersion != plan.CurrentVersion {
		return BackupMetadata{}, fmt.Errorf("job and plan source do not match")
	}
	if len(plan.ReleaseChain) != 1 {
		return BackupMetadata{}, fmt.Errorf("backup requires exactly one bound release step")
	}
	if err := validatePlanReleaseChain(plan); err != nil {
		return BackupMetadata{}, fmt.Errorf("backup release step is invalid: %w", err)
	}
	if progress == nil {
		return BackupMetadata{}, fmt.Errorf("backup progress callback is required")
	}
	if err := progress(JobValidating); err != nil {
		return BackupMetadata{}, err
	}
	step := plan.ReleaseChain[0]
	version, digest, health, err := runtime.ensureSourceReady(ctx, job.CurrentVersion, step)
	if err != nil {
		return BackupMetadata{}, err
	}
	if !digestAllowed(digest, step.SourceImageDigests) {
		return BackupMetadata{}, errUntrustedSourceImageDigest
	}
	if err := progress(JobBackingUp); err != nil {
		return BackupMetadata{}, err
	}
	staging, err := runtime.backups.Begin(ctx, job.ID)
	if err != nil {
		return BackupMetadata{}, err
	}
	stagingOwned := true
	defer func() {
		if stagingOwned {
			returnErr = errors.Join(returnErr, runtime.backups.Discard(staging))
		}
	}()
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("stop", "message-server")...); err != nil {
		return BackupMetadata{}, serviceUnavailableError{cause: fmt.Errorf("stop source message-server for backup: %w", err)}
	}
	backupErr := func() error {
		if err := runtime.writeCommandFile(ctx, filepath.Join(staging, "postgres.dump"), nil,
			runtime.composeArgs("exec", "-T", "postgres", "pg_dump", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-Fc")...); err != nil {
			return err
		}
		dump, err := os.Open(filepath.Join(staging, "postgres.dump"))
		if err != nil {
			return fmt.Errorf("open staged PostgreSQL dump: %w", err)
		}
		err = runtime.runner.Run(ctx, dump, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "pg_restore", "--list")...)
		closeErr := dump.Close()
		if err != nil || closeErr != nil {
			return errors.Join(fmt.Errorf("validate PostgreSQL dump: %w", err), closeErr)
		}
		if err := runtime.writeCommandFile(ctx, filepath.Join(staging, "message-config.tar"), nil,
			runtime.composeArgs("run", "--rm", "--no-deps", "--entrypoint", "tar", "message-server", "-C", "/etc/dirextalk-message-server", "-cf", "-", ".")...); err != nil {
			return err
		}
		if err := runtime.writeCommandFile(ctx, filepath.Join(staging, "message-data.tar"), nil,
			runtime.composeArgs("run", "--rm", "--no-deps", "--entrypoint", "tar", "message-server", "-C", "/var/dirextalk-message-server", "--exclude=./p2p", "-cf", "-", ".")...); err != nil {
			return err
		}
		return archiveDirectory(runtime.paths.p2pDir, filepath.Join(staging, "p2p.tar"))
	}()
	_, _, _, restartErr := runtime.ensureSourceReady(ctx, job.CurrentVersion, step)
	if restartErr != nil {
		return BackupMetadata{}, serviceUnavailableError{cause: errors.Join(backupErr, fmt.Errorf("restore source message-server after backup: %w", restartErr))}
	}
	if backupErr != nil {
		return BackupMetadata{}, backupErr
	}
	metadata = BackupMetadata{
		SchemaVersion:             BackupMetadataSchemaVersion,
		JobID:                     job.ID,
		Version:                   version,
		ImageDigest:               digest,
		ImageRef:                  pinnedImageRef(version, digest),
		DatabaseSchema:            health.SchemaVersion,
		SchemaCompatVersion:       health.SchemaCompatVersion,
		LegacyBootstrapAssumption: isTrustedLegacyBootstrapStep(job.CurrentVersion, digest, step),
		CreatedAt:                 runtime.paths.now().UTC(),
	}
	for _, name := range requiredBackupArtifacts {
		path := filepath.Join(staging, name)
		info, statErr := os.Stat(path)
		if statErr != nil {
			return BackupMetadata{}, fmt.Errorf("inspect staged artifact %s: %w", name, statErr)
		}
		digest, hashErr := fileSHA256(path)
		if hashErr != nil {
			return BackupMetadata{}, hashErr
		}
		metadata.Artifacts = append(metadata.Artifacts, BackupArtifact{Name: name, Size: info.Size(), SHA256: digest})
	}
	if err := WriteBackupMetadata(staging, metadata); err != nil {
		return BackupMetadata{}, err
	}
	if err := runtime.backups.Commit(ctx, staging); err != nil {
		return BackupMetadata{}, err
	}
	stagingOwned = false
	return metadata, nil
}

func (runtime *ComposeRuntime) ActivateTarget(ctx context.Context, manifest Manifest, progress func(JobStatus) error) error {
	ctx, cancel := context.WithTimeout(ctx, mutationTimeout)
	defer cancel()
	if err := manifest.Validate(); err != nil {
		return hostMutationError{cause: err}
	}
	if progress == nil {
		return hostMutationError{cause: fmt.Errorf("activation progress callback is required")}
	}
	if err := progress(JobPulling); err != nil {
		return hostMutationError{cause: err}
	}
	targetRef := manifest.Image + "@" + manifest.ImageDigest
	if err := runtime.pullVerifiedImage(ctx, targetRef, manifest.ImageDigest); err != nil {
		return hostMutationError{cause: err}
	}
	if err := progress(JobStopping); err != nil {
		return hostMutationError{cause: err}
	}
	if err := writePinnedImage(runtime.paths.envFile, targetRef, os.Rename, syncDirectory); err != nil {
		return hostMutationError{cause: err, mutated: true}
	}
	if err := progress(JobMigrating); err != nil {
		return hostMutationError{cause: err, mutated: true}
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "--force-recreate", "message-server")...); err != nil {
		return hostMutationError{cause: fmt.Errorf("recreate target message-server: %w", err), mutated: true}
	}
	if err := progress(JobStarting); err != nil {
		return hostMutationError{cause: err, mutated: true}
	}
	return nil
}

func (runtime *ComposeRuntime) CheckTarget(ctx context.Context, manifest Manifest) error {
	return runtime.waitExpected(ctx, manifest.Version, manifest.ImageDigest, manifest.SchemaVersion, manifest.SchemaCompatVersion)
}

func (runtime *ComposeRuntime) RestoreBackup(ctx context.Context, recovery BackupMetadata) error {
	ctx, cancel := context.WithTimeout(ctx, restoreTimeout)
	defer cancel()
	current, err := runtime.backups.Current(ctx)
	if err != nil {
		return err
	}
	if !sameRecoveryPoint(current, recovery) {
		return fmt.Errorf("requested recovery point is not the committed backup")
	}
	local, err := runtime.localPinnedImageAvailable(ctx, recovery.ImageRef, recovery.ImageDigest)
	if err != nil || !local {
		if err := runtime.pullVerifiedImage(ctx, recovery.ImageRef, recovery.ImageDigest); err != nil {
			return err
		}
	}
	if err := writePinnedImage(runtime.paths.envFile, recovery.ImageRef, os.Rename, syncDirectory); err != nil {
		return err
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("stop", "message-server")...); err != nil {
		return fmt.Errorf("stop target message-server: %w", err)
	}
	currentDir := filepath.Join(runtime.paths.backupRoot, committedBackupName)
	if err := runtime.restoreTar(ctx, filepath.Join(currentDir, "message-config.tar"), "/etc/dirextalk-message-server", ""); err != nil {
		return err
	}
	if err := runtime.restoreTar(ctx, filepath.Join(currentDir, "message-data.tar"), "/var/dirextalk-message-server", "p2p"); err != nil {
		return err
	}
	if err := restoreDirectoryArchive(filepath.Join(currentDir, "p2p.tar"), runtime.paths.p2pDir); err != nil {
		return err
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "psql", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-v", "ON_ERROR_STOP=1", "-c", "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")...); err != nil {
		return fmt.Errorf("reset recovery database: %w", err)
	}
	dump, err := os.Open(filepath.Join(currentDir, "postgres.dump"))
	if err != nil {
		return fmt.Errorf("open recovery PostgreSQL dump: %w", err)
	}
	err = runtime.runner.Run(ctx, dump, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "pg_restore", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "--no-owner", "--no-privileges", "--exit-on-error")...)
	closeErr := dump.Close()
	if err != nil || closeErr != nil {
		return errors.Join(fmt.Errorf("restore PostgreSQL dump: %w", err), closeErr)
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "psql", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-v", "ON_ERROR_STOP=1", "-c", "CHECKPOINT;")...); err != nil {
		return fmt.Errorf("checkpoint restored PostgreSQL data: %w", err)
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "sync"); err != nil {
		return fmt.Errorf("flush restored persistent state: %w", err)
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "--force-recreate", "message-server")...); err != nil {
		return fmt.Errorf("start recovered message-server: %w", err)
	}
	return nil
}

func (runtime *ComposeRuntime) CheckRestored(ctx context.Context, recovery BackupMetadata) error {
	if recovery.LegacyBootstrapAssumption {
		return runtime.waitExpectedWithMode(ctx, recovery.Version, recovery.ImageDigest, recovery.DatabaseSchema, recovery.SchemaCompatVersion, healthValidationLegacyBootstrapRestore)
	}
	return runtime.waitExpected(ctx, recovery.Version, recovery.ImageDigest, recovery.DatabaseSchema, recovery.SchemaCompatVersion)
}

func (runtime *ComposeRuntime) RestartCurrent(ctx context.Context, job Job) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "message-server")...); err != nil {
		return serviceUnavailableError{cause: fmt.Errorf("start current message-server: %w", err)}
	}
	imageRef, err := readEnvironmentValue(runtime.paths.envFile, "MESSAGE_SERVER_IMAGE")
	if err != nil {
		return err
	}
	version, digest, err := parsePinnedImageRef(imageRef)
	if err != nil || (version != job.CurrentVersion && version != job.TargetVersion) {
		return fmt.Errorf("current pinned image does not belong to the job version edge")
	}
	consecutive := 0
	var lastErr error
	for attempt := 0; attempt < runtime.paths.healthAttempts; attempt++ {
		lastErr = runtime.checkCurrentOnce(ctx, version, digest)
		if lastErr == nil {
			consecutive++
			if consecutive >= runtime.paths.healthConsecutive {
				return nil
			}
		} else {
			consecutive = 0
		}
		if attempt+1 < runtime.paths.healthAttempts {
			if err := runtime.paths.sleep(ctx, runtime.paths.healthInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("current service did not pass the confirmation window: %w", lastErr)
}

// ObserveWatchdog verifies the already configured release without resolving or
// mutating any release input. Repeated failed observations are budgeted by
// Watchdog before RepairWatchdog is allowed to touch the host.
func (runtime *ComposeRuntime) ObserveWatchdog(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	if err := runtime.runner.Run(ctx, nil, io.Discard, "systemctl", "is-active", "--quiet", "docker.service"); err != nil {
		return fmt.Errorf("Docker service is unavailable")
	}
	imageRef, err := readEnvironmentValue(runtime.paths.envFile, "MESSAGE_SERVER_IMAGE")
	if err != nil {
		return err
	}
	version, digest, err := parsePinnedImageRef(imageRef)
	if err != nil {
		return fmt.Errorf("configured message-server image is not pinned")
	}
	if err := runtime.checkCurrentOnce(ctx, version, digest); err != nil {
		return fmt.Errorf("configured message-server release is unhealthy")
	}
	return nil
}

// RepairWatchdog starts only the fixed, currently pinned host topology. It
// never pulls an image, resolves a release, rotates backup state, or runs an
// upgrade/migration command.
func (runtime *ComposeRuntime) RepairWatchdog(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	if err := runtime.runner.Run(ctx, nil, io.Discard, "systemctl", "start", "docker.service"); err != nil {
		return fmt.Errorf("start Docker service: %w", err)
	}
	imageRef, err := readEnvironmentValue(runtime.paths.envFile, "MESSAGE_SERVER_IMAGE")
	if err != nil {
		return err
	}
	version, digest, err := parsePinnedImageRef(imageRef)
	if err != nil {
		return fmt.Errorf("configured message-server image is not pinned")
	}
	local, err := runtime.localPinnedImageAvailable(ctx, imageRef, digest)
	if err != nil || !local {
		return fmt.Errorf("configured pinned message-server image is not available locally")
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "--pull", "never", "postgres")...); err != nil {
		return fmt.Errorf("start watchdog PostgreSQL: %w", err)
	}
	if err := runtime.waitWatchdogPostgres(ctx); err != nil {
		return err
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "--pull", "never", "message-server")...); err != nil {
		return fmt.Errorf("start watchdog message-server: %w", err)
	}
	switch runtime.caddyMode {
	case CaddyModeCompose:
		if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "--pull", "never", "caddy")...); err != nil {
			return fmt.Errorf("start watchdog Compose Caddy: %w", err)
		}
	case CaddyModeSystemd:
		if err := runtime.runner.Run(ctx, nil, io.Discard, "systemctl", "start", "caddy.service"); err != nil {
			return fmt.Errorf("start watchdog systemd Caddy: %w", err)
		}
	default:
		return fmt.Errorf("configured Caddy mode is invalid")
	}
	consecutive := 0
	var lastErr error
	for attempt := 0; attempt < runtime.paths.healthAttempts; attempt++ {
		lastErr = runtime.checkCurrentOnce(ctx, version, digest)
		if lastErr == nil {
			consecutive++
			if consecutive >= runtime.paths.healthConsecutive {
				return nil
			}
		} else {
			consecutive = 0
		}
		if attempt+1 < runtime.paths.healthAttempts {
			if err := runtime.paths.sleep(ctx, runtime.paths.healthInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("watchdog repair did not restore the pinned release: %w", lastErr)
}

func (runtime *ComposeRuntime) waitWatchdogPostgres(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < runtime.paths.healthAttempts; attempt++ {
		lastErr = runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "pg_isready", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server")...)
		if lastErr == nil {
			return nil
		}
		if attempt+1 < runtime.paths.healthAttempts {
			if err := runtime.paths.sleep(ctx, runtime.paths.healthInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("watchdog PostgreSQL is not ready: %w", lastErr)
}

func (runtime *ComposeRuntime) StreamWatchdogEvents(ctx context.Context, notify func()) error {
	if notify == nil {
		return fmt.Errorf("watchdog event callback is required")
	}
	writer := &watchdogEventWriter{notify: notify}
	return runtime.runner.Run(ctx, nil, writer, "docker", "events",
		"--filter", "label=com.docker.compose.project="+AllowedComposeProject,
		"--filter", "event=die",
		"--filter", "event=stop",
		"--filter", "event=kill",
		"--format", "{{.ID}}",
	)
}

type watchdogEventWriter struct {
	pending []byte
	notify  func()
}

func (writer *watchdogEventWriter) Write(data []byte) (int, error) {
	original := len(data)
	writer.pending = append(writer.pending, data...)
	for {
		newline := bytes.IndexByte(writer.pending, '\n')
		if newline < 0 {
			break
		}
		line := bytes.TrimSpace(writer.pending[:newline])
		writer.pending = writer.pending[newline+1:]
		if len(line) > 0 {
			writer.notify()
		}
	}
	return original, nil
}

func (runtime *ComposeRuntime) ensureSourceReady(ctx context.Context, expectedVersion string, step ReleaseStep) (string, string, runtimeHealth, error) {
	imageRef, err := readEnvironmentValue(runtime.paths.envFile, "MESSAGE_SERVER_IMAGE")
	if err != nil {
		return "", "", runtimeHealth{}, err
	}
	version, digest, err := parsePinnedImageRef(imageRef)
	if err != nil || version != expectedVersion {
		return "", "", runtimeHealth{}, fmt.Errorf("pinned source image does not match the planned version")
	}
	if !digestAllowed(digest, step.SourceImageDigests) {
		return "", "", runtimeHealth{}, errUntrustedSourceImageDigest
	}
	mode := healthValidationStrict
	if isTrustedLegacyBootstrapStep(expectedVersion, digest, step) {
		mode = healthValidationLegacyBootstrapSource
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("up", "-d", "--no-deps", "message-server")...); err != nil {
		return "", "", runtimeHealth{}, serviceUnavailableError{cause: fmt.Errorf("start source message-server: %w", err)}
	}
	consecutive := 0
	var lastErr error
	var health runtimeHealth
	for attempt := 0; attempt < runtime.paths.healthAttempts; attempt++ {
		health, lastErr = runtime.checkCurrentHealthWithMode(ctx, version, digest, mode)
		if lastErr == nil {
			consecutive++
			if consecutive >= runtime.paths.healthConsecutive {
				return version, digest, health, nil
			}
		} else {
			consecutive = 0
		}
		if attempt+1 < runtime.paths.healthAttempts {
			if err := runtime.paths.sleep(ctx, runtime.paths.healthInterval); err != nil {
				return "", "", runtimeHealth{}, serviceUnavailableError{cause: err}
			}
		}
	}
	return "", "", runtimeHealth{}, serviceUnavailableError{cause: fmt.Errorf("source service did not recover: %w", lastErr)}
}

type runtimeHealth struct {
	Status              string `json:"status"`
	Version             string `json:"version"`
	SchemaVersion       int    `json:"schema_version"`
	SchemaCompatVersion int    `json:"schema_compat_version"`
	exactMinimal        bool
}

func (health *runtimeHealth) UnmarshalJSON(data []byte) error {
	type wireHealth struct {
		Status              string `json:"status"`
		Version             string `json:"version"`
		SchemaVersion       int    `json:"schema_version"`
		SchemaCompatVersion int    `json:"schema_compat_version"`
	}
	var wire wireHealth
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	_, hasStatus := fields["status"]
	*health = runtimeHealth{
		Status: wire.Status, Version: wire.Version, SchemaVersion: wire.SchemaVersion,
		SchemaCompatVersion: wire.SchemaCompatVersion, exactMinimal: hasStatus && len(fields) == 1,
	}
	return nil
}

type healthValidationMode uint8

const (
	healthValidationStrict healthValidationMode = iota
	healthValidationLegacyBootstrapSource
	healthValidationLegacyBootstrapRestore
)

func isTrustedLegacyBootstrapStep(sourceVersion, sourceDigest string, step ReleaseStep) bool {
	return sourceVersion == legacyInitialVersion && step.Manifest.Version == firstFormalVersion && digestAllowed(sourceDigest, step.SourceImageDigests)
}

type serviceUnavailableError struct {
	cause error
}

func (err serviceUnavailableError) Error() string      { return err.cause.Error() }
func (err serviceUnavailableError) Unwrap() error      { return err.cause }
func (serviceUnavailableError) ServiceAvailable() bool { return false }

type hostMutationError struct {
	cause   error
	mutated bool
}

func (err hostMutationError) Error() string         { return err.cause.Error() }
func (err hostMutationError) Unwrap() error         { return err.cause }
func (err hostMutationError) MutationStarted() bool { return err.mutated }

func (runtime *ComposeRuntime) inspectHealth(ctx context.Context) (runtimeHealth, error) {
	output, err := runtime.commandOutput(ctx, "docker", runtime.composeArgs("exec", "-T", "message-server", "wget", "-qO-", "http://127.0.0.1:8008/_p2p/health")...)
	if err != nil {
		return runtimeHealth{}, fmt.Errorf("read message-server health: %w", err)
	}
	var health runtimeHealth
	decoder := json.NewDecoder(strings.NewReader(output))
	if err := decoder.Decode(&health); err != nil {
		return runtimeHealth{}, fmt.Errorf("decode message-server health: %w", err)
	}
	if err := ensureJSONEOF(decoder, "message-server health"); err != nil {
		return runtimeHealth{}, err
	}
	return health, nil
}

func (runtime *ComposeRuntime) waitExpected(ctx context.Context, version, digest string, schemaVersion, schemaCompatVersion int) error {
	return runtime.waitExpectedWithMode(ctx, version, digest, schemaVersion, schemaCompatVersion, healthValidationStrict)
}

func (runtime *ComposeRuntime) waitExpectedWithMode(ctx context.Context, version, digest string, schemaVersion, schemaCompatVersion int, mode healthValidationMode) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	consecutive := 0
	var lastErr error
	for attempt := 0; attempt < runtime.paths.healthAttempts; attempt++ {
		lastErr = runtime.checkExpectedOnceWithMode(ctx, version, digest, schemaVersion, schemaCompatVersion, mode)
		if lastErr == nil {
			consecutive++
			if consecutive >= runtime.paths.healthConsecutive {
				return nil
			}
		} else {
			consecutive = 0
		}
		if attempt+1 < runtime.paths.healthAttempts {
			if err := runtime.paths.sleep(ctx, runtime.paths.healthInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("release did not pass the confirmation window: %w", lastErr)
}

func (runtime *ComposeRuntime) checkExpectedOnce(ctx context.Context, version, digest string, schemaVersion, schemaCompatVersion int) error {
	return runtime.checkExpectedOnceWithMode(ctx, version, digest, schemaVersion, schemaCompatVersion, healthValidationStrict)
}

func (runtime *ComposeRuntime) checkExpectedOnceWithMode(ctx context.Context, version, digest string, schemaVersion, schemaCompatVersion int, mode healthValidationMode) error {
	health, err := runtime.checkCurrentHealthWithMode(ctx, version, digest, mode)
	if err != nil {
		return err
	}
	if health.SchemaVersion != schemaVersion || health.SchemaCompatVersion != schemaCompatVersion {
		return fmt.Errorf("message-server schema health does not match expected release")
	}
	return nil
}

func (runtime *ComposeRuntime) checkCurrentOnce(ctx context.Context, version, digest string) error {
	_, err := runtime.checkCurrentHealth(ctx, version, digest)
	return err
}

func (runtime *ComposeRuntime) checkCurrentHealth(ctx context.Context, version, digest string) (runtimeHealth, error) {
	return runtime.checkCurrentHealthWithMode(ctx, version, digest, healthValidationStrict)
}

func (runtime *ComposeRuntime) checkCurrentHealthWithMode(ctx context.Context, version, digest string, mode healthValidationMode) (runtimeHealth, error) {
	if runtime.caddyMode == CaddyModeSystemd {
		if err := runtime.runner.Run(ctx, nil, io.Discard, "systemctl", "is-active", "--quiet", "caddy.service"); err != nil {
			return runtimeHealth{}, fmt.Errorf("systemd Caddy service is unavailable")
		}
	} else if runtime.caddyMode != CaddyModeCompose {
		return runtimeHealth{}, fmt.Errorf("configured Caddy mode is invalid")
	}
	containerState, err := runtime.commandOutput(ctx, "docker", "inspect", "--format", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}", composeContainerName("message-server"))
	if err != nil {
		return runtimeHealth{}, err
	}
	if strings.TrimSpace(containerState) != "running healthy" {
		return runtimeHealth{}, fmt.Errorf("message-server container is not running and Docker-healthy")
	}
	imageRef, err := runtime.commandOutput(ctx, "docker", "inspect", "--format", "{{.Config.Image}}", composeContainerName("message-server"))
	if err != nil {
		return runtimeHealth{}, err
	}
	actualVersion, actualDigest, err := parsePinnedImageRef(strings.TrimSpace(imageRef))
	if err != nil || actualVersion != version || actualDigest != digest {
		return runtimeHealth{}, fmt.Errorf("running image does not match expected release")
	}
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "pg_isready", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server")...); err != nil {
		return runtimeHealth{}, fmt.Errorf("PostgreSQL is not ready: %w", err)
	}
	probe := "BEGIN; CREATE TEMP TABLE dirextalk_updater_probe(value integer); INSERT INTO dirextalk_updater_probe VALUES (1); SELECT value FROM dirextalk_updater_probe; ROLLBACK;"
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", runtime.composeArgs("exec", "-T", "postgres", "psql", "-U", "dirextalk_message_server", "-d", "dirextalk_message_server", "-v", "ON_ERROR_STOP=1", "-c", probe)...); err != nil {
		return runtimeHealth{}, fmt.Errorf("PostgreSQL read/write probe failed: %w", err)
	}
	health, err := runtime.inspectHealth(ctx)
	if err != nil {
		return runtimeHealth{}, err
	}
	strictHealth := health.Status == "ok" && health.Version == version && health.SchemaVersion >= 1 && health.SchemaCompatVersion >= 1 && health.SchemaCompatVersion <= health.SchemaVersion
	legacyFullHealth := health.Status == "ok" && healthVersionMatches(version, health.Version) && health.SchemaVersion >= 1 && health.SchemaCompatVersion >= 1 && health.SchemaCompatVersion <= health.SchemaVersion
	legacyMinimalHealth := health.Status == "ok" && health.exactMinimal
	legacyMode := mode == healthValidationLegacyBootstrapSource || mode == healthValidationLegacyBootstrapRestore
	if !strictHealth && !(legacyMode && version == legacyInitialVersion && (legacyFullHealth || legacyMinimalHealth)) {
		return runtimeHealth{}, fmt.Errorf("message-server build health does not match expected release")
	}
	domain, err := readEnvironmentValue(runtime.paths.envFile, "DOMAIN")
	if err != nil {
		return runtimeHealth{}, err
	}
	publicHealth, err := runtime.publicHealth(ctx, domain)
	if err != nil {
		return runtimeHealth{}, fmt.Errorf("Caddy public health is unavailable: %w", err)
	}
	if publicHealth != health {
		return runtimeHealth{}, fmt.Errorf("Caddy public health does not match internal health")
	}
	if legacyMode && legacyMinimalHealth {
		return runtimeHealth{Status: "ok", Version: legacyInitialVersion, SchemaVersion: 1, SchemaCompatVersion: 1}, nil
	}
	return health, nil
}

func healthVersionMatches(expected, observed string) bool {
	if observed == expected {
		return true
	}
	return expected == legacyInitialVersion && observed == strings.TrimPrefix(legacyInitialVersion, "v")
}

func (runtime *ComposeRuntime) fetchPublicHealth(ctx context.Context, domain string) (runtimeHealth, error) {
	if !safePublicDomain(domain) {
		return runtimeHealth{}, fmt.Errorf("public domain is invalid")
	}
	endpoint := (&url.URL{Scheme: "https", Host: domain, Path: "/_p2p/health"}).String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return runtimeHealth{}, err
	}
	response, err := runtime.httpClient.Do(request)
	if err != nil {
		return runtimeHealth{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return runtimeHealth{}, fmt.Errorf("public health returned HTTP %d", response.StatusCode)
	}
	var health runtimeHealth
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64*1024))
	if err := decoder.Decode(&health); err != nil {
		return runtimeHealth{}, err
	}
	if err := ensureJSONEOF(decoder, "public message-server health"); err != nil {
		return runtimeHealth{}, err
	}
	return health, nil
}

func (runtime *ComposeRuntime) restoreTar(ctx context.Context, archivePath, destination, preserve string) error {
	archive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open recovery archive: %w", err)
	}
	cleanup := "find \"$1\" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +"
	extra := []string{"dirextalk-restore", destination}
	if destination == "/var/dirextalk-message-server" && preserve == "p2p" {
		cleanup = "find \"$1\" -mindepth 1 -maxdepth 1 ! -name \"$2\" -exec rm -rf -- {} +"
		extra = append(extra, preserve)
	} else if destination != "/etc/dirextalk-message-server" || preserve != "" {
		_ = archive.Close()
		return fmt.Errorf("persistent restore destination is not allowed")
	}
	args := runtime.composeArgs("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "message-server", "-ec", cleanup+"; tar -C \"$1\" -xf -; sync")
	args = append(args, extra...)
	err = runtime.runner.Run(ctx, archive, io.Discard, "docker", args...)
	closeErr := archive.Close()
	if err != nil || closeErr != nil {
		return errors.Join(fmt.Errorf("restore persistent archive: %w", err), closeErr)
	}
	return nil
}

func (runtime *ComposeRuntime) commandOutput(ctx context.Context, name string, args ...string) (string, error) {
	var output bytes.Buffer
	limited := &limitedWriter{writer: &output, remaining: maxCommandOutput}
	if err := runtime.runner.Run(ctx, nil, limited, name, args...); err != nil {
		return "", err
	}
	if limited.exceeded {
		return "", fmt.Errorf("host command output exceeded the fixed limit")
	}
	return output.String(), nil
}

func (runtime *ComposeRuntime) pullVerifiedImage(ctx context.Context, imageRef, expectedDigest string) error {
	if err := runtime.runner.Run(ctx, nil, io.Discard, "docker", "pull", imageRef); err != nil {
		return fmt.Errorf("pull pinned message-server image: %w", err)
	}
	repoDigests, err := runtime.commandOutput(ctx, "docker", "image", "inspect", "--format", "{{join .RepoDigests \"\\n\"}}", imageRef)
	if err != nil {
		return err
	}
	if !linePresent(repoDigests, AllowedImageRepository+"@"+expectedDigest) {
		return fmt.Errorf("pulled image digest does not match the expected release")
	}
	return nil
}

func (runtime *ComposeRuntime) localPinnedImageAvailable(ctx context.Context, imageRef, expectedDigest string) (bool, error) {
	repoDigests, err := runtime.commandOutput(ctx, "docker", "image", "inspect", "--format", "{{join .RepoDigests \"\\n\"}}", imageRef)
	if err != nil {
		return false, err
	}
	return linePresent(repoDigests, AllowedImageRepository+"@"+expectedDigest), nil
}

func (runtime *ComposeRuntime) writeCommandFile(ctx context.Context, path string, stdin io.Reader, args ...string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create staged backup artifact: %w", err)
	}
	runErr := runtime.runner.Run(ctx, stdin, file, "docker", args...)
	syncErr := file.Sync()
	closeErr := file.Close()
	if runErr != nil || syncErr != nil || closeErr != nil {
		return errors.Join(runErr, syncErr, closeErr)
	}
	return nil
}

func (runtime *ComposeRuntime) composeArgs(args ...string) []string {
	prefix := []string{"compose", "--project-name", AllowedComposeProject, "--file", runtime.paths.composeFile}
	return append(prefix, args...)
}

func composeContainerName(service string) string {
	return AllowedComposeProject + "-" + service + "-1"
}

func parsePinnedImageRef(value string) (string, string, error) {
	prefix := AllowedImageRepository + ":"
	if !strings.HasPrefix(value, prefix) {
		return "", "", fmt.Errorf("image repository is not allowed")
	}
	parts := strings.Split(strings.TrimPrefix(value, prefix), "@")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("image reference must contain one digest")
	}
	if _, err := parseCanonicalVersion("image version", parts[0]); err != nil {
		return "", "", err
	}
	if !digestPattern.MatchString(parts[1]) {
		return "", "", fmt.Errorf("image digest is invalid")
	}
	return parts[0], parts[1], nil
}

func pinnedImageRef(version, digest string) string {
	return AllowedImageRepository + ":" + version + "@" + digest
}

func writePinnedImage(envPath, imageRef string, replace func(string, string) error, syncDir func(string) error) error {
	if _, _, err := parsePinnedImageRef(imageRef); err != nil {
		return err
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		return fmt.Errorf("read Compose environment: %w", err)
	}
	if len(data) > maxCommandOutput || bytes.Contains(data, []byte{'\r'}) {
		return fmt.Errorf("Compose environment is invalid")
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	replaced := 0
	for index, line := range lines {
		if strings.HasPrefix(line, "MESSAGE_SERVER_IMAGE=") {
			lines[index] = "MESSAGE_SERVER_IMAGE=" + imageRef
			replaced++
		}
	}
	if replaced != 1 {
		return fmt.Errorf("Compose environment must contain exactly one MESSAGE_SERVER_IMAGE")
	}
	updated := []byte(strings.Join(lines, "\n") + "\n")
	directory := filepath.Dir(envPath)
	temporary, err := os.CreateTemp(directory, ".env.updater-*")
	if err != nil {
		return fmt.Errorf("create Compose environment temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(updated); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := replace(temporaryPath, envPath); err != nil {
		return fmt.Errorf("replace Compose environment: %w", err)
	}
	if err := syncDir(directory); err != nil {
		return fmt.Errorf("fsync Compose environment directory: %w", err)
	}
	return nil
}

func archiveDirectory(source, destination string) error {
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create host-state archive: %w", err)
	}
	archive := tar.NewWriter(file)
	err = filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("host-state archive rejects symbolic links")
		}
		if path == source {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(relative)
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(archive, input)
		closeErr := input.Close()
		return errors.Join(copyErr, closeErr)
	})
	closeArchiveErr := archive.Close()
	syncErr := file.Sync()
	closeFileErr := file.Close()
	if err != nil || closeArchiveErr != nil || syncErr != nil || closeFileErr != nil {
		return errors.Join(err, closeArchiveErr, syncErr, closeFileErr)
	}
	return nil
}

func restoreDirectoryArchive(archivePath, destination string) error {
	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("clear host-state directory: %w", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return fmt.Errorf("create host-state directory: %w", err)
	}
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := tar.NewReader(file)
	directories := map[string]struct{}{destination: {}}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("host-state archive contains an unsafe path")
		}
		target := filepath.Join(destination, clean)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)&0o700); err != nil {
				return err
			}
			directories[target] = struct{}{}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			directories[filepath.Dir(target)] = struct{}{}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode)&0o600)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(output, reader, header.Size)
			syncErr := output.Sync()
			closeErr := output.Close()
			if copyErr != nil || syncErr != nil || closeErr != nil {
				return errors.Join(copyErr, syncErr, closeErr)
			}
		default:
			return fmt.Errorf("host-state archive contains an unsupported entry")
		}
	}
	orderedDirectories := make([]string, 0, len(directories))
	for directory := range directories {
		orderedDirectories = append(orderedDirectories, directory)
	}
	sort.Slice(orderedDirectories, func(i, j int) bool {
		return len(orderedDirectories[i]) > len(orderedDirectories[j])
	})
	for _, directory := range orderedDirectories {
		if err := syncDirectory(directory); err != nil {
			return fmt.Errorf("fsync restored host-state directory: %w", err)
		}
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return fmt.Errorf("fsync host-state parent directory: %w", err)
	}
	return nil
}

func linePresent(output, expected string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == expected {
			return true
		}
	}
	return false
}

func readEnvironmentValue(path, key string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read Compose environment: %w", err)
	}
	if len(data) > maxCommandOutput || bytes.Contains(data, []byte{'\r'}) {
		return "", fmt.Errorf("Compose environment is invalid")
	}
	prefix := key + "="
	value := ""
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, prefix) {
			value = strings.TrimPrefix(line, prefix)
			count++
		}
	}
	if count != 1 || strings.TrimSpace(value) != value || value == "" {
		return "", fmt.Errorf("Compose environment must contain exactly one valid %s", key)
	}
	return value, nil
}

func safePublicDomain(domain string) bool {
	if len(domain) == 0 || len(domain) > 253 || domain != strings.ToLower(domain) || net.ParseIP(domain) != nil {
		return false
	}
	labels := strings.Split(domain, ".")
	if len(labels) < 2 || labels[len(labels)-1] == "local" {
		return false
	}
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sameRecoveryPoint(left, right BackupMetadata) bool {
	leftArtifacts := append([]BackupArtifact(nil), left.Artifacts...)
	rightArtifacts := append([]BackupArtifact(nil), right.Artifacts...)
	sort.Slice(leftArtifacts, func(i, j int) bool { return leftArtifacts[i].Name < leftArtifacts[j].Name })
	sort.Slice(rightArtifacts, func(i, j int) bool { return rightArtifacts[i].Name < rightArtifacts[j].Name })
	left.Artifacts = leftArtifacts
	right.Artifacts = rightArtifacts
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}
