# Dirextalk Updater

Dirextalk Updater is a small host service that keeps server upgrade control
available while the containerized message server is stopped or restarting. It
owns release discovery, compatibility plans, durable job state, and the fixed
host-operation boundary. It does not accept arbitrary shell, Compose, image,
or service input from callers.

Version 1 supports only Ubuntu 24.04 on `linux/amd64`. The daemon listens on a
Unix socket; it does not expose a TCP port. The message server may call the
root-owned control routes through the mounted socket, while a client receives
only a scoped job bearer for progress polling.

## Development

Normal development requires Go 1.24 or newer. Formal release assets are built
with exactly Go 1.24.13.

```text
go test ./...
go test -race ./...
go vet ./...
go mod verify
```

Local builds identify themselves as `v0.0.0-dev+local`:

```text
go build -o dirextalk-updater ./cmd/dirextalk-updater
./dirextalk-updater version
```

`serve` and `trigger-discovery` run a host preflight and refuse every platform
except Ubuntu 24.04 `linux/amd64`. `resolve-release` and `version` are read-only
development/inspection commands.

## Runtime configuration

The default config is `/etc/dirextalk-updater/config.json`:

```json
{
  "schema_version": 1,
  "state_dir": "/var/lib/dirextalk-updater",
  "socket_path": "/run/dirextalk-updater/http.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token"
}
```

Start the daemon as root with `dirextalk-updater -config <path> serve`. State
is stored with restrictive permissions and atomic replacement. The control
token must be a root-owned regular file with permissions exactly `0600`; a
non-root runtime fails closed.

The frozen v1 Unix API prefix is `/_dirextalk/updater/v1/`. It includes control
routes for release discovery, status, desired state, and job creation, plus a
bearer-scoped job status route. Compatibility and operation availability are
server decisions; clients must not infer an upgrade path locally.
Discovered Release metadata is fresh for at most 36 hours. Older, future, or
missing check timestamps are treated as stale and cannot issue a plan.
The `upgrading` desired state is internal to the job transaction, and external
desired-state changes are rejected while a job is active.

## Release assets

A stable `vX.Y.Z` tag runs CI on `ubuntu-24.04` with Go 1.24.13, builds one
statically linked `linux/amd64` binary twice with separate caches, requires
byte-identical output, verifies its embedded identity, and publishes exactly:

- `dirextalk-updater-linux-amd64`
- `dirextalk-updater-linux-amd64.sha256`
- `dirextalk-updater-release.json`

The JSON manifest binds the version, source commit, build time, supported OS,
architecture, Ubuntu version, asset name, and SHA-256 digest. No `latest`
binary URL is treated as an immutable installation target; installers resolve
a formal tagged Release and verify the manifest and checksum.

The embedded build time is the tagged commit's UTC commit timestamp, not the
runner clock. From a clean checkout of that commit, pre-release digest
calculation on Ubuntu 24.04 `linux/amd64` uses the same contract as CI:

```text
VERSION=v1.0.0 COMMIT=<full-commit> BUILD_TIME=<commit-time-in-UTC> scripts/build-release.sh
```

Official GitHub Actions and the exact Go version are pinned in
[dependency-pins.md](.github/dependency-pins.md).

See [README_zh.md](README_zh.md) for Chinese documentation.
