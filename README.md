# Dirextalk Updater

Dirextalk Updater is a small host service that keeps server upgrade control
available while the containerized message server is stopped or restarting. It
accepts a centrally selected stable target version, pins that target to its
resolved Docker digest, and owns durable job state plus the fixed
host-operation boundary. It does not accept arbitrary shell, Compose, image,
digest, URL, or service input from callers.

Version 1 supports Ubuntu 22.04 and 24.04 on `linux/amd64`. The daemon listens on a
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

`serve` runs a host preflight and refuses every platform except Ubuntu 22.04
or 24.04 `linux/amd64`. `version` is read-only and may run on development
systems.

## Runtime configuration

The default config is `/etc/dirextalk-updater/config.json`:

```json
{
  "schema_version": 1,
  "state_dir": "/var/lib/dirextalk-updater",
  "socket_path": "/run/dirextalk-updater/http.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token",
  "caddy_mode": "compose"
}
```

Start the daemon as root with `dirextalk-updater -config <path> serve`. State
is stored with restrictive permissions and atomic replacement. The control
token must be a root-owned regular file with permissions exactly `0600`; a
non-root runtime fails closed.
The config file has the same root ownership and exact `0600` requirement and
must be a regular non-symlink file.
`caddy_mode` is the fixed enum `compose` or `systemd`; omitted legacy configs
default to `compose`. Systemd mode can operate only `caddy.service`. This mode
comes only from the root-owned config and is never accepted by the API.
`compose_project` is optional and defaults to `dirextalk-p2p`. The only other
accepted value is the code-owned `dirextalk-message-server` legacy migration
layout. It is root configuration, never a control-API input.

The v1 Unix API prefix is `/_dirextalk/updater/v1/`. The root-owned control
token is required for all control routes. `POST control/status` accepts only
`{}` and reads the current server version from the host's pinned image. It
returns `current_version`, `updater_ready`, desired state, any active job, and
watchdog state.

`POST control/jobs` accepts only this strict request shape:

```json
{
  "target_version": "v1.0.3",
  "idempotency_key": "lowercase UUID",
  "confirm": "apply_release_change"
}
```

The target must be canonical `vX.Y.Z` and greater than the host's current
version. The updater pulls only `dirextalk/message-server:<target_version>`,
resolves exactly one repository digest, and atomically persists the
digest-pinned target before the server is stopped. Every new job is a single
hop. GitHub Release discovery, release-index freshness, plan tokens, and
multi-hop selection are not part of the active control path.
Existing persisted legacy jobs retain their plans only so automatic recovery
can finish after an updater replacement; no new GitHub-discovered plan can be
created.
The `upgrading` desired state is internal to the job transaction, and external
desired-state changes are rejected while a job is active.

An accepted job persists its checkpoint before host mutation. The updater
stops message-server briefly to create a consistent PostgreSQL custom dump,
message configuration/data archives, and host `p2p` archive. Checksums and
source build/image/schema metadata are validated before the staging directory
atomically replaces `backup/current`; only that one committed recovery point
is retained. A corrupt staging backup never replaces it.

The target is pulled and recreated only as `vX.Y.Z@sha256:...`. Success needs
consecutive agreement between the running container image, PostgreSQL,
internal health, and the same-domain Caddy health endpoint. Failure starts
automatic recovery, and every restore checkpoint survives an updater restart.
After three recovery failures the job enters maintenance and may expose only
`restart` through `POST /_dirextalk/updater/v1/jobs/{job_id}/restart` with the
job bearer. Manual rollback is never exposed; internal automatic recovery is
retained. No infrastructure parameters are accepted by these routes.

The resident process also monitors the fixed Compose project through Docker
failure events and a 30-second reconciliation loop. Recovery is allowed only
while the persisted desired state is `running`, requires three failed
observations, uses at most three repair attempts in ten minutes, and enters a
15-minute degraded cooldown when that budget is exhausted. Repair starts only
Docker, PostgreSQL, message-server, and Caddy in that order using the already
configured local tag-and-digest image. It never resolves a Release, pulls
`latest`, rotates a backup, or runs a migration.
In `systemd` mode the PostgreSQL and message-server steps remain in the fixed
Compose project while Caddy observation and repair use only `caddy.service`.

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
