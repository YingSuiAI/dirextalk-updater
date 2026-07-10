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
Discovery accepts only `release-index.json` and `release-index.json.sha256`
from the latest published stable GitHub Release. The checksum binds the whole
index; every embedded manifest has its own digest, and every upgrade edge
binds exact source image digests. A direct edge is preferred. Otherwise the
path to the latest release must be unique, and the exact ordered chain is
persisted in the plan before a job is created.
Both the index and its embedded manifests use deterministic compact JSON so
their digests can be rechecked after state persistence. Formal source releases
must remain in the index. The only unindexed bootstrap source is the explicit
`v0.15.2 -> v1.0.0` legacy edge.
That bootstrap edge alone may accept the legacy `{\"status\":\"ok\"}` health
shape after the canonical pinned image and edge digest match. Its backup records
an explicit schema `1/1` assumption so only that legacy recovery can use the
same narrow health check. Watchdog, formal targets, and formal restores remain
on the complete version/schema health contract.
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
internal health, schema metadata, and the same-domain Caddy health endpoint.
Failure starts automatic rollback, and every restore checkpoint survives an
updater restart. After three recovery failures the job enters maintenance and
offers only the persisted `rollback`/`restart` operations through
`POST /_dirextalk/updater/v1/jobs/{job_id}/{operation}` with the job bearer.
No infrastructure parameters are accepted by these routes.
For a multi-hop plan, each hop independently rotates the single backup,
activates its immutable image, runs migrations, and passes the complete health
gate before the next hop starts. An observed source digest mismatch stops
before target mutation. A later-hop failure restores the most recent healthy
hop, not the original installation.

The resident process also monitors the fixed Compose project through Docker
failure events and a 30-second reconciliation loop. Recovery is allowed only
while the persisted desired state is `running`, requires three failed
observations, uses at most three repair attempts in ten minutes, and enters a
15-minute degraded cooldown when that budget is exhausted. Repair starts only
Docker, PostgreSQL, message-server, and Caddy in that order using the already
configured local tag-and-digest image. It never resolves a Release, pulls
`latest`, rotates a backup, or runs a migration.

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
