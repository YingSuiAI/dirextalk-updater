# Dirextalk Updater

`dirextalk-updater` is the root host recovery boundary for a containerized message server. Keep it independent so discovery, upgrade progress, restart, rollback, desired state, and watchdog recovery remain available while the app is down.

## Supported Contract

- Version 1 supports Ubuntu 22.04 and 24.04 on `linux/amd64`. Host-mutating commands fail closed elsewhere; read-only version/release inspection may run on development hosts.
- The frozen v1 control API stays on the root-owned Unix socket. Do not add TCP, arbitrary service names, Compose paths, commands, images, digests, or shell input.
- Read the README's frozen v1 API/runtime contract and focused tests before changing routes, request shapes, allowlists, job semantics, or watchdog behavior.
- Root configuration owns the fixed Compose/Caddy topology. API callers request declarative operations only.

## Safety Invariants

- Require root and root-owned regular non-symlink config/token files with exact `0600` permissions.
- Treat control and per-job bearers as secrets. Persist hashes/references only; keep tokens out of logs, errors, argv, fixtures, and reports.
- Persist state atomically and durably. Create a job and enter `upgrading` transactionally; allow one active job and reject external desired-state changes while it runs.
- Same-key idempotent replay recovers the committed job. A new key cannot reuse an expired plan.
- Release discovery, compatibility, exact image identities, retained-data evidence, backup/restore, and recovery fail closed. Legacy bootstrap behavior is limited to its explicit tested migration path.
- The watchdog operates only fixed code-owned services and already configured immutable images. It does not discover releases, pull `latest`, migrate, or expand its repair scope.

## Change And Verification

1. Reproduce the contract/state transition and add a focused failing test when behavior changes.
2. Make the smallest change in the owning package; preserve the frozen API unless an explicit versioned contract change is approved.
3. Run `go test ./...`, `go test -race ./...`, `go vet ./...`, `go mod verify`, and `git diff --check` for behavior changes.
4. Build `CGO_ENABLED=0 GOOS=linux GOARCH=amd64` and use the applicable Ubuntu CI/host integration for platform-mutating changes.
5. Review and commit only current-task changes.

Stable publication uses canonical `vX.Y.Z` tags and the repository release workflow. Pushing tags, creating Releases, uploading assets, or changing external state requires explicit authorization; local preparation and verification do not grant it.
