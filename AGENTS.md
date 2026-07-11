# AGENTS.md

`dirextalk-updater` is the host-resident recovery boundary for a self-hosted
Dirextalk message server. Keep it independent from the message-server
container so upgrade progress, restart, and rollback remain available while
the application is down.

## Supported Platform

- Version 1 supports Ubuntu 22.04 and 24.04 on `linux/amd64` only.
- Do not add architecture matrices, cross-platform service managers, or
  compatibility claims without an explicit product decision and real-host
  verification.
- `serve` and host-mutating trigger commands must fail closed outside the
  supported platform. Read-only `version` and release metadata tooling may run
  elsewhere for development and inspection.

## Safety Contract

- Keep the control API on its Unix socket. Do not add a TCP listener.
- Caddy topology is selected only by the root-owned fixed enum `compose` or
  `systemd`; systemd mode may operate only `caddy.service`. Never accept a
  service name or Caddy mode through the control API.
- The daemon is a root host-operation boundary. Require EUID 0 and a
  root-owned control-token regular file with mode exactly `0600`.
- Treat the control token and per-job bearer tokens as secrets.
  Persist hashes only and never print raw tokens in logs or errors.
- Keep request bodies declarative. Callers must not provide shell commands,
  Compose paths, service names, image repositories, or digests.
- Preserve the fixed Compose project, file, image repository, and service
  allowlist in code-owned configuration. `compose_project` is limited to the
  code-owned `dirextalk-p2p` and `dirextalk-message-server` migration layouts;
  it is never accepted from the control API.
- Persist state atomically and durably. A new job and the transition to
  `upgrading` are one transaction; only one active job is allowed. External
  desired-state calls cannot select `upgrading` or overwrite any state while a
  job is active.
- Same-key idempotent replay must recover an already committed job even after
  its plan expires. A new key must never reuse an expired plan.
- A fresh checksum-bound `release-index.json` from the latest formal server
  Release is required before
  issuing a plan. Freshness expires after the frozen 36-hour window; future or
  missing check times fail closed. Client, schema, and upgrade-edge
  compatibility also fail closed. Every edge binds the exact source image
  digest, and an indirect upgrade must have one unambiguous ordered path. Plan registration atomically rechecks
  discovery freshness and host eligibility before returning an operation.
- Keep the legacy minimal-health exception limited to the trusted
  `v0.15.2 -> v1.0.0` bootstrap source and its explicitly marked recovery
  point, after canonical image and exact edge digest verification. Never apply
  it to watchdog, formal targets, or formal restores.

## Change Workflow

1. Write a failing contract test before behavior changes.
2. Make the smallest change in the owning package.
3. Run `go test ./...`, `go test -race ./...`, `go vet ./...`, and
   `go mod verify`.
4. Build `GOOS=linux GOARCH=amd64` and smoke-test its `version` command on an
   Ubuntu 24.04 runner; run host integration on Ubuntu 22.04 or 24.04 as
   appropriate for the changed path.
5. Run `git diff --check`, inspect all changes, and commit a focused change.

## Release Contract

- Stable tags are canonical `vX.Y.Z` tags.
- Formal binaries use exactly Go 1.24.13 and must inject version, full commit,
  and the tagged commit's RFC3339 UTC timestamp through linker flags; local
  builds remain `v0.0.0-dev+local`.
- The release builder uses two independent Go caches and requires byte-for-byte
  identical binaries before creating assets.
- Publish exactly `dirextalk-updater-linux-amd64`, its `.sha256` file, and
  `dirextalk-updater-release.json` from the tag workflow.
- Do not create a repository, remote, tag, GitHub Release, or upload assets
  without explicit authorization.
