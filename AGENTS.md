# AGENTS.md

`dirextalk-updater` is the host-resident recovery boundary for a self-hosted
Dirextalk message server. Keep it independent from the message-server
container so upgrade progress, restart, and rollback remain available while
the application is down.

## Supported Platform

- Version 1 supports Ubuntu 24.04 on `linux/amd64` only.
- Do not add architecture matrices, cross-platform service managers, or
  compatibility claims without an explicit product decision and real-host
  verification.
- `serve` and host-mutating trigger commands must fail closed outside the
  supported platform. Read-only `version` and release metadata tooling may run
  elsewhere for development and inspection.

## Safety Contract

- Keep the control API on its Unix socket. Do not add a TCP listener.
- Treat the root-owned control token and per-job bearer tokens as secrets.
  Persist hashes only and never print raw tokens in logs or errors.
- Keep request bodies declarative. Callers must not provide shell commands,
  Compose paths, service names, image repositories, or digests.
- Preserve the fixed Compose project, file, image repository, and service
  allowlist in code-owned configuration.
- Persist state atomically and durably. A new job and the transition to
  `upgrading` are one transaction; only one active job is allowed.
- Same-key idempotent replay must recover an already committed job even after
  its plan expires. A new key must never reuse an expired plan.
- A fresh discovered and validated server Release manifest is required before
  issuing a plan. Client, schema, and upgrade-edge compatibility fail closed.

## Change Workflow

1. Write a failing contract test before behavior changes.
2. Make the smallest change in the owning package.
3. Run `go test ./...`, `go test -race ./...`, `go vet ./...`, and
   `go mod verify`.
4. Build `GOOS=linux GOARCH=amd64` and smoke-test its `version` command on an
   Ubuntu 24.04 runner or host.
5. Run `git diff --check`, inspect all changes, and commit a focused change.

## Release Contract

- Stable tags are canonical `vX.Y.Z` tags.
- Formal binaries must inject version, full commit, and RFC3339 UTC build time
  through linker flags; local builds remain `v0.0.0-dev+local`.
- Publish exactly `dirextalk-updater-linux-amd64`, its `.sha256` file, and
  `dirextalk-updater-release.json` from the tag workflow.
- Do not create a repository, remote, tag, GitHub Release, or upload assets
  without explicit authorization.
