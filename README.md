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

Go 1.24 or newer is required.

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
  "socket_path": "/run/dirextalk-updater/updater.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token"
}
```

Start the daemon with `dirextalk-updater -config <path> serve`. State is stored
with restrictive permissions and atomic replacement. The control token file
must be root-owned and protected as enforced by the runtime.

The frozen v1 Unix API prefix is `/_dirextalk/updater/v1/`. It includes control
routes for release discovery, status, desired state, and job creation, plus a
bearer-scoped job status route. Compatibility and operation availability are
server decisions; clients must not infer an upgrade path locally.

## Release assets

A stable `vX.Y.Z` tag runs CI on `ubuntu-24.04`, builds one statically linked
`linux/amd64` binary, verifies its embedded identity, and publishes exactly:

- `dirextalk-updater-linux-amd64`
- `dirextalk-updater-linux-amd64.sha256`
- `dirextalk-updater-release.json`

The JSON manifest binds the version, source commit, build time, supported OS,
architecture, Ubuntu version, asset name, and SHA-256 digest. No `latest`
binary URL is treated as an immutable installation target; installers resolve
a formal tagged Release and verify the manifest and checksum.

See [README_zh.md](README_zh.md) for Chinese documentation.
