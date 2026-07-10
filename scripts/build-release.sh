#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_GO_VERSION="go1.24.13"
readonly BINARY_NAME="dirextalk-updater-linux-amd64"
readonly OUTPUT_DIR="dist"

[[ "$(uname -s)" == "Linux" ]]
[[ "$(uname -m)" == "x86_64" ]]
grep -qE '^ID="?ubuntu"?$' /etc/os-release
grep -qE '^VERSION_ID="?24\.04"?$' /etc/os-release

: "${VERSION:?VERSION is required}"
: "${COMMIT:?COMMIT is required}"
: "${BUILD_TIME:?BUILD_TIME is required}"

[[ "$VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
[[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]]
[[ "$BUILD_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]
[[ "$(GOTOOLCHAIN="$EXPECTED_GO_VERSION" go env GOVERSION)" == "$EXPECTED_GO_VERSION" ]]
[[ "$(git rev-parse HEAD)" == "$COMMIT" ]]
[[ -z "$(git status --porcelain --untracked-files=normal)" ]]

commit_epoch="$(git show -s --format=%ct "$COMMIT")"
expected_build_time="$(date -u -d "@$commit_epoch" +'%Y-%m-%dT%H:%M:%SZ')"
[[ "$BUILD_TIME" == "$expected_build_time" ]]

temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
mkdir -p "$temporary/cache-a" "$temporary/cache-b" "$OUTPUT_DIR"

readonly LDFLAGS="-s -w -buildid= -X github.com/YingSuiAI/dirextalk-updater/internal/buildinfo.Version=$VERSION -X github.com/YingSuiAI/dirextalk-updater/internal/buildinfo.Commit=$COMMIT -X github.com/YingSuiAI/dirextalk-updater/internal/buildinfo.BuildTime=$BUILD_TIME"

GOFLAGS= GOTOOLCHAIN="$EXPECTED_GO_VERSION" GOCACHE="$temporary/cache-a" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -mod=readonly -buildvcs=false -trimpath -ldflags "$LDFLAGS" -o "$temporary/build-a" ./cmd/dirextalk-updater
GOFLAGS= GOTOOLCHAIN="$EXPECTED_GO_VERSION" GOCACHE="$temporary/cache-b" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -mod=readonly -buildvcs=false -trimpath -ldflags "$LDFLAGS" -o "$temporary/build-b" ./cmd/dirextalk-updater

cmp --silent "$temporary/build-a" "$temporary/build-b"
install -m 0755 "$temporary/build-a" "$OUTPUT_DIR/$BINARY_NAME"
GOFLAGS= GOTOOLCHAIN="$EXPECTED_GO_VERSION" go run -mod=readonly -trimpath -ldflags "$LDFLAGS" ./cmd/release-artifacts \
  -binary "$OUTPUT_DIR/$BINARY_NAME" -output "$OUTPUT_DIR"

mapfile -t release_files < <(find "$OUTPUT_DIR" -maxdepth 1 -type f -printf '%f\n' | sort)
[[ "${release_files[*]}" == "dirextalk-updater-linux-amd64 dirextalk-updater-linux-amd64.sha256 dirextalk-updater-release.json" ]]
