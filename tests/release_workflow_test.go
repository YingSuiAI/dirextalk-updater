package tests

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestCIAndReleaseWorkflowsCoverSupportedUbuntuAMD64Hosts(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	root := filepath.Dir(filepath.Dir(filename))
	ci := readWorkflow(t, filepath.Join(root, ".github", "workflows", "ci.yml"))
	for _, required := range []string{"ubuntu-22.04", "ubuntu-24.04", "contents: read", "persist-credentials: false", "go-version: '1.24.13'", "go test ./...", "go test -race ./...", "go vet ./...", "GOOS=linux GOARCH=amd64", "dirextalk-updater-linux-amd64", "version"} {
		if !strings.Contains(ci, required) {
			t.Fatalf("CI workflow is missing %q", required)
		}
	}

	release := readWorkflow(t, filepath.Join(root, ".github", "workflows", "release.yml"))
	for _, required := range []string{"ubuntu-22.04", "ubuntu-24.04", "tags:", "- 'v*.*.*'", "build:", "compatibility:", "publish:", "needs: [build, compatibility]", "contents: read", "contents: write", "persist-credentials: false", "go-version: '1.24.13'", "git show -s --format=%ct", "scripts/build-release.sh", "actions/upload-artifact@", "actions/download-artifact@", "GH_REPO: ${{ github.repository }}", "dirextalk-updater-linux-amd64", "dirextalk-updater-linux-amd64.sha256", "dirextalk-updater-release.json", "gh release create"} {
		if !strings.Contains(release, required) {
			t.Fatalf("release workflow is missing %q", required)
		}
	}
	actionRef := regexp.MustCompile(`uses: actions/[a-z-]+@([0-9a-f]{40})([[:space:]]|$)`)
	for _, workflow := range []string{ci, release} {
		uses := strings.Count(workflow, "uses: actions/")
		if matches := len(actionRef.FindAllStringSubmatch(workflow, -1)); matches != uses {
			t.Fatalf("every official action must use a 40-character commit SHA: uses=%d matches=%d", uses, matches)
		}
	}
	for _, mutable := range []string{"actions/checkout@v", "actions/setup-go@v", "actions/upload-artifact@v", "actions/download-artifact@v"} {
		if strings.Contains(ci, mutable) || strings.Contains(release, mutable) {
			t.Fatalf("workflow contains mutable action ref %q", mutable)
		}
	}

	buildScript := readWorkflow(t, filepath.Join(root, "scripts", "build-release.sh"))
	for _, required := range []string{"go1.24.13", "uname -s", "uname -m", "/etc/os-release", "24\\.04", "GOTOOLCHAIN=\"$EXPECTED_GO_VERSION\"", "GOCACHE=\"$temporary/cache-a\"", "GOCACHE=\"$temporary/cache-b\"", "cmp --silent", "-buildvcs=false", "-trimpath", "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64"} {
		if !strings.Contains(buildScript, required) {
			t.Fatalf("reproducible build script is missing %q", required)
		}
	}
	for _, forbidden := range []string{"arm64", "matrix:"} {
		if strings.Contains(release, forbidden) {
			t.Fatalf("release workflow unexpectedly contains %q", forbidden)
		}
	}
}

func readWorkflow(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
