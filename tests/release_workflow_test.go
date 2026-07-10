package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCIAndReleaseWorkflowsStayUbuntu2404AMD64Only(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	root := filepath.Dir(filepath.Dir(filename))
	ci := readWorkflow(t, filepath.Join(root, ".github", "workflows", "ci.yml"))
	for _, required := range []string{"ubuntu-24.04", "go test ./...", "go test -race ./...", "go vet ./...", "GOOS=linux GOARCH=amd64", "dirextalk-updater-linux-amd64", "version"} {
		if !strings.Contains(ci, required) {
			t.Fatalf("CI workflow is missing %q", required)
		}
	}

	release := readWorkflow(t, filepath.Join(root, ".github", "workflows", "release.yml"))
	for _, required := range []string{"ubuntu-24.04", "tags:", "- 'v*.*.*'", "[[ \"$VERSION\" =~ ^v(0|[1-9][0-9]*)", "GOOS=linux GOARCH=amd64", "dirextalk-updater-linux-amd64", "dirextalk-updater-linux-amd64.sha256", "dirextalk-updater-release.json", "gh release create"} {
		if !strings.Contains(release, required) {
			t.Fatalf("release workflow is missing %q", required)
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
