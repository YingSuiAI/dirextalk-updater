//go:build !windows

package updater

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadControlTokenRequiresPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-token")
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 32)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadControlToken(path); err == nil {
		t.Fatal("expected public control token file rejection")
	}
}

func TestLoadControlTokenRequiresRootRuntimeAndRootOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-token")
	want := strings.Repeat("r", 32)
	if err := os.WriteFile(path, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadControlToken(path)
	if os.Geteuid() != 0 {
		if err == nil || !strings.Contains(err.Error(), "run as root") {
			t.Fatalf("non-root runtime did not fail closed: token=%q err=%v", got, err)
		}
		return
	}
	if err != nil || got != want {
		t.Fatalf("root-owned token was not loaded by root: token=%q err=%v", got, err)
	}
}
