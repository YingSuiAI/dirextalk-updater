package updater

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadControlTokenRejectsEmptyAndOversizedFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-token")
	if err := os.WriteFile(path, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadControlToken(path); err == nil {
		t.Fatal("expected empty control token rejection")
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 4097)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadControlToken(path); err == nil {
		t.Fatal("expected oversized control token rejection")
	}
}

func TestLoadControlTokenReturnsTrimmedToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-token")
	want := strings.Repeat("a", 32)
	if err := os.WriteFile(path, []byte(want+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadControlToken(path)
	if runtime.GOOS == "windows" {
		if err == nil {
			t.Fatal("Windows control-token loading must fail closed without ACL verification")
		}
		return
	}
	if err == nil && got != want {
		t.Fatal("successfully loaded control token was not trimmed")
	}
}

func TestValidateRootControlTokenMetadata(t *testing.T) {
	if err := validateRootControlTokenMetadata(0o600, 0, 0); err != nil {
		t.Fatalf("root-owned 0600 token rejected: %v", err)
	}
	for _, test := range []struct {
		name         string
		mode         os.FileMode
		ownerUID     uint32
		effectiveUID uint32
		want         string
	}{
		{"non-root process", 0o600, 0, 1000, "run as root"},
		{"non-root owner", 0o600, 1000, 0, "owned by root"},
		{"group readable", 0o640, 0, 0, "0600"},
		{"owner executable", 0o700, 0, 0, "0600"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateRootControlTokenMetadata(test.mode, test.ownerUID, test.effectiveUID)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %v", test.want, err)
			}
		})
	}
}
