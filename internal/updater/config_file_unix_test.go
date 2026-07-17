//go:build !windows

package updater

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validConfigFileJSON = `{"schema_version":1,"state_dir":"/var/lib/dirextalk-updater","socket_path":"/run/dirextalk-updater/http.sock","control_token_file":"/etc/dirextalk-updater/control-token","caddy_mode":"systemd"}`

func TestLoadConfigFileRequiresRootOwnedPrivateRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(validConfigFileJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfigFile(path); err == nil {
		t.Fatal("public updater config was accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfigFile(path)
	if os.Geteuid() != 0 {
		if err == nil || !strings.Contains(err.Error(), "run as root") {
			t.Fatalf("non-root config load did not fail closed: config=%#v err=%v", config, err)
		}
		return
	}
	if err != nil || config.CaddyMode != CaddyModeSystemd {
		t.Fatalf("root-owned private config rejected: config=%#v err=%v", config, err)
	}
}

func TestLoadConfigFileRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.json")
	link := filepath.Join(directory, "config.json")
	if err := os.WriteFile(target, []byte(validConfigFileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfigFile(link); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("config symlink was not rejected: %v", err)
	}
}
