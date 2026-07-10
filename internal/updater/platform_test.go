package updater

import (
	"strings"
	"testing"
)

func TestValidateHostPlatformAcceptsOnlyUbuntu2404AMD64(t *testing.T) {
	ubuntu := []byte("ID=ubuntu\nVERSION_ID=\"24.04\"\n")
	if err := ValidateHostPlatform("linux", "amd64", ubuntu); err != nil {
		t.Fatalf("Ubuntu 24.04 amd64 rejected: %v", err)
	}

	tests := []struct {
		name      string
		goos      string
		goarch    string
		osRelease []byte
		want      string
	}{
		{"windows", "windows", "amd64", ubuntu, "linux/amd64"},
		{"arm64", "linux", "arm64", ubuntu, "linux/amd64"},
		{"Ubuntu 22", "linux", "amd64", []byte("ID=ubuntu\nVERSION_ID=22.04\n"), "Ubuntu 24.04"},
		{"Debian", "linux", "amd64", []byte("ID=debian\nVERSION_ID=\"12\"\n"), "Ubuntu 24.04"},
		{"malformed", "linux", "amd64", []byte("NAME=Ubuntu\n"), "identify"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateHostPlatform(test.goos, test.goarch, test.osRelease)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
}
