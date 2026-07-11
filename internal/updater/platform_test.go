package updater

import (
	"strings"
	"testing"
)

func TestValidateHostPlatformAcceptsUbuntu2204And2404AMD64(t *testing.T) {
	ubuntu24 := []byte("ID=ubuntu\nVERSION_ID=\"24.04\"\n")
	for _, version := range []string{"22.04", "24.04"} {
		ubuntu := []byte("ID=ubuntu\nVERSION_ID=\"" + version + "\"\n")
		if err := ValidateHostPlatform("linux", "amd64", ubuntu); err != nil {
			t.Fatalf("Ubuntu %s amd64 rejected: %v", version, err)
		}
	}

	tests := []struct {
		name      string
		goos      string
		goarch    string
		osRelease []byte
		want      string
	}{
		{"windows", "windows", "amd64", ubuntu24, "linux/amd64"},
		{"arm64", "linux", "arm64", ubuntu24, "linux/amd64"},
		{"Ubuntu 20", "linux", "amd64", []byte("ID=ubuntu\nVERSION_ID=20.04\n"), "Ubuntu 22.04 or 24.04"},
		{"Debian", "linux", "amd64", []byte("ID=debian\nVERSION_ID=\"12\"\n"), "Ubuntu 22.04 or 24.04"},
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
