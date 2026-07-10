package updater

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
)

const osReleasePath = "/etc/os-release"

func CheckSupportedHost() error {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return ValidateHostPlatform(runtime.GOOS, runtime.GOARCH, nil)
	}
	data, err := os.ReadFile(osReleasePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", osReleasePath, err)
	}
	return ValidateHostPlatform(runtime.GOOS, runtime.GOARCH, data)
}

func ValidateHostPlatform(goos, goarch string, osRelease []byte) error {
	if goos != "linux" || goarch != "amd64" {
		return fmt.Errorf("unsupported host %s/%s: dirextalk-updater v1 supports only Ubuntu 24.04 linux/amd64", goos, goarch)
	}
	values := parseOSRelease(osRelease)
	if values["ID"] == "" || values["VERSION_ID"] == "" {
		return fmt.Errorf("could not identify Ubuntu 24.04 from %s", osReleasePath)
	}
	if values["ID"] != "ubuntu" || values["VERSION_ID"] != "24.04" {
		return fmt.Errorf("unsupported distribution %s %s: dirextalk-updater v1 supports only Ubuntu 24.04", values["ID"], values["VERSION_ID"])
	}
	return nil
}

func parseOSRelease(data []byte) map[string]string {
	values := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || (key != "ID" && key != "VERSION_ID") {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		values[key] = value
	}
	return values
}
