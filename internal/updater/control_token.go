package updater

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const maxControlTokenBytes = 4096

func validateRootControlTokenMetadata(mode os.FileMode, ownerUID, effectiveUID uint32) error {
	if effectiveUID != 0 {
		return fmt.Errorf("dirextalk-updater must run as root to load the control token")
	}
	if ownerUID != 0 {
		return fmt.Errorf("control token file must be owned by root")
	}
	if mode.Perm() != 0o600 {
		return fmt.Errorf("control token file permissions must be exactly 0600")
	}
	return nil
}

func LoadControlToken(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open control token: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("read control token metadata: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("control token must be a regular file")
	}
	if info.Size() > maxControlTokenBytes {
		return "", fmt.Errorf("control token file is too large")
	}
	if err := validateControlTokenFile(info); err != nil {
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxControlTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("read control token: %w", err)
	}
	if len(data) > maxControlTokenBytes {
		return "", fmt.Errorf("control token file is too large")
	}
	token := strings.TrimSpace(string(data))
	for index := range data {
		data[index] = 0
	}
	if len(token) < 32 {
		return "", fmt.Errorf("control token is missing or too short")
	}
	return token, nil
}
