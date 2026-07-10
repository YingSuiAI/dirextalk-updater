package updater

import (
	"fmt"
	"os"
)

const maxConfigFileBytes = 64 * 1024

func validateRootConfigMetadata(mode os.FileMode, ownerUID, effectiveUID uint32) error {
	if effectiveUID != 0 {
		return fmt.Errorf("dirextalk-updater must run as root to load the config")
	}
	if ownerUID != 0 {
		return fmt.Errorf("updater config file must be owned by root")
	}
	if mode.Perm() != 0o600 {
		return fmt.Errorf("updater config file permissions must be exactly 0600")
	}
	return nil
}

func LoadConfigFile(configPath string) (Config, error) {
	pathInfo, err := os.Lstat(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("inspect updater config path: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 {
		return Config{}, fmt.Errorf("updater config path must not be a symlink")
	}
	if !pathInfo.Mode().IsRegular() {
		return Config{}, fmt.Errorf("updater config must be a regular file")
	}
	file, err := os.Open(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("open updater config: %w", err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return Config{}, fmt.Errorf("read updater config metadata: %w", err)
	}
	if !fileInfo.Mode().IsRegular() || !os.SameFile(pathInfo, fileInfo) {
		return Config{}, fmt.Errorf("updater config changed while opening")
	}
	if fileInfo.Size() > maxConfigFileBytes {
		return Config{}, fmt.Errorf("updater config file is too large")
	}
	if err := validateConfigFile(fileInfo); err != nil {
		return Config{}, err
	}
	config, err := LoadConfig(file)
	if err != nil {
		return Config{}, err
	}
	return config, nil
}
