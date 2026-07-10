package releaseartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/YingSuiAI/dirextalk-updater/internal/buildinfo"
)

const (
	BinaryAssetName   = "dirextalk-updater-linux-amd64"
	ChecksumAssetName = BinaryAssetName + ".sha256"
	ManifestAssetName = "dirextalk-updater-release.json"
)

type Identity = buildinfo.Info

type Manifest struct {
	ManifestVersion int    `json:"manifest_version"`
	Version         string `json:"version"`
	Commit          string `json:"commit"`
	BuildTime       string `json:"build_time"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	UbuntuVersion   string `json:"ubuntu_version"`
	Asset           string `json:"asset"`
	SHA256          string `json:"sha256"`
}

func Generate(outputDir, binaryPath string, identity Identity) error {
	if err := identity.ValidateRelease(); err != nil {
		return fmt.Errorf("validate release identity: %w", err)
	}
	if filepath.Base(binaryPath) != BinaryAssetName {
		return fmt.Errorf("binary asset must be named %s", BinaryAssetName)
	}
	binary, err := os.ReadFile(binaryPath)
	if err != nil {
		return fmt.Errorf("read release binary: %w", err)
	}
	if len(binary) == 0 {
		return fmt.Errorf("release binary is empty")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create release directory: %w", err)
	}
	digest := sha256.Sum256(binary)
	digestHex := hex.EncodeToString(digest[:])
	checksum := []byte(digestHex + "  " + BinaryAssetName + "\n")
	if err := os.WriteFile(filepath.Join(outputDir, ChecksumAssetName), checksum, 0o644); err != nil {
		return fmt.Errorf("write release checksum: %w", err)
	}
	manifest := Manifest{
		ManifestVersion: 1,
		Version:         identity.Version,
		Commit:          identity.Commit,
		BuildTime:       identity.BuildTime,
		OS:              "linux",
		Arch:            "amd64",
		UbuntuVersion:   "24.04",
		Asset:           BinaryAssetName,
		SHA256:          digestHex,
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode release manifest: %w", err)
	}
	manifestData = append(manifestData, '\n')
	if err := os.WriteFile(filepath.Join(outputDir, ManifestAssetName), manifestData, 0o644); err != nil {
		return fmt.Errorf("write release manifest: %w", err)
	}
	return nil
}
