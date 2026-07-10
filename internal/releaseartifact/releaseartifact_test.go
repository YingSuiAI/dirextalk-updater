package releaseartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateWritesSingleUbuntuAMD64ReleaseContract(t *testing.T) {
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, BinaryAssetName)
	binary := []byte("linux-amd64-binary")
	if err := os.WriteFile(binaryPath, binary, 0o755); err != nil {
		t.Fatal(err)
	}
	identity := Identity{
		Version:   "v1.0.0",
		Commit:    strings.Repeat("a", 40),
		BuildTime: "2026-07-10T08:09:10Z",
	}
	if err := Generate(dir, binaryPath, identity); err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256(binary)
	wantHex := hex.EncodeToString(digest[:])
	checksum, err := os.ReadFile(filepath.Join(dir, ChecksumAssetName))
	if err != nil {
		t.Fatal(err)
	}
	if string(checksum) != wantHex+"  "+BinaryAssetName+"\n" {
		t.Fatalf("unexpected checksum: %q", checksum)
	}
	manifestData, err := os.ReadFile(filepath.Join(dir, ManifestAssetName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ManifestVersion != 1 || manifest.Version != identity.Version || manifest.Commit != identity.Commit || manifest.BuildTime != identity.BuildTime || manifest.OS != "linux" || manifest.Arch != "amd64" || manifest.UbuntuVersion != "24.04" || manifest.Asset != BinaryAssetName || manifest.SHA256 != wantHex {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected binary plus two metadata assets, got %d", len(entries))
	}
}

func TestGenerateRejectsNonReleaseIdentityOrUnexpectedBinaryName(t *testing.T) {
	dir := t.TempDir()
	wrongPath := filepath.Join(dir, "dirextalk-updater-linux-arm64")
	if err := os.WriteFile(wrongPath, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	valid := Identity{Version: "v1.0.0", Commit: strings.Repeat("b", 40), BuildTime: "2026-07-10T08:09:10Z"}
	if err := Generate(dir, wrongPath, valid); err == nil {
		t.Fatal("unexpected asset name was accepted")
	}
	if err := Generate(dir, wrongPath, Identity{Version: "v0.0.0-dev+local", Commit: "uncommitted"}); err == nil {
		t.Fatal("development identity was accepted")
	}
}
