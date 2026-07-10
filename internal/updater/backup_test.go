package updater

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBackupStoreCommitRetainsExactlyOneValidatedRecoveryPoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store := NewBackupStore(root)

	first := stageCompleteBackup(t, store, "job_first", "v1.0.0")
	if err := store.Commit(context.Background(), first); err != nil {
		t.Fatalf("commit first backup: %v", err)
	}
	second := stageCompleteBackup(t, store, "job_second", "v1.1.0")
	if err := store.Commit(context.Background(), second); err != nil {
		t.Fatalf("commit second backup: %v", err)
	}

	metadata, err := store.Current(context.Background())
	if err != nil {
		t.Fatalf("read current backup: %v", err)
	}
	if metadata.JobID != "job_second" || metadata.Version != "v1.1.0" {
		t.Fatalf("unexpected current backup: %#v", metadata)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != committedBackupName {
		t.Fatalf("expected only one committed backup, got %#v", entryNames(entries))
	}
}

func TestBackupStoreRejectsCorruptStagingWithoutReplacingCurrent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store := NewBackupStore(root)
	good := stageCompleteBackup(t, store, "job_good", "v1.0.0")
	if err := store.Commit(context.Background(), good); err != nil {
		t.Fatal(err)
	}

	corrupt := stageCompleteBackup(t, store, "job_bad", "v1.1.0")
	if err := os.WriteFile(filepath.Join(corrupt, requiredBackupArtifacts[0]), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(context.Background(), corrupt); err == nil {
		t.Fatal("expected corrupt staging backup to be rejected")
	}
	metadata, err := store.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.JobID != "job_good" {
		t.Fatalf("corrupt staging replaced recovery point: %#v", metadata)
	}
	if _, err := os.Stat(corrupt); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt staging retained secrets or unexpected error: %v", err)
	}
}

func TestBackupMetadataRejectsLegacyAssumptionForFormalRelease(t *testing.T) {
	t.Parallel()
	store := NewBackupStore(t.TempDir())
	staging := stageCompleteBackup(t, store, "job_formal", "v1.0.0")
	if err := store.Commit(context.Background(), staging); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	metadata.LegacyBootstrapAssumption = true
	if err := validateBackupMetadataShape(metadata); err == nil {
		t.Fatal("formal backup accepted legacy bootstrap assumption")
	}
}

func TestBackupStoreRepairsRotationBeforeReturningFsyncFailure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		failCall  int
		wantJobID string
		wantError bool
	}{
		{name: "retired entry not durable", failCall: 2, wantJobID: "job_old", wantError: true},
		{name: "new current entry repaired", failCall: 3, wantJobID: "job_new"},
		{name: "cleanup entry repaired", failCall: 4, wantJobID: "job_new"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store := NewBackupStore(root)
			old := stageCompleteBackup(t, store, "job_old", "v1.0.0")
			if err := store.Commit(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			staging := stageCompleteBackup(t, store, "job_new", "v1.1.0")
			calls := 0
			store.syncDirectory = func(string) error {
				calls++
				if calls == test.failCall {
					return errors.New("simulated fsync failure")
				}
				return nil
			}
			commitErr := store.Commit(context.Background(), staging)
			if test.wantError && commitErr == nil {
				t.Fatal("expected unrecoverable injected fsync failure")
			}
			if !test.wantError && commitErr != nil {
				t.Fatalf("reconciled committed backup returned an error: %v", commitErr)
			}
			current, err := store.Current(context.Background())
			if err != nil {
				t.Fatalf("single recovery slot was not repaired: %v", err)
			}
			if current.JobID != test.wantJobID {
				t.Fatalf("repaired current=%s, want %s", current.JobID, test.wantJobID)
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if names := entryNames(entries); len(names) != 1 || names[0] != committedBackupName {
				t.Fatalf("rotation artifacts remain after repair: %#v", names)
			}
		})
	}
}

func TestBackupStoreRecoverRemovesAbandonedSecretStaging(t *testing.T) {
	t.Parallel()
	store := NewBackupStore(t.TempDir())
	staging, err := store.Begin(context.Background(), "job_abandoned")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "secret"), []byte("credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staging was not removed: %v", err)
	}
}

func TestBackupStoreRecoversInterruptedDirectoryRotation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store := NewBackupStore(root)
	staging := stageCompleteBackup(t, store, "job_current", "v1.0.0")
	if err := store.Commit(context.Background(), staging); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, committedBackupName), filepath.Join(root, retiringBackupName)); err != nil {
		t.Fatal(err)
	}

	if err := store.Recover(context.Background()); err != nil {
		t.Fatalf("recover interrupted rotation: %v", err)
	}
	metadata, err := store.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.JobID != "job_current" {
		t.Fatalf("wrong recovered backup: %#v", metadata)
	}
}

func TestBackupTarValidationRejectsTraversalAndLinks(t *testing.T) {
	t.Parallel()
	for _, header := range []*tar.Header{
		{Name: "../../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg},
		{Name: "link", Mode: 0o777, Typeflag: tar.TypeSymlink, Linkname: "/etc/shadow"},
	} {
		path := filepath.Join(t.TempDir(), "unsafe.tar")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		writer := tar.NewWriter(file)
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			_, _ = writer.Write([]byte("x"))
		}
		_ = writer.Close()
		_ = file.Close()
		if err := validateSafeTar(path); err == nil {
			t.Fatalf("expected unsafe tar header to be rejected: %#v", header)
		}
	}
}

func stageCompleteBackup(t *testing.T, store *BackupStore, jobID, version string) string {
	t.Helper()
	staging, err := store.Begin(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	metadata := BackupMetadata{
		SchemaVersion:       BackupMetadataSchemaVersion,
		JobID:               jobID,
		Version:             version,
		ImageDigest:         "sha256:" + strings.Repeat("0", 64),
		ImageRef:            AllowedImageRepository + ":" + version + "@sha256:" + strings.Repeat("0", 64),
		DatabaseSchema:      1,
		SchemaCompatVersion: 1,
		CreatedAt:           time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	for _, name := range requiredBackupArtifacts {
		content := []byte(jobID + ":" + name)
		artifactPath := filepath.Join(staging, name)
		if strings.HasSuffix(name, ".tar") {
			writeTestTar(t, artifactPath, content)
			content, err = os.ReadFile(artifactPath)
			if err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(artifactPath, content, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(content)
		metadata.Artifacts = append(metadata.Artifacts, BackupArtifact{
			Name:   name,
			Size:   int64(len(content)),
			SHA256: hex.EncodeToString(digest[:]),
		})
	}
	if err := WriteBackupMetadata(staging, metadata); err != nil {
		t.Fatal(err)
	}
	return staging
}

func writeTestTar(t *testing.T, path string, content []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(file)
	if err := writer.WriteHeader(&tar.Header{Name: "data", Mode: 0o600, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
