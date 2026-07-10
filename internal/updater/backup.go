package updater

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	BackupMetadataSchemaVersion = 1
	committedBackupName         = "current"
	retiringBackupName          = ".retiring"
	backupMetadataName          = "metadata.json"
)

var (
	backupJobIDPattern      = regexp.MustCompile(`^job_[A-Za-z0-9_-]{1,128}$`)
	hexDigestPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	requiredBackupArtifacts = [...]string{
		"message-config.tar",
		"message-data.tar",
		"p2p.tar",
		"postgres.dump",
	}
)

type BackupArtifact struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type BackupMetadata struct {
	SchemaVersion       int              `json:"schema_version"`
	JobID               string           `json:"job_id"`
	Version             string           `json:"version"`
	ImageDigest         string           `json:"image_digest"`
	ImageRef            string           `json:"image_ref"`
	DatabaseSchema      int              `json:"database_schema"`
	SchemaCompatVersion int              `json:"schema_compat_version"`
	CreatedAt           time.Time        `json:"created_at"`
	Artifacts           []BackupArtifact `json:"artifacts"`
}

type BackupStore struct {
	root          string
	syncDirectory func(string) error
}

func NewBackupStore(root string) *BackupStore {
	return &BackupStore{root: root, syncDirectory: syncDirectory}
}

func (store *BackupStore) Begin(ctx context.Context, jobID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !backupJobIDPattern.MatchString(jobID) {
		return "", fmt.Errorf("backup job id is invalid")
	}
	if err := os.MkdirAll(store.root, 0o700); err != nil {
		return "", fmt.Errorf("create backup root: %w", err)
	}
	if err := os.Chmod(store.root, 0o700); err != nil {
		return "", fmt.Errorf("protect backup root: %w", err)
	}
	path, err := os.MkdirTemp(store.root, ".staging-"+jobID+"-")
	if err != nil {
		return "", fmt.Errorf("create staging backup: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		_ = os.RemoveAll(path)
		return "", fmt.Errorf("protect staging backup: %w", err)
	}
	return path, nil
}

func WriteBackupMetadata(staging string, metadata BackupMetadata) error {
	if err := validateBackupMetadataShape(metadata); err != nil {
		return err
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode backup metadata: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(staging, backupMetadataName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create backup metadata: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write backup metadata: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("fsync backup metadata: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close backup metadata: %w", err)
	}
	return nil
}

func (store *BackupStore) Commit(ctx context.Context, staging string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !directChild(staging, store.root, ".staging-") {
		return fmt.Errorf("staging backup is outside the backup root")
	}
	expected, err := validateBackupDirectory(staging)
	if err != nil {
		return errors.Join(err, store.removeStaging(staging))
	}
	if err := syncBackupFiles(staging); err != nil {
		return err
	}
	if err := store.syncDirectory(staging); err != nil {
		return fmt.Errorf("fsync staging backup directory: %w", err)
	}
	current := filepath.Join(store.root, committedBackupName)
	retiring := filepath.Join(store.root, retiringBackupName)
	if _, err := os.Stat(retiring); err == nil {
		return fmt.Errorf("backup rotation recovery is required")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect retiring backup: %w", err)
	}
	if _, err := os.Stat(current); err == nil {
		if err := os.Rename(current, retiring); err != nil {
			return fmt.Errorf("retire current backup: %w", err)
		}
		if err := store.syncDirectory(store.root); err != nil {
			return store.recoverCommitFailure(ctx, fmt.Errorf("fsync retired backup: %w", err), expected)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect current backup: %w", err)
	}
	if err := os.Rename(staging, current); err != nil {
		return store.recoverCommitFailure(ctx, fmt.Errorf("commit staging backup: %w", err), expected)
	}
	if err := store.syncDirectory(store.root); err != nil {
		return store.recoverCommitFailure(ctx, fmt.Errorf("fsync committed backup: %w", err), expected)
	}
	if err := os.RemoveAll(retiring); err != nil {
		return store.recoverCommitFailure(ctx, fmt.Errorf("remove retired backup: %w", err), expected)
	}
	if err := store.syncDirectory(store.root); err != nil {
		return store.recoverCommitFailure(ctx, fmt.Errorf("fsync backup cleanup: %w", err), expected)
	}
	return nil
}

func (store *BackupStore) Recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current := filepath.Join(store.root, committedBackupName)
	retiring := filepath.Join(store.root, retiringBackupName)
	_, currentErr := os.Stat(current)
	_, retiringErr := os.Stat(retiring)
	switch {
	case currentErr == nil && retiringErr == nil:
		if err := os.RemoveAll(retiring); err != nil {
			return fmt.Errorf("remove interrupted retired backup: %w", err)
		}
	case errors.Is(currentErr, os.ErrNotExist) && retiringErr == nil:
		if err := os.Rename(retiring, current); err != nil {
			return fmt.Errorf("restore interrupted backup rotation: %w", err)
		}
	case currentErr != nil && !errors.Is(currentErr, os.ErrNotExist):
		return fmt.Errorf("inspect current backup: %w", currentErr)
	case retiringErr != nil && !errors.Is(retiringErr, os.ErrNotExist):
		return fmt.Errorf("inspect retiring backup: %w", retiringErr)
	}
	if err := os.MkdirAll(store.root, 0o700); err != nil {
		return fmt.Errorf("create backup root: %w", err)
	}
	entries, err := os.ReadDir(store.root)
	if err != nil {
		return fmt.Errorf("read backup root: %w", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".staging-") {
			if err := os.RemoveAll(filepath.Join(store.root, entry.Name())); err != nil {
				return fmt.Errorf("remove abandoned staging backup: %w", err)
			}
		}
	}
	return store.syncDirectory(store.root)
}

func (store *BackupStore) recoverCommitFailure(ctx context.Context, cause error, expected BackupMetadata) error {
	if err := store.Recover(ctx); err != nil {
		return errors.Join(cause, err)
	}
	current, err := store.Current(ctx)
	if err == nil && sameRecoveryPoint(current, expected) {
		// Recovery durably completed the intended rotation. Returning success
		// keeps runtime authority aligned with the physical single slot.
		return nil
	}
	return errors.Join(cause, err)
}

func (store *BackupStore) removeStaging(staging string) error {
	if err := os.RemoveAll(staging); err != nil {
		return fmt.Errorf("remove rejected staging backup: %w", err)
	}
	if err := store.syncDirectory(store.root); err != nil {
		return fmt.Errorf("fsync rejected staging cleanup: %w", err)
	}
	return nil
}

func (store *BackupStore) Discard(staging string) error {
	if !directChild(staging, store.root, ".staging-") {
		return fmt.Errorf("staging backup is outside the backup root")
	}
	return store.removeStaging(staging)
}

func (store *BackupStore) Current(ctx context.Context) (BackupMetadata, error) {
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	return validateBackupDirectory(filepath.Join(store.root, committedBackupName))
}

func validateBackupDirectory(directory string) (BackupMetadata, error) {
	metadataFile, err := os.Open(filepath.Join(directory, backupMetadataName))
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("open backup metadata: %w", err)
	}
	defer metadataFile.Close()
	var metadata BackupMetadata
	decoder := json.NewDecoder(io.LimitReader(metadataFile, 256*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return BackupMetadata{}, fmt.Errorf("decode backup metadata: %w", err)
	}
	if err := ensureJSONEOF(decoder, "backup metadata"); err != nil {
		return BackupMetadata{}, err
	}
	if err := validateBackupMetadataShape(metadata); err != nil {
		return BackupMetadata{}, err
	}
	seen := make(map[string]struct{}, len(metadata.Artifacts))
	for _, artifact := range metadata.Artifacts {
		if _, duplicate := seen[artifact.Name]; duplicate {
			return BackupMetadata{}, fmt.Errorf("backup artifact %s is duplicated", artifact.Name)
		}
		seen[artifact.Name] = struct{}{}
		path := filepath.Join(directory, artifact.Name)
		info, err := os.Lstat(path)
		if err != nil {
			return BackupMetadata{}, fmt.Errorf("inspect backup artifact %s: %w", artifact.Name, err)
		}
		unsafePermissions := runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0
		if !info.Mode().IsRegular() || unsafePermissions || info.Size() != artifact.Size {
			return BackupMetadata{}, fmt.Errorf("backup artifact %s has unsafe metadata", artifact.Name)
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return BackupMetadata{}, err
		}
		if digest != artifact.SHA256 {
			return BackupMetadata{}, fmt.Errorf("backup artifact %s checksum mismatch", artifact.Name)
		}
		if strings.HasSuffix(artifact.Name, ".tar") {
			if err := validateSafeTar(path); err != nil {
				return BackupMetadata{}, fmt.Errorf("backup artifact %s is unsafe: %w", artifact.Name, err)
			}
		}
	}
	for _, required := range requiredBackupArtifacts {
		if _, ok := seen[required]; !ok {
			return BackupMetadata{}, fmt.Errorf("required backup artifact %s is missing", required)
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("read backup directory: %w", err)
	}
	if len(entries) != len(metadata.Artifacts)+1 {
		return BackupMetadata{}, fmt.Errorf("backup directory contains untracked files")
	}
	return metadata, nil
}

func validateBackupMetadataShape(metadata BackupMetadata) error {
	if metadata.SchemaVersion != BackupMetadataSchemaVersion {
		return fmt.Errorf("backup schema_version %d is not supported", metadata.SchemaVersion)
	}
	if !backupJobIDPattern.MatchString(metadata.JobID) {
		return fmt.Errorf("backup job id is invalid")
	}
	if _, err := parseCanonicalVersion("backup version", metadata.Version); err != nil {
		return err
	}
	if !digestPattern.MatchString(metadata.ImageDigest) {
		return fmt.Errorf("backup image_digest is invalid")
	}
	expectedRef := AllowedImageRepository + ":" + metadata.Version + "@" + metadata.ImageDigest
	if metadata.ImageRef != expectedRef {
		return fmt.Errorf("backup image_ref must be %s", expectedRef)
	}
	if metadata.DatabaseSchema < 1 || metadata.SchemaCompatVersion < 1 || metadata.SchemaCompatVersion > metadata.DatabaseSchema {
		return fmt.Errorf("backup schema compatibility is invalid")
	}
	if metadata.CreatedAt.IsZero() || !metadata.CreatedAt.Equal(metadata.CreatedAt.UTC()) {
		return fmt.Errorf("backup created_at must be UTC")
	}
	names := make([]string, 0, len(metadata.Artifacts))
	for _, artifact := range metadata.Artifacts {
		if artifact.Name == "" || filepath.Base(artifact.Name) != artifact.Name || artifact.Name == backupMetadataName || artifact.Size < 0 || !hexDigestPattern.MatchString(artifact.SHA256) {
			return fmt.Errorf("backup artifact metadata is invalid")
		}
		names = append(names, artifact.Name)
	}
	if !sort.StringsAreSorted(names) {
		return fmt.Errorf("backup artifacts must be sorted by name")
	}
	return nil
}

func directChild(value, root, prefix string) bool {
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	cleanValue, err := filepath.Abs(value)
	if err != nil {
		return false
	}
	return filepath.Dir(cleanValue) == cleanRoot && strings.HasPrefix(filepath.Base(cleanValue), prefix)
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open backup artifact: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash backup artifact: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func syncBackupFiles(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read staging backup: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return fmt.Errorf("staging backup contains a non-regular file")
		}
		file, err := os.OpenFile(filepath.Join(directory, entry.Name()), os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open staging backup file: %w", err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return fmt.Errorf("fsync staging backup file: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close staging backup file: %w", closeErr)
		}
	}
	return nil
}

func validateSafeTar(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := tar.NewReader(file)
	entries := 0
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("archive path is not relative")
		}
		switch header.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeDir:
		default:
			return fmt.Errorf("archive entry type is not allowed")
		}
		if header.Size < 0 {
			return fmt.Errorf("archive entry size is invalid")
		}
		entries++
	}
	if entries == 0 {
		return fmt.Errorf("archive is empty")
	}
	return nil
}
