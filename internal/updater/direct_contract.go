package updater

import (
	"context"
	"fmt"
	"strings"
)

const DirectContractVersion = 2

// ReleaseSource returns the checksum-verified canonical index from the latest
// published stable message-server release.
type ReleaseSource interface {
	Latest(context.Context) ([]byte, error)
}

// DirectSource contains only host-observed facts. It is compared with the
// trusted release edge before a job and its bound plan are committed.
type DirectSource struct {
	Version             string
	ImageDigest         string
	SchemaVersion       int
	SchemaCompatVersion int
}

func (source DirectSource) Validate() error {
	if _, err := parseCanonicalVersion("current_version", source.Version); err != nil {
		return err
	}
	if !digestPattern.MatchString(source.ImageDigest) {
		return fmt.Errorf("current image digest is invalid")
	}
	if source.SchemaVersion < 1 || source.SchemaCompatVersion < 1 || source.SchemaCompatVersion > source.SchemaVersion {
		return fmt.Errorf("current schema versions are invalid")
	}
	return nil
}

func normalizeRequiredClientVersion(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("client_version is required")
	}
	if !strings.HasPrefix(value, "v") {
		value = "v" + value
	}
	if _, err := parseCanonicalVersion("client_version", value); err != nil {
		return "", err
	}
	return value, nil
}

func validateClientCompatibility(clientVersion string, manifest Manifest) error {
	client, err := parseCanonicalVersion("client_version", clientVersion)
	if err != nil {
		return err
	}
	minimum, err := parseCanonicalVersion("minimum_client_version", manifest.MinimumClientVersion)
	if err != nil {
		return err
	}
	maximum, err := parseCanonicalVersion("maximum_client_version_exclusive", manifest.MaximumClientVersionExclusive)
	if err != nil {
		return err
	}
	if client.LessThan(minimum) || !client.LessThan(maximum) {
		return fmt.Errorf("client version is outside the target release compatibility range")
	}
	return nil
}

func validateSchemaCompatibility(source DirectSource, manifest Manifest) error {
	if err := source.Validate(); err != nil {
		return err
	}
	if source.SchemaVersion < manifest.SchemaCompatVersion || source.SchemaCompatVersion > manifest.SchemaVersion {
		return fmt.Errorf("current schema is incompatible with the target release")
	}
	return nil
}
