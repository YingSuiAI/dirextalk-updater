package updater

import (
	"fmt"
)

const DirectContractVersion = 2

// DirectSource is retained for validation and recovery of persisted legacy
// release-index plans. New direct jobs do not create or consume it.
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
