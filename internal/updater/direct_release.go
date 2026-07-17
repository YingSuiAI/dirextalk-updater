package updater

import (
	"context"
	"fmt"
)

// DirectRelease is retained only so an already persisted job created by the
// pre-contract-v2 development build can finish recovery. Contract v2 creates a
// release-index-bound Plan instead of this standalone target.
type DirectRelease struct {
	Version     string `json:"version"`
	ImageDigest string `json:"image_digest"`
}

func (release DirectRelease) Validate() error {
	if _, err := parseCanonicalVersion("target_version", release.Version); err != nil {
		return err
	}
	if !digestPattern.MatchString(release.ImageDigest) {
		return fmt.Errorf("direct target image_digest is invalid")
	}
	return nil
}

func (release DirectRelease) ImageRef() string {
	return pinnedImageRef(release.Version, release.ImageDigest)
}

// DirectJobRuntime reads and proves host-owned source facts. The caller never
// supplies an image reference, digest, schema, command, or path.
type DirectJobRuntime interface {
	CurrentVersion(context.Context) (string, error)
	InspectDirectSource(context.Context, string, ReleaseStep) (DirectSource, error)
}
