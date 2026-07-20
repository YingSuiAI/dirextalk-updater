package updater

import (
	"context"
	"fmt"
)

// DirectRelease is the centrally authorized target stored with a direct job.
// ImageDigest is retained only to decode and finish older persisted jobs.
type DirectRelease struct {
	Version     string `json:"version"`
	ImageDigest string `json:"image_digest,omitempty"`
}

func (release DirectRelease) Validate() error {
	if _, err := parseCanonicalVersion("target_version", release.Version); err != nil {
		return err
	}
	if release.ImageDigest != "" && !digestPattern.MatchString(release.ImageDigest) {
		return fmt.Errorf("direct target image_digest is invalid")
	}
	return nil
}

func (release DirectRelease) ImageRef() string {
	if release.ImageDigest == "" {
		return taggedImageRef(release.Version)
	}
	return pinnedImageRef(release.Version, release.ImageDigest)
}

// DirectJobRuntime reads the host-owned current version. Central authorization
// supplies only a canonical target version; callers cannot select a repository,
// image digest, command, or path.
type DirectJobRuntime interface {
	CurrentVersion(context.Context) (string, error)
}
