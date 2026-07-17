package updater

import (
	"context"
	"fmt"
)

// DirectRelease is the immutable target recorded for a centrally selected
// single-hop upgrade. The repository is intentionally not persisted or
// caller-controlled: it is always AllowedImageRepository.
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

// DirectJobRuntime is deliberately small so the Unix control plane can resolve
// only a centrally supplied version into the fixed image repository. It never
// accepts an image reference, digest, command, or path from its caller.
type DirectJobRuntime interface {
	CurrentVersion(context.Context) (string, error)
	ResolveDirectRelease(context.Context, string) (DirectRelease, error)
}
