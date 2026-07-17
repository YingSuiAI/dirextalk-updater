package updater

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const SupportedReleaseIndexVersion = 1

const (
	legacyInitialVersion     = "v0.15.2"
	legacyInitialImageDigest = "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
	firstFormalVersion       = "v1.0.0"
)

type IndexedRelease struct {
	Manifest       Manifest `json:"manifest"`
	ManifestDigest string   `json:"manifest_digest"`
}

type UpgradeEdge struct {
	FromVersion      string   `json:"from_version"`
	FromImageDigests []string `json:"from_image_digests"`
	ToVersion        string   `json:"to_version"`
}

type ReleaseIndex struct {
	ReleaseIndexVersion int              `json:"release_index_version"`
	LatestVersion       string           `json:"latest_version"`
	Releases            []IndexedRelease `json:"releases"`
	Edges               []UpgradeEdge    `json:"upgrade_edges"`
}

type ReleaseStep struct {
	Manifest           Manifest `json:"manifest"`
	ManifestDigest     string   `json:"manifest_digest"`
	SourceImageDigests []string `json:"source_image_digests"`
}

type rawIndexedRelease struct {
	Manifest       json.RawMessage `json:"manifest"`
	ManifestDigest string          `json:"manifest_digest"`
}

type rawReleaseIndex struct {
	ReleaseIndexVersion int                 `json:"release_index_version"`
	LatestVersion       string              `json:"latest_version"`
	Releases            []rawIndexedRelease `json:"releases"`
	Edges               []UpgradeEdge       `json:"upgrade_edges"`
}

func ValidateReleaseIndex(data []byte) (ReleaseIndex, error) {
	var raw rawReleaseIndex
	if err := decodeStrict(data, &raw, "release index"); err != nil {
		return ReleaseIndex{}, err
	}
	index := ReleaseIndex{
		ReleaseIndexVersion: raw.ReleaseIndexVersion,
		LatestVersion:       raw.LatestVersion,
		Edges:               raw.Edges,
		Releases:            make([]IndexedRelease, 0, len(raw.Releases)),
	}
	for releaseNumber, rawRelease := range raw.Releases {
		manifestData := bytes.TrimSpace(rawRelease.Manifest)
		if len(manifestData) == 0 {
			return ReleaseIndex{}, fmt.Errorf("releases[%d] manifest is required", releaseNumber)
		}
		manifest, err := ValidateManifest(manifestData)
		if err != nil {
			return ReleaseIndex{}, fmt.Errorf("releases[%d] manifest is invalid: %w", releaseNumber, err)
		}
		canonicalManifest, err := json.Marshal(manifest)
		if err != nil || !bytes.Equal(canonicalManifest, manifestData) {
			return ReleaseIndex{}, fmt.Errorf("releases[%d] manifest must use canonical compact encoding", releaseNumber)
		}
		digest := sha256.Sum256(manifestData)
		expectedDigest := "sha256:" + hex.EncodeToString(digest[:])
		if rawRelease.ManifestDigest != expectedDigest {
			return ReleaseIndex{}, fmt.Errorf("releases[%d] manifest_digest mismatch", releaseNumber)
		}
		index.Releases = append(index.Releases, IndexedRelease{Manifest: manifest, ManifestDigest: expectedDigest})
	}
	if err := index.Validate(); err != nil {
		return ReleaseIndex{}, err
	}
	canonicalIndex, err := json.Marshal(index)
	if err != nil || !bytes.Equal(canonicalIndex, data) {
		return ReleaseIndex{}, fmt.Errorf("release index must use canonical compact encoding")
	}
	return index, nil
}

func releaseIndexDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func canonicalManifestDigest(manifest Manifest) string {
	data, err := json.Marshal(manifest)
	if err != nil {
		return ""
	}
	return manifestDigest(data)
}

func canonicalReleaseIndexDigest(index ReleaseIndex) string {
	data, err := json.Marshal(index)
	if err != nil {
		return ""
	}
	return releaseIndexDigest(data)
}

func (index ReleaseIndex) Validate() error {
	if index.ReleaseIndexVersion != SupportedReleaseIndexVersion {
		return fmt.Errorf("release_index_version %d is not supported", index.ReleaseIndexVersion)
	}
	if _, err := parseCanonicalVersion("latest_version", index.LatestVersion); err != nil {
		return err
	}
	if len(index.Releases) == 0 {
		return fmt.Errorf("releases must not be empty")
	}
	byVersion := make(map[string]IndexedRelease, len(index.Releases))
	for releaseNumber, release := range index.Releases {
		if err := release.Manifest.Validate(); err != nil {
			return fmt.Errorf("releases[%d] manifest is invalid: %w", releaseNumber, err)
		}
		if !digestPattern.MatchString(release.ManifestDigest) {
			return fmt.Errorf("releases[%d] manifest_digest is invalid", releaseNumber)
		}
		if canonicalManifestDigest(release.Manifest) != release.ManifestDigest {
			return fmt.Errorf("releases[%d] manifest_digest mismatch", releaseNumber)
		}
		if _, exists := byVersion[release.Manifest.Version]; exists {
			return fmt.Errorf("release version %s is duplicated", release.Manifest.Version)
		}
		if releaseNumber > 0 {
			previous, _ := parseCanonicalVersion("version", index.Releases[releaseNumber-1].Manifest.Version)
			current, _ := parseCanonicalVersion("version", release.Manifest.Version)
			if !previous.LessThan(current) {
				return fmt.Errorf("releases must be strictly ordered by version")
			}
		}
		byVersion[release.Manifest.Version] = release
	}
	if index.Releases[len(index.Releases)-1].Manifest.Version != index.LatestVersion {
		return fmt.Errorf("latest_version must identify the last release")
	}
	seenEdges := make(map[string]struct{}, len(index.Edges))
	for edgeNumber, edge := range index.Edges {
		from, err := parseCanonicalVersion("from_version", edge.FromVersion)
		if err != nil {
			return fmt.Errorf("upgrade_edges[%d]: %w", edgeNumber, err)
		}
		to, err := parseCanonicalVersion("to_version", edge.ToVersion)
		if err != nil {
			return fmt.Errorf("upgrade_edges[%d]: %w", edgeNumber, err)
		}
		if !from.LessThan(to) {
			return fmt.Errorf("upgrade_edges[%d] is not an upgrade", edgeNumber)
		}
		target, ok := byVersion[edge.ToVersion]
		if !ok {
			return fmt.Errorf("upgrade_edges[%d] target is not indexed", edgeNumber)
		}
		if err := target.Manifest.ValidateUpgradeFrom(edge.FromVersion); err != nil {
			return fmt.Errorf("upgrade_edges[%d] violates target manifest: %w", edgeNumber, err)
		}
		if len(edge.FromImageDigests) == 0 || !sort.StringsAreSorted(edge.FromImageDigests) {
			return fmt.Errorf("upgrade_edges[%d] source digests must be non-empty and sorted", edgeNumber)
		}
		for digestNumber, digest := range edge.FromImageDigests {
			if !digestPattern.MatchString(digest) || (digestNumber > 0 && digest == edge.FromImageDigests[digestNumber-1]) {
				return fmt.Errorf("upgrade_edges[%d] source digest is invalid or duplicated", edgeNumber)
			}
		}
		if source, formal := byVersion[edge.FromVersion]; formal {
			if len(edge.FromImageDigests) != 1 || edge.FromImageDigests[0] != source.Manifest.ImageDigest {
				return fmt.Errorf("upgrade_edges[%d] formal source digest is not bound to its manifest", edgeNumber)
			}
		} else if edge.FromVersion != legacyInitialVersion || edge.ToVersion != firstFormalVersion {
			return fmt.Errorf("upgrade_edges[%d] source release manifest is not indexed", edgeNumber)
		}
		key := edge.FromVersion + "\x00" + edge.ToVersion
		if _, duplicate := seenEdges[key]; duplicate {
			return fmt.Errorf("upgrade edge %s to %s is duplicated", edge.FromVersion, edge.ToVersion)
		}
		seenEdges[key] = struct{}{}
		if edgeNumber > 0 && compareUpgradeEdges(index.Edges[edgeNumber-1], edge) >= 0 {
			return fmt.Errorf("upgrade_edges must be strictly ordered")
		}
	}
	return nil
}

func compareUpgradeEdges(left, right UpgradeEdge) int {
	leftFrom, _ := parseCanonicalVersion("from_version", left.FromVersion)
	rightFrom, _ := parseCanonicalVersion("from_version", right.FromVersion)
	if comparison := leftFrom.Compare(rightFrom); comparison != 0 {
		return comparison
	}
	leftTo, _ := parseCanonicalVersion("to_version", left.ToVersion)
	rightTo, _ := parseCanonicalVersion("to_version", right.ToVersion)
	return leftTo.Compare(rightTo)
}

// DirectUpgradeStep resolves only an explicitly published single-hop edge.
// It never turns an indirect path into an implicit direct upgrade.
func (index ReleaseIndex) DirectUpgradeStep(currentVersion, targetVersion string) (ReleaseStep, error) {
	if err := index.Validate(); err != nil {
		return ReleaseStep{}, err
	}
	current, err := parseCanonicalVersion("current_version", currentVersion)
	if err != nil {
		return ReleaseStep{}, err
	}
	target, err := parseCanonicalVersion("target_version", targetVersion)
	if err != nil {
		return ReleaseStep{}, err
	}
	if !current.LessThan(target) {
		return ReleaseStep{}, fmt.Errorf("target version must be newer than current version")
	}
	for _, edge := range index.Edges {
		if edge.FromVersion == currentVersion && edge.ToVersion == targetVersion {
			return index.stepFor(edge)
		}
	}
	return ReleaseStep{}, fmt.Errorf("direct upgrade edge is not published from %s to %s", currentVersion, targetVersion)
}

func (index ReleaseIndex) releaseForVersion(version string) (IndexedRelease, bool) {
	for _, release := range index.Releases {
		if release.Manifest.Version == version {
			return release, true
		}
	}
	return IndexedRelease{}, false
}

func (index ReleaseIndex) UpgradePath(currentVersion string) ([]ReleaseStep, error) {
	current, err := parseCanonicalVersion("current_version", currentVersion)
	if err != nil {
		return nil, err
	}
	latest, err := parseCanonicalVersion("latest_version", index.LatestVersion)
	if err != nil {
		return nil, err
	}
	if current.Equal(latest) {
		return []ReleaseStep{}, nil
	}
	if !current.LessThan(latest) {
		return nil, fmt.Errorf("upgrade path unsupported from %s to %s", currentVersion, index.LatestVersion)
	}
	for _, edge := range index.Edges {
		if edge.FromVersion == currentVersion && edge.ToVersion == index.LatestVersion {
			step, stepErr := index.stepFor(edge)
			if stepErr != nil {
				return nil, stepErr
			}
			return []ReleaseStep{step}, nil
		}
	}

	var paths [][]ReleaseStep
	var visit func(string, []ReleaseStep)
	visit = func(version string, path []ReleaseStep) {
		if len(paths) > 1 {
			return
		}
		if version == index.LatestVersion {
			paths = append(paths, append([]ReleaseStep(nil), path...))
			return
		}
		for _, edge := range index.Edges {
			if edge.FromVersion != version {
				continue
			}
			step, stepErr := index.stepFor(edge)
			if stepErr != nil {
				continue
			}
			visit(edge.ToVersion, append(path, step))
		}
	}
	visit(currentVersion, nil)
	if len(paths) == 0 {
		return nil, fmt.Errorf("upgrade path unsupported from %s to %s", currentVersion, index.LatestVersion)
	}
	if len(paths) > 1 {
		return nil, fmt.Errorf("upgrade path is ambiguous from %s to %s", currentVersion, index.LatestVersion)
	}
	return paths[0], nil
}

func (index ReleaseIndex) stepFor(edge UpgradeEdge) (ReleaseStep, error) {
	for _, release := range index.Releases {
		if release.Manifest.Version == edge.ToVersion {
			return ReleaseStep{
				Manifest:           release.Manifest,
				ManifestDigest:     release.ManifestDigest,
				SourceImageDigests: append([]string(nil), edge.FromImageDigests...),
			}, nil
		}
	}
	return ReleaseStep{}, fmt.Errorf("upgrade edge target %s is not indexed", strings.TrimSpace(edge.ToVersion))
}
