package updater

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const (
	GitHubLatestReleaseAPI = "https://api.github.com/repos/YingSuiAI/dirextalk-message-server/releases/latest"
	indexAssetName         = "release-index.json"
	checksumAssetName      = "release-index.json.sha256"
	maxReleaseMetadataSize = 1024 * 1024
	maxReleaseAssetSize    = 1024 * 1024
	maxReleaseRedirects    = 5
)

var checksumPattern = regexp.MustCompile(`^([0-9a-f]{64})  release-index\.json\n$`)

type ResolvedRelease struct {
	Source         string `json:"source"`
	Version        string `json:"version"`
	Image          string `json:"image"`
	Digest         string `json:"digest"`
	ImageRef       string `json:"image_ref"`
	ManifestDigest string `json:"manifest_digest"`
	IndexDigest    string `json:"index_digest"`
	indexData      []byte
}

type GitHubReleaseSource struct {
	client *http.Client
}

// The canonical index is the runtime authority for retained-data edges. The
// message-server release workflow publishes this asset only after verifying an
// exact retained-upgrade attestation (and its checksum) for every configured
// source digest; the updater then independently verifies the published index
// checksum, canonical encoding, manifest digests, and exact edge binding.

func NewGitHubReleaseSource(client *http.Client) *GitHubReleaseSource {
	if client == nil {
		client = http.DefaultClient
	}
	return &GitHubReleaseSource{client: client}
}

func (source *GitHubReleaseSource) Latest(ctx context.Context) ([]byte, error) {
	resolved, err := source.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), resolved.indexData...), nil
}

func (source *GitHubReleaseSource) Resolve(ctx context.Context) (ResolvedRelease, error) {
	metadataData, err := source.get(ctx, GitHubLatestReleaseAPI, maxReleaseMetadataSize, nil)
	if err != nil {
		return ResolvedRelease{}, fmt.Errorf("fetch latest formal release: %w", err)
	}
	var metadata struct {
		TagName    string `json:"tag_name"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
		Assets     []struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
		} `json:"assets"`
	}
	decoder := json.NewDecoder(bytes.NewReader(metadataData))
	if err := decoder.Decode(&metadata); err != nil {
		return ResolvedRelease{}, fmt.Errorf("decode latest formal release metadata: %w", err)
	}
	if err := ensureJSONEOF(decoder, "latest formal release metadata"); err != nil {
		return ResolvedRelease{}, err
	}
	if metadata.Draft || metadata.Prerelease {
		return ResolvedRelease{}, fmt.Errorf("latest release is not a published stable release")
	}
	if _, err := parseCanonicalVersion("release tag", metadata.TagName); err != nil {
		return ResolvedRelease{}, err
	}
	assetURLs := map[string]string{}
	for _, asset := range metadata.Assets {
		if asset.Name != indexAssetName && asset.Name != checksumAssetName {
			continue
		}
		if assetURLs[asset.Name] != "" {
			return ResolvedRelease{}, fmt.Errorf("release contains duplicate %s assets", asset.Name)
		}
		if err := validateReleaseAssetURL(asset.URL, metadata.TagName, asset.Name); err != nil {
			return ResolvedRelease{}, err
		}
		assetURLs[asset.Name] = asset.URL
	}
	indexURL := assetURLs[indexAssetName]
	checksumURL := assetURLs[checksumAssetName]
	if indexURL == "" || checksumURL == "" {
		return ResolvedRelease{}, fmt.Errorf("formal release requires %s and %s assets", indexAssetName, checksumAssetName)
	}
	assetRedirect := func(candidate *url.URL) error {
		return validateReleaseAssetRedirect(candidate, metadata.TagName)
	}
	indexData, err := source.get(ctx, indexURL, maxReleaseAssetSize, assetRedirect)
	if err != nil {
		return ResolvedRelease{}, fmt.Errorf("fetch release index: %w", err)
	}
	checksumData, err := source.get(ctx, checksumURL, maxReleaseAssetSize, assetRedirect)
	if err != nil {
		return ResolvedRelease{}, fmt.Errorf("fetch release index checksum: %w", err)
	}
	checksumMatch := checksumPattern.FindStringSubmatch(string(checksumData))
	if checksumMatch == nil {
		return ResolvedRelease{}, fmt.Errorf("release index checksum has invalid format")
	}
	indexHash := sha256.Sum256(indexData)
	indexHashHex := hex.EncodeToString(indexHash[:])
	if checksumMatch[1] != indexHashHex {
		return ResolvedRelease{}, fmt.Errorf("release index checksum mismatch")
	}
	index, err := ValidateReleaseIndex(indexData)
	if err != nil {
		return ResolvedRelease{}, err
	}
	if index.LatestVersion != metadata.TagName {
		return ResolvedRelease{}, fmt.Errorf("release tag %s does not match index latest_version %s", metadata.TagName, index.LatestVersion)
	}
	latest := index.Releases[len(index.Releases)-1]
	return ResolvedRelease{
		Source:         "github_release",
		Version:        latest.Manifest.Version,
		Image:          latest.Manifest.Image,
		Digest:         latest.Manifest.ImageDigest,
		ImageRef:       latest.Manifest.Image + "@" + latest.Manifest.ImageDigest,
		ManifestDigest: latest.ManifestDigest,
		IndexDigest:    "sha256:" + indexHashHex,
		indexData:      append([]byte(nil), indexData...),
	}, nil
}

func (source *GitHubReleaseSource) get(ctx context.Context, requestURL string, maximum int64, validateRedirect func(*url.URL) error) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", "dirextalk-updater")
	client := *source.client
	configuredRedirect := client.CheckRedirect
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if validateRedirect == nil {
			return http.ErrUseLastResponse
		}
		if len(via) > maxReleaseRedirects {
			return fmt.Errorf("release asset exceeded redirect limit")
		}
		if err := validateRedirect(next.URL); err != nil {
			return err
		}
		if configuredRedirect != nil {
			return configuredRedirect(next, via)
		}
		return nil
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}
	if validateRedirect == nil {
		if response.Request == nil || response.Request.URL.String() != requestURL {
			return nil, fmt.Errorf("fixed release endpoint redirected")
		}
	} else if response.Request == nil || validateRedirect(response.Request.URL) != nil {
		return nil, fmt.Errorf("release asset response URL is not trusted")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, fmt.Errorf("response exceeds %d bytes", maximum)
	}
	return data, nil
}

func validateReleaseAssetURL(rawURL, tag, name string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("release asset %s must use an HTTPS github.com URL", name)
	}
	wantPath := "/YingSuiAI/dirextalk-message-server/releases/download/" + tag + "/" + name
	if parsed.EscapedPath() != wantPath || strings.Contains(parsed.Path, "..") {
		return fmt.Errorf("release asset %s URL does not match release tag", name)
	}
	return nil
}

func validateReleaseAssetRedirect(candidate *url.URL, tag string) error {
	if candidate == nil || candidate.Scheme != "https" || candidate.User != nil || candidate.Fragment != "" {
		return fmt.Errorf("release asset redirect is not HTTPS")
	}
	switch candidate.Host {
	case "github.com":
		prefix := "/YingSuiAI/dirextalk-message-server/releases/download/" + tag + "/"
		if candidate.RawQuery != "" || !strings.HasPrefix(candidate.EscapedPath(), prefix) || strings.Contains(candidate.Path, "..") {
			return fmt.Errorf("release asset redirect changed the release identity")
		}
	case "release-assets.githubusercontent.com", "objects.githubusercontent.com":
		if candidate.Path == "" || candidate.Path == "/" {
			return fmt.Errorf("release asset redirect path is empty")
		}
	default:
		return fmt.Errorf("release asset redirect host is not allowed")
	}
	return nil
}
