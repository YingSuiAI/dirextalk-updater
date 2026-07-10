package updater

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const (
	testIndexURL    = "https://github.com/YingSuiAI/dirextalk-message-server/releases/download/v1.2.0/release-index.json"
	testChecksumURL = "https://github.com/YingSuiAI/dirextalk-message-server/releases/download/v1.2.0/release-index.json.sha256"
)

type staticReleaseTransport struct {
	responses map[string]staticHTTPResponse
}

type staticHTTPResponse struct {
	status int
	body   string
}

func (transport staticReleaseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, ok := transport.responses[request.URL.String()]
	if !ok {
		return nil, fmt.Errorf("unexpected request: %s", request.URL)
	}
	return &http.Response{
		StatusCode: response.status,
		Status:     fmt.Sprintf("%d test", response.status),
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Header:     make(http.Header),
		Request:    request,
	}, nil
}

func TestGitHubReleaseSourceResolvesFormalReleaseToImmutableImage(t *testing.T) {
	indexData := validReleaseIndexJSON(t)
	resolved, err := NewGitHubReleaseSource(releaseHTTPClient(indexData, formalReleaseJSON("v1.2.0", false, false), "")).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Version != "v1.2.0" || resolved.Image != "dirextalk/message-server:v1.2.0" {
		t.Fatalf("unexpected release identity: %#v", resolved)
	}
	if resolved.Digest != "sha256:"+strings.Repeat("b", 64) {
		t.Fatalf("unexpected image digest: %#v", resolved)
	}
	if resolved.ImageRef != resolved.Image+"@"+resolved.Digest {
		t.Fatalf("release is not pinned by digest: %#v", resolved)
	}
	wantIndexDigest := sha256.Sum256([]byte(indexData))
	if resolved.IndexDigest != "sha256:"+hex.EncodeToString(wantIndexDigest[:]) {
		t.Fatalf("index digest mismatch: %#v", resolved)
	}
}

func TestGitHubReleaseSourceRejectsUntrustedOrInconsistentRelease(t *testing.T) {
	tests := []struct {
		name         string
		release      string
		index        string
		checksumBody string
	}{
		{name: "draft", release: formalReleaseJSON("v1.2.0", true, false), index: validReleaseIndexJSON(t)},
		{name: "prerelease", release: formalReleaseJSON("v1.2.0", false, true), index: validReleaseIndexJSON(t)},
		{name: "non canonical tag", release: formalReleaseJSON("latest", false, false), index: validReleaseIndexJSON(t)},
		{name: "tag index mismatch", release: formalReleaseJSON("v1.3.0", false, false), index: validReleaseIndexJSON(t)},
		{name: "embedded manifest tamper", release: formalReleaseJSON("v1.2.0", false, false), index: strings.Replace(validReleaseIndexJSON(t), "dirextalk/message-server:v1.1.0", "dirextalk/message-server:v9.9.9", 1)},
		{name: "invalid image digest", release: formalReleaseJSON("v1.2.0", false, false), index: strings.Replace(validReleaseIndexJSON(t), "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("A", 64), 1)},
		{name: "checksum mismatch", release: formalReleaseJSON("v1.2.0", false, false), index: validReleaseIndexJSON(t), checksumBody: strings.Repeat("0", 64) + "  release-index.json\n"},
		{name: "missing assets", release: `{"tag_name":"v1.2.0","draft":false,"prerelease":false,"assets":[]}`, index: validReleaseIndexJSON(t)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewGitHubReleaseSource(releaseHTTPClient(test.index, test.release, test.checksumBody)).Resolve(context.Background())
			if err == nil {
				t.Fatal("expected release to be rejected")
			}
		})
	}
}

func TestGitHubReleaseSourceRejectsUnavailableRelease(t *testing.T) {
	client := &http.Client{Transport: staticReleaseTransport{responses: map[string]staticHTTPResponse{
		GitHubLatestReleaseAPI: {status: http.StatusNotFound, body: `{"message":"Not Found"}`},
	}}}
	if _, err := NewGitHubReleaseSource(client).Resolve(context.Background()); err == nil {
		t.Fatal("expected unavailable GitHub Release to fail closed")
	}
}

func releaseHTTPClient(indexData, release, checksumBody string) *http.Client {
	if checksumBody == "" {
		digest := sha256.Sum256([]byte(indexData))
		checksumBody = hex.EncodeToString(digest[:]) + "  release-index.json\n"
	}
	return &http.Client{Transport: staticReleaseTransport{responses: map[string]staticHTTPResponse{
		GitHubLatestReleaseAPI: {status: http.StatusOK, body: release},
		testIndexURL:           {status: http.StatusOK, body: indexData},
		testChecksumURL:        {status: http.StatusOK, body: checksumBody},
	}}}
}

func formalReleaseJSON(tag string, draft, prerelease bool) string {
	return fmt.Sprintf(`{
		"tag_name": %q,
		"draft": %t,
		"prerelease": %t,
		"assets": [
			{"name":"release-index.json","browser_download_url":%q},
			{"name":"release-index.json.sha256","browser_download_url":%q}
		]
	}`, tag, draft, prerelease, testIndexURL, testChecksumURL)
}
