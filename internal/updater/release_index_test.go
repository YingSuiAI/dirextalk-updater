package updater

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func manifestJSONFor(version, digest, upgradeFrom string) string {
	data := validManifestJSON()
	data = strings.ReplaceAll(data, "v1.1.0", version)
	data = strings.Replace(data, strings.Repeat("a", 64), digest, 1)
	data = strings.Replace(data, ">=v1.0.0 <"+version, upgradeFrom, 1)
	return data
}

func indexedManifestJSON(t *testing.T, manifest string) string {
	t.Helper()
	var compact json.RawMessage
	if err := json.Unmarshal([]byte(manifest), &compact); err != nil {
		t.Fatal(err)
	}
	compactData, err := json.Marshal(compact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(compactData)
	return `{"manifest":` + string(compactData) + `,"manifest_digest":"sha256:` + hex.EncodeToString(digest[:]) + `"}`
}

func validReleaseIndexJSON(t *testing.T) string {
	t.Helper()
	v100 := indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0"))
	v110 := indexedManifestJSON(t, validManifestJSON())
	v120 := indexedManifestJSON(t, manifestJSONFor("v1.2.0", strings.Repeat("b", 64), ">=v1.1.0 <v1.2.0"))
	return `{"release_index_version":1,"latest_version":"v1.2.0","releases":[` + v100 + `,` + v110 + `,` + v120 + `],"upgrade_edges":[` +
		`{"from_version":"v1.0.0","from_image_digests":["sha256:` + strings.Repeat("0", 64) + `"],"to_version":"v1.1.0"},` +
		`{"from_version":"v1.1.0","from_image_digests":["sha256:` + strings.Repeat("a", 64) + `"],"to_version":"v1.2.0"}]}`
}

func validSingleReleaseIndexJSON(t *testing.T) string {
	t.Helper()
	v100 := indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0"))
	return `{"release_index_version":1,"latest_version":"v1.1.0","releases":[` + v100 + `,` + indexedManifestJSON(t, validManifestJSON()) + `],"upgrade_edges":[` +
		`{"from_version":"v1.0.0","from_image_digests":["sha256:` + strings.Repeat("0", 64) + `"],"to_version":"v1.1.0"}]}`
}

func testDiscoveryCache(t *testing.T, data []byte, status DiscoveryStatus, checkedAt time.Time) DiscoveryCache {
	t.Helper()
	index, err := ValidateReleaseIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	latest := index.Releases[len(index.Releases)-1]
	return DiscoveryCache{
		Status:         status,
		CheckedAt:      checkedAt.UTC(),
		Manifest:       &latest.Manifest,
		ManifestDigest: latest.ManifestDigest,
		Index:          &index,
		IndexDigest:    releaseIndexDigest(data),
	}
}

// mustUpgradePath remains a test helper for validating recovery of legacy
// persisted jobs. It does not exercise or enable GitHub discovery.
func mustUpgradePath(t *testing.T, index ReleaseIndex, current string) []ReleaseStep {
	t.Helper()
	path, err := index.UpgradePath(current)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestValidateReleaseIndexBindsEmbeddedManifestsAndBuildsUniquePath(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatalf("ValidateReleaseIndex: %v", err)
	}
	path, err := index.UpgradePath("v1.0.0")
	if err != nil {
		t.Fatalf("UpgradePath: %v", err)
	}
	if len(path) != 2 || path[0].Manifest.Version != "v1.1.0" || path[1].Manifest.Version != "v1.2.0" {
		t.Fatalf("unexpected path: %#v", path)
	}
	if path[0].SourceImageDigests[0] != "sha256:"+strings.Repeat("0", 64) {
		t.Fatalf("legacy source digest was not bound: %#v", path[0])
	}
}

func TestValidateReleaseIndexRejectsTamperOrderingAndUnboundFormalSource(t *testing.T) {
	valid := validReleaseIndexJSON(t)
	v100 := indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0"))
	tests := []struct {
		name string
		data string
	}{
		{"embedded manifest tamper", strings.Replace(valid, "dirextalk/message-server:v1.1.0", "dirextalk/message-server:v9.9.9", 1)},
		{"release ordering", strings.Replace(valid, `"releases":[`+indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0"))+`,`+indexedManifestJSON(t, validManifestJSON()), `"releases":[`+indexedManifestJSON(t, validManifestJSON())+`,`+indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0")), 1)},
		{"formal source digest mismatch", strings.Replace(valid, `"sha256:`+strings.Repeat("a", 64)+`"],"to_version":"v1.2.0"`, `"sha256:`+strings.Repeat("c", 64)+`"],"to_version":"v1.2.0"`, 1)},
		{"formal source manifest omitted", strings.Replace(valid, v100+`,`, "", 1)},
		{"unknown field", strings.TrimSuffix(valid, "}") + `,"command":"rm -rf /"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateReleaseIndex([]byte(test.data)); err == nil {
				t.Fatal("expected invalid index")
			}
		})
	}
}

func TestUpgradePathPrefersDirectEdgeAndRejectsAmbiguousIntermediatePaths(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	index.Edges = append(index.Edges, UpgradeEdge{
		FromVersion:      "v1.0.0",
		FromImageDigests: []string{"sha256:" + strings.Repeat("0", 64)},
		ToVersion:        "v1.2.0",
	})
	path, err := index.UpgradePath("v1.0.0")
	if err != nil || len(path) != 1 || path[0].Manifest.Version != "v1.2.0" {
		t.Fatalf("direct path must win: path=%#v err=%v", path, err)
	}

	index.Edges = index.Edges[:len(index.Edges)-1]
	index.Releases = append([]IndexedRelease{{Manifest: Manifest{Version: "v1.1.5", ImageDigest: "sha256:" + strings.Repeat("d", 64)}}}, index.Releases...)
	index.Edges = append(index.Edges,
		UpgradeEdge{FromVersion: "v1.0.0", FromImageDigests: []string{"sha256:" + strings.Repeat("0", 64)}, ToVersion: "v1.1.5"},
		UpgradeEdge{FromVersion: "v1.1.5", FromImageDigests: []string{"sha256:" + strings.Repeat("d", 64)}, ToVersion: "v1.2.0"},
	)
	if _, err := index.UpgradePath("v1.0.0"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguous path rejection, got %v", err)
	}
}

func TestDirectUpgradeStepRequiresPublishedSingleHopEdge(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	step, err := index.DirectUpgradeStep("v1.0.0", "v1.1.0")
	if err != nil || step.Manifest.Version != "v1.1.0" || !reflect.DeepEqual(step.SourceImageDigests, []string{"sha256:" + strings.Repeat("0", 64)}) {
		t.Fatalf("published direct edge was not resolved: step=%#v err=%v", step, err)
	}
	if _, err := index.DirectUpgradeStep("v1.0.0", "v1.2.0"); err == nil || !strings.Contains(err.Error(), "not published") {
		t.Fatalf("indirect path was accepted as a direct edge: %v", err)
	}
}

func TestUpgradePathRejectsUnsupportedEdge(t *testing.T) {
	index, err := ValidateReleaseIndex([]byte(validReleaseIndexJSON(t)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.UpgradePath("v0.15.2"); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported path, got %v", err)
	}
}

func TestReleaseIndexAllowsOnlyExactLegacyEdgeIntoFirstFormalRelease(t *testing.T) {
	v100 := indexedManifestJSON(t, manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0"))
	legacyDigest := "sha256:d57a0b7830f7248e29fe7c45c0848cb1167454709fd33effe07ff074415f571c"
	data := `{"release_index_version":1,"latest_version":"v1.0.0","releases":[` + v100 + `],"upgrade_edges":[` +
		`{"from_version":"v0.15.2","from_image_digests":["` + legacyDigest + `"],"to_version":"v1.0.0"}]}`
	index, err := ValidateReleaseIndex([]byte(data))
	if err != nil {
		t.Fatalf("validate legacy rollout index: %v", err)
	}
	path, err := index.UpgradePath("v0.15.2")
	if err != nil || len(path) != 1 || !reflect.DeepEqual(path[0].SourceImageDigests, []string{legacyDigest}) {
		t.Fatalf("legacy exact digest edge was not preserved: path=%#v err=%v", path, err)
	}

	invalid := strings.Replace(data, `"to_version":"v1.0.0"`, `"to_version":"v1.1.0"`, 1)
	if _, err := ValidateReleaseIndex([]byte(invalid)); err == nil {
		t.Fatal("legacy source was allowed to skip the first formal release")
	}
}
