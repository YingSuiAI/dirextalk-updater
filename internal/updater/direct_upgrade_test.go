package updater

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

var errTestDirectSource = errors.New("unexpected direct source")

type directControlRuntime struct {
	currentVersion string
	source         DirectSource
	inspectCalls   int
	inspectErr     error
}

func (runtime *directControlRuntime) CurrentVersion(context.Context) (string, error) {
	return runtime.currentVersion, nil
}

func (runtime *directControlRuntime) InspectDirectSource(_ context.Context, expectedVersion string, step ReleaseStep) (DirectSource, error) {
	runtime.inspectCalls++
	if runtime.inspectErr != nil {
		return DirectSource{}, runtime.inspectErr
	}
	if expectedVersion != runtime.source.Version || step.Manifest.Version == "" {
		return DirectSource{}, errTestDirectSource
	}
	return runtime.source, nil
}

type staticDirectReleaseSource struct {
	data  []byte
	err   error
	calls int
}

func (source *staticDirectReleaseSource) Latest(context.Context) ([]byte, error) {
	source.calls++
	if source.err != nil {
		return nil, source.err
	}
	return append([]byte(nil), source.data...), nil
}

func directTestReleaseIndexJSON(t *testing.T) string {
	t.Helper()
	targetManifest := manifestJSONFor("v1.0.3", strings.Repeat("a", 64), ">=v1.0.0 <v1.0.3")
	return directTestReleaseIndexWithTarget(t, targetManifest)
}

func directTestReleaseIndexWithTarget(t *testing.T, targetManifest string) string {
	t.Helper()
	sourceManifest := manifestJSONFor("v1.0.0", strings.Repeat("0", 64), ">=v0.15.2 <v1.0.0")
	sourceManifest = strings.Replace(sourceManifest, `"schema_version": 2`, `"schema_version": 1`, 1)
	return `{"release_index_version":1,"latest_version":"v1.0.3","releases":[` + indexedManifestJSON(t, sourceManifest) + `,` + indexedManifestJSON(t, targetManifest) + `],"upgrade_edges":[` +
		`{"from_version":"v1.0.0","from_image_digests":["sha256:` + strings.Repeat("0", 64) + `"],"to_version":"v1.0.3"}]}`
}

func TestControlDirectJobRejectsSchemaAndClientIncompatibility(t *testing.T) {
	tests := []struct {
		name         string
		index        func(*testing.T) string
		client       string
		responseCode string
	}{
		{
			name: "schema",
			index: func(t *testing.T) string {
				target := strings.Replace(manifestJSONFor("v1.0.3", strings.Repeat("a", 64), ">=v1.0.0 <v1.0.3"), `"schema_compat_version": 1`, `"schema_compat_version": 2`, 1)
				return directTestReleaseIndexWithTarget(t, target)
			},
			client:       "v1.0.0",
			responseCode: "schema_incompatible",
		},
		{name: "client", index: directTestReleaseIndexJSON, client: "v2.0.0", responseCode: "client_version_incompatible"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
			service, err := NewService(store, testControlToken, WithDirectJobRuntime(newTestDirectRuntime()), WithReleaseSource(&staticDirectReleaseSource{data: []byte(test.index(t))}))
			if err != nil {
				t.Fatal(err)
			}
			body := strings.Replace(directJobRequest("v1.0.3", "4d4d8444-2b3d-4f8f-8503-910f58b5b1df"), `"client_version":"v1.0.0"`, `"client_version":"`+test.client+`"`, 1)
			response := postJSON(t, service.Handler(), controlJobsPath, body, testControlToken, "")
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), test.responseCode) {
				t.Fatalf("incompatible direct job was accepted: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestControlDirectJobPersistsTrustedPlanBeforeExecution(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	runtime := newTestDirectRuntime()
	releaseSource := &staticDirectReleaseSource{data: []byte(directTestReleaseIndexJSON(t))}
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime), WithReleaseSource(releaseSource))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	response := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.3", "2d4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("create direct job: %d %s", response.Code, response.Body.String())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.inspectCalls != 1 || releaseSource.calls != 1 || len(state.Jobs) != 1 || len(state.Plans) != 1 {
		t.Fatalf("trusted target was not resolved into one bound plan: runtime_calls=%d release_calls=%d state=%#v", runtime.inspectCalls, releaseSource.calls, state)
	}
	for _, job := range state.Jobs {
		plan := state.Plans[job.PlanTokenHash]
		if job.DirectRelease != nil || plan.DirectContractVersion != DirectContractVersion || plan.Manifest.ImageDigest != "sha256:"+strings.Repeat("a", 64) || plan.SourceImageDigest != runtime.source.ImageDigest || plan.ReleaseIndexDigest != releaseIndexDigest(releaseSource.data) || job.CurrentVersion != "v1.0.0" || job.TargetVersion != "v1.0.3" {
			t.Fatalf("job did not durably bind the trusted release contract: job=%#v plan=%#v", job, plan)
		}
	}
}

func TestControlDirectJobRejectsUntrustedSourceDigestAndMissingEdge(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		digest string
		code   string
	}{
		{name: "source digest", target: "v1.0.3", digest: "sha256:" + strings.Repeat("f", 64), code: "source_image_digest_untrusted"},
		{name: "missing direct edge", target: "v1.0.2", digest: "sha256:" + strings.Repeat("0", 64), code: "upgrade_edge_unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
			runtime := newTestDirectRuntime()
			runtime.source.ImageDigest = test.digest
			service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime), WithReleaseSource(&staticDirectReleaseSource{data: []byte(directTestReleaseIndexJSON(t))}))
			if err != nil {
				t.Fatal(err)
			}
			response := postJSON(t, service.Handler(), controlJobsPath, directJobRequest(test.target, "3d4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), test.code) {
				t.Fatalf("unsafe direct target was not rejected: %d %s", response.Code, response.Body.String())
			}
			state, loadErr := store.Load(context.Background())
			if loadErr != nil || len(state.Jobs) != 0 || len(state.Plans) != 0 {
				t.Fatalf("rejected target changed durable state: state=%#v err=%v", state, loadErr)
			}
		})
	}
}
