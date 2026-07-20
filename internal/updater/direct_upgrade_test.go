package updater

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

type directControlRuntime struct {
	currentVersion string
	currentCalls   int
}

func (runtime *directControlRuntime) CurrentVersion(context.Context) (string, error) {
	runtime.currentCalls++
	return runtime.currentVersion, nil
}

func TestControlDirectJobQueuesAnyNewerCentralTargetWithoutReleaseLookup(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	runtime := &directControlRuntime{currentVersion: "v1.0.5"}
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	response := postJSON(t, service.Handler(), controlJobsPath, directJobRequest("v1.0.7", "2d4d8444-2b3d-4f8f-8503-910f58b5b1df"), testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("create direct job: %d %s", response.Code, response.Body.String())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte(`"image_digest"`), []byte(`"release_chain"`), []byte(`"release_index_digest"`), []byte(`"client_version"`)} {
		if bytes.Contains(data, forbidden) {
			t.Fatalf("new direct job persisted legacy release metadata %s: %s", forbidden, data)
		}
	}
	if runtime.currentCalls != 1 || len(state.Jobs) != 1 || len(state.Plans) != 0 {
		t.Fatalf("direct target was not queued without release discovery: current_calls=%d state=%#v", runtime.currentCalls, state)
	}
	for _, job := range state.Jobs {
		if job.DirectRelease == nil || job.DirectRelease.Version != "v1.0.7" || job.DirectRelease.ImageDigest != "" || job.PlanTokenHash != "" || job.ManifestDigest != "" || job.CurrentVersion != "v1.0.5" || job.TargetVersion != "v1.0.7" {
			t.Fatalf("job persisted more than the centrally authorized target version: %#v", job)
		}
	}
}

func TestControlDirectJobDoesNotRequireClientReleaseMetadata(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(&directControlRuntime{currentVersion: "v1.0.5"}))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"target_version":"v1.0.7","idempotency_key":"3d4d8444-2b3d-4f8f-8503-910f58b5b1df","confirm":"apply_release_change"}`
	response := postJSON(t, service.Handler(), controlJobsPath, body, testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("metadata-free direct request was rejected: %d %s", response.Code, response.Body.String())
	}
}
