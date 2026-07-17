package updater

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
)

var errTestDirectTarget = errors.New("unexpected direct target")

type directControlRuntime struct {
	currentVersion string
	resolved       DirectRelease
	resolveCalls   int
}

func (runtime *directControlRuntime) CurrentVersion(context.Context) (string, error) {
	return runtime.currentVersion, nil
}

func (runtime *directControlRuntime) ResolveDirectRelease(_ context.Context, targetVersion string) (DirectRelease, error) {
	runtime.resolveCalls++
	if targetVersion != runtime.resolved.Version {
		return DirectRelease{}, errTestDirectTarget
	}
	return runtime.resolved, nil
}

func TestControlDirectJobPersistsResolvedDigestBeforeExecution(t *testing.T) {
	store := NewStateStore(filepath.Join(t.TempDir(), "state.json"))
	runtime := &directControlRuntime{
		currentVersion: "v1.0.0",
		resolved: DirectRelease{
			Version:     "v1.0.3",
			ImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	service, err := NewService(store, testControlToken, WithDirectJobRuntime(runtime))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	response := postJSON(t, service.Handler(), controlJobsPath, `{"target_version":"v1.0.3","idempotency_key":"2d4d8444-2b3d-4f8f-8503-910f58b5b1df","confirm":"apply_release_change"}`, testControlToken, "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("create direct job: %d %s", response.Code, response.Body.String())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.resolveCalls != 1 || len(state.Jobs) != 1 || len(state.Plans) != 0 {
		t.Fatalf("direct target was not resolved into a standalone job: calls=%d state=%#v", runtime.resolveCalls, state)
	}
	for _, job := range state.Jobs {
		if job.DirectRelease == nil || job.DirectRelease.ImageDigest != runtime.resolved.ImageDigest || job.CurrentVersion != "v1.0.0" || job.TargetVersion != "v1.0.3" {
			t.Fatalf("job did not durably bind the resolved direct target: %#v", job)
		}
	}
}
