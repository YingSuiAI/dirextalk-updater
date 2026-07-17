package main

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-updater/internal/updater"
)

type recordingInitialPinner struct {
	called bool
}

func (pinner *recordingInitialPinner) PinInitialLatest(context.Context) error {
	pinner.called = true
	return nil
}

func TestPinInitialLatestUsesConfiguredComposeProject(t *testing.T) {
	pinner := &recordingInitialPinner{}
	var gotMode updater.CaddyMode
	var gotProject updater.ComposeProject
	config := updater.Config{
		CaddyMode:      updater.CaddyModeCompose,
		ComposeProject: updater.ComposeProjectLegacy,
	}
	err := pinInitialLatestWithConfig(context.Background(), config, func(mode updater.CaddyMode, project updater.ComposeProject) (initialLatestPinner, error) {
		gotMode = mode
		gotProject = project
		return pinner, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotMode != config.CaddyMode || gotProject != config.ComposeProject {
		t.Fatalf("runtime selection = %q/%q, want %q/%q", gotMode, gotProject, config.CaddyMode, config.ComposeProject)
	}
	if !pinner.called {
		t.Fatal("initial image pin was not invoked")
	}
}
