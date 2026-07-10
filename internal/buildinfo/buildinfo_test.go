package buildinfo

import (
	"strings"
	"testing"
)

func TestCurrentDefaultsToLocalDevelopmentIdentity(t *testing.T) {
	got := Current()
	if got.Version != "v0.0.0-dev+local" || got.Commit != "uncommitted" || got.BuildTime != "" {
		t.Fatalf("unexpected local build identity: %#v", got)
	}
	if err := got.ValidateRelease(); err == nil {
		t.Fatal("local development identity must not validate as a formal release")
	}
}

func TestValidateReleaseRequiresCanonicalStableIdentity(t *testing.T) {
	valid := Info{Version: "v1.0.0", Commit: strings.Repeat("a", 40), BuildTime: "2026-07-10T08:09:10Z"}
	if err := valid.ValidateRelease(); err != nil {
		t.Fatalf("valid release identity rejected: %v", err)
	}
	for _, invalid := range []Info{
		{Version: "1.0.0", Commit: valid.Commit, BuildTime: valid.BuildTime},
		{Version: "v1.0.0-rc.1", Commit: valid.Commit, BuildTime: valid.BuildTime},
		{Version: valid.Version, Commit: "uncommitted", BuildTime: valid.BuildTime},
		{Version: valid.Version, Commit: valid.Commit, BuildTime: "yesterday"},
	} {
		if err := invalid.ValidateRelease(); err == nil {
			t.Fatalf("invalid release identity accepted: %#v", invalid)
		}
	}
}
