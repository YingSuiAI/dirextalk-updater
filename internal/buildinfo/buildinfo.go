package buildinfo

import (
	"fmt"
	"regexp"
	"time"
)

var (
	Version   = "v0.0.0-dev+local"
	Commit    = "uncommitted"
	BuildTime = ""
)

var (
	stableVersionPattern = regexp.MustCompile(`^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$`)
	commitPattern        = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

func Current() Info {
	return Info{Version: Version, Commit: Commit, BuildTime: BuildTime}
}

func (info Info) ValidateRelease() error {
	if !stableVersionPattern.MatchString(info.Version) {
		return fmt.Errorf("version must be a canonical stable tag such as v1.0.0")
	}
	if !commitPattern.MatchString(info.Commit) {
		return fmt.Errorf("commit must be a full lowercase Git commit hash")
	}
	builtAt, err := time.Parse(time.RFC3339, info.BuildTime)
	if err != nil || builtAt.Location() != time.UTC {
		return fmt.Errorf("build_time must be an RFC3339 UTC timestamp")
	}
	return nil
}
