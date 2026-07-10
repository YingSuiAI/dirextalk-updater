package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/YingSuiAI/dirextalk-updater/internal/buildinfo"
	"github.com/YingSuiAI/dirextalk-updater/internal/releaseartifact"
)

func main() {
	binaryPath := flag.String("binary", "", "path to the release binary")
	outputDir := flag.String("output", "dist", "release asset output directory")
	flag.Parse()
	if *binaryPath == "" {
		fmt.Fprintln(os.Stderr, "release-artifacts: -binary is required")
		os.Exit(2)
	}
	if err := releaseartifact.Generate(*outputDir, *binaryPath, buildinfo.Current()); err != nil {
		fmt.Fprintln(os.Stderr, "release-artifacts:", err)
		os.Exit(1)
	}
}
