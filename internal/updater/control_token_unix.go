//go:build !windows

package updater

import (
	"fmt"
	"os"
	"syscall"
)

func validateControlTokenFile(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("control token file owner metadata is unavailable")
	}
	return validateRootControlTokenMetadata(info.Mode(), stat.Uid, uint32(os.Geteuid()))
}
