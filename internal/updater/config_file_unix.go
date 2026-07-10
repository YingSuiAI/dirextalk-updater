//go:build !windows

package updater

import (
	"fmt"
	"os"
	"syscall"
)

func validateConfigFile(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("updater config file owner metadata is unavailable")
	}
	return validateRootConfigMetadata(info.Mode(), stat.Uid, uint32(os.Geteuid()))
}
