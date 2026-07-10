//go:build windows

package updater

import (
	"fmt"
	"os"
)

func validateConfigFile(os.FileInfo) error {
	return fmt.Errorf("updater config ACL validation is not implemented on Windows; updater execution is Linux-only")
}
