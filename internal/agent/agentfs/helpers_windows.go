//go:build windows

package agentfs

import (
	"os"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

func mapWinError(err error, helper string) error {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_PATH_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_ACCESS_DENIED:
		return os.ErrPermission
	default:
		syslog.L.Error(err).WithMessage("unknown windows error").WithField("helper", helper).Write()
		return &os.PathError{
			Op:   "access",
			Path: "",
			Err:  err,
		}
	}
}
