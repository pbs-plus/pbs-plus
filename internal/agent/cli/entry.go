package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func Entry() {
	cmdMode := flag.String("cmdMode", "", "Cmd Mode")
	sourceMode := flag.String("sourceMode", "", "Restore source mode (direct or snapshot)")
	readMode := flag.String("readMode", "", "File read mode (standard or mmap)")
	restoreMode := flag.Int("restoreMode", 0, "Restore mode")
	drive := flag.String("drive", "", "Drive or path for backup job")
	id := flag.String("id", "", "Unique job identifier for the job")
	srcPath := flag.String("srcPath", "/", "Path to be restored within snapshot")
	destPath := flag.String("destPath", "", "Destination path of files to be restored from snapshot")
	token := flag.String("token", "", "Auth Token")
	flag.Parse()

	if *cmdMode != "restore" && *cmdMode != "backup" {
		syslog.L.Debug().WithMessage("CLI: cmdMode invalid, returning").WithField("cmdMode", *cmdMode).Write()
		return
	}

	if *token == "" {
		fmt.Fprintln(os.Stderr, "Error: token required")
		os.Exit(1)
	}

	syslog.L.Debug().WithMessage("CmdFork: invoked").
		WithField("cmdMode", *cmdMode).
		WithField("sourceMode", *sourceMode).
		WithField("readMode", *readMode).
		WithField("restoreMode", *restoreMode).
		WithField("drive", *drive).
		WithField("id", *id).
		WithField("srcPath", *srcPath).
		WithField("destPath", *destPath).
		Write()

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-%s-%s", *cmdMode, *id))
	expectedToken, err := os.ReadFile(tokenFile)
	os.Remove(tokenFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: token file not found")
		syslog.L.Error(err).WithMessage("cmdBackup: token file read failed").Write()
		os.Exit(1)
	}

	if string(expectedToken) != *token {
		fmt.Fprintln(os.Stderr, "Error: invalid token")
		os.Exit(1)
	}

	switch *cmdMode {
	case "backup":
		cmdBackup(sourceMode, readMode, drive, id)
	case "restore":
		cmdRestore(id, srcPath, destPath, restoreMode)
	}
}
