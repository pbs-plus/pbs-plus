package forks

import (
	"flag"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func CmdForkEntry() {
	cmdMode := flag.String("cmdMode", "", "Cmd Mode")
	sourceMode := flag.String("sourceMode", "", "Restore source mode (direct or snapshot)")
	readMode := flag.String("readMode", "", "File read mode (standard or mmap)")
	drive := flag.String("drive", "", "Drive or path for backup job")
	backupId := flag.String("backupId", "", "Unique job identifier for the backup job")
	restoreId := flag.String("restoreId", "", "Unique job identifier for the restore job")
	srcPath := flag.String("srcPath", "", "Path to be restored within snapshot")
	destPath := flag.String("destPath", "", "Destination path of files to be restored from snapshot")
	flag.Parse()

	syslog.L.Info().WithMessage("CmdFork: invoked").
		WithField("cmdMode", *cmdMode).
		WithField("sourceMode", *sourceMode).
		WithField("readMode", *readMode).
		WithField("drive", *drive).
		WithField("backupId", *backupId).
		WithField("restoreId", *restoreId).
		WithField("srcPath", *srcPath).
		WithField("destPath", *destPath).
		Write()

	if *cmdMode != "restore" && *cmdMode != "backup" {
		syslog.L.Info().WithMessage("CmdRestore: cmdMode not valid, returning").WithField("cmdMode", *cmdMode).Write()
		return
	}

	switch *cmdMode {
	case "backup":
		cmdBackup(sourceMode, readMode, drive, backupId)
	case "restore":
		cmdRestore(restoreId, srcPath, destPath)
	}
}
