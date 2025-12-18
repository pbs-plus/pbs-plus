package forks

import (
	"flag"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func CmdForkEntry() {
	cmdMode := flag.String("cmdMode", "", "Cmd Mode")
	sourceMode := flag.String("sourceMode", "", "Restore source mode (direct or snapshot)")
	readMode := flag.String("readMode", "", "File read mode (standard or mmap)")
	drive := flag.String("drive", "", "Drive or path for restore")
	jobId := flag.String("jobId", "", "Unique job identifier for the restore")
	flag.Parse()

	syslog.L.Info().WithMessage("CmdFork: invoked").
		WithField("cmdMode", *cmdMode).
		WithField("sourceMode", *sourceMode).
		WithField("readMode", *readMode).
		WithField("drive", *drive).
		WithField("jobId", *jobId).
		Write()

	if *cmdMode != "restore" && *cmdMode != "backup" {
		syslog.L.Info().WithMessage("CmdRestore: cmdMode not valid, returning").WithField("cmdMode", *cmdMode).Write()
		return
	}

	switch *cmdMode {
	case "backup":
		cmdBackup(sourceMode, readMode, drive, jobId)
	case "restore":
		cmdRestore(sourceMode, readMode, drive, jobId)
	}
}
