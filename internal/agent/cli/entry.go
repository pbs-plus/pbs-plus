package cli

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/log"
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

	if *cmdMode != "restore" && *cmdMode != "backup" && *cmdMode != "verify" {
		log.Debug("cli: invalid command mode", "cmdMode", *cmdMode)
		return
	}

	if err := log.L.SetServiceLogger(); err != nil {
		log.Error(err, "failed to initialize service logger")
	}

	if *token == "" {
		fmt.Fprintln(os.Stderr, "Error: token required")
		os.Exit(1)
	}
	log.Debug("cli: fork invoked",

		"destPath", *destPath, "srcPath", *srcPath, "id", *id, "drive", *drive, "restoreMode", *restoreMode, "readMode", *readMode, "sourceMode", *sourceMode, "cmdMode", *cmdMode)

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-%s-%s", *cmdMode, *id))
	expectedToken, err := os.ReadFile(tokenFile)
	if err := os.Remove(tokenFile); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: token file not found")
		log.Error(err, "backup: token file read failed")
		os.Exit(1)
	}

	if string(expectedToken) != *token {
		os.Exit(1)
	}

	if os.Getenv("PBS_PLUS_PPROF") == "true" {
		go func() {
			port := 6060
			maxAttempts := 10

			for range maxAttempts {
				addr := fmt.Sprintf(":%d", port)
				listener, err := net.Listen("tcp", addr)
				if err != nil {
					port++
					continue
				}

				log.Info("pprof server listening", "addr", addr)
				log.Error(http.Serve(listener, nil), "pprof server stopped")
				return
			}

			log.Error(fmt.Errorf("failed to start pprof server after %d attempts", maxAttempts), "")
		}()
	}

	switch *cmdMode {
	case "backup":
		cmdBackup(sourceMode, readMode, drive, id)
	case "restore":
		cmdRestore(id, srcPath, destPath, restoreMode)
	case "verify":
		cmdVerify(id)
	}
}
