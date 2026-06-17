package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
)

func main() {
	pbsURL := flag.String("pbs-url", "https://localhost:8007/api2/json", "PBS API URL")
	datastore := flag.String("datastore", "", "PBS datastore name (required for remote mode)")
	namespace := flag.String("namespace", "", "PBS namespace")
	authToken := flag.String("auth-token", "", "PBS API token (reads /etc/pbs-plus/auth-token if empty)")
	localDir := flag.String("local-store", "", "Local store directory (offline mode; no PBS upload)")
	backupID := flag.String("backup-id", "", "Backup ID (derived from BKF machine name if empty)")
	archiveName := flag.String("archive-name", "", "Archive name (defaults to backup-id)")
	tapeDevice := flag.String("tape", "", "Tape device path (e.g. /dev/nst0)")
	verbose := flag.Bool("v", false, "Verbose output")
	spanning := flag.Bool("spanning", false, "Enable media spanning for multi-tape sets")
	skipTLS := flag.Bool("skip-tls-verify", true, "Skip TLS certificate verification")
	flag.Usage = usage
	flag.Parse()

	if *datastore == "" && *localDir == "" {
		die("-datastore or -local-store is required")
	}
	if len(flag.Args()) == 0 && *tapeDevice == "" {
		die("at least one BKF path or -tape device is required")
	}

	stats, err := bkf2pxar.Run(context.Background(), bkf2pxar.Config{
		PBSURL:      *pbsURL,
		Datastore:   *datastore,
		Namespace:   *namespace,
		AuthToken:   *authToken,
		SkipTLS:     *skipTLS,
		BackupID:    *backupID,
		ArchiveName: *archiveName,
		LocalDir:    *localDir,
		TapeDevice:  *tapeDevice,
		Sources:     flag.Args(),
		Verbose:     *verbose,
		Spanning:    *spanning,
	})
	if err != nil {
		log.Fatalf("conversion failed: %v", err)
	}

	dur := time.Since(stats.StartTime).Round(time.Second)
	fmt.Fprintf(os.Stderr, "Done: host=%s backup-id=%s → %d files, %d dirs, %s in %s\n",
		stats.Host, stats.BackupID, stats.Files, stats.Dirs,
		humanBytes(stats.Bytes), dur)
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: bkf2pxar [flags] <path...>

Converts Backup Exec .bkf files (or LTO tapes) into pxar archives and uploads
to Proxmox Backup Server via the PBS backup protocol.

Backup ID and time are derived from the BKF metadata (machine name + SSET
create time). The volume root directory is flattened to the pxar root.

Paths can be .bkf files or directories containing .bkf files.
Use -tape for LTO tape device input.

Flags:
`)
	flag.PrintDefaults()
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	flag.Usage()
	os.Exit(1)
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TB", float64(b)/(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
