package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/pbstoken"
)

func main() {
	pbsURL := flag.String("pbs-url", pbstoken.DefaultAPIURL, "PBS API URL")
	datastore := flag.String("datastore", "", "PBS datastore name (required for remote mode)")
	namespace := flag.String("namespace", "", "PBS namespace")
	authToken := flag.String("auth-token", "", "PBS API token in tokenid:value form (defaults to /var/lib/proxmox-backup/pbs-plus-token.json)")
	localDir := flag.String("local-store", "", "Local store directory (offline mode; no PBS upload)")
	backupID := flag.String("backup-id", "", "Backup ID (derived from BKF machine name if empty)")
	archiveName := flag.String("archive-name", "", "Archive name (defaults to backup-id)")
	tapeDevice := flag.String("tape", "", "Tape device path (e.g. /dev/nst0)")
	changerDevice := flag.String("changer", "", "SCSI changer device (e.g. /dev/sg1) for robotic auto-load; requires -tape")
	driveIndex := flag.Int("drive", 0, "Target drive index within the changer (0-based; almost always 0)")
	verbose := flag.Bool("v", false, "Verbose output")
	compress := flag.Bool("compress", false, "Enable zstd compression for chunks (off by default; useful for remote PBS, wasteful for localhost)")
	spanning := flag.Bool("spanning", false, "Enable media spanning for multi-tape sets")
	listMode := flag.Bool("list", false, "List snapshots (backup sets) in the input and exit")
	snapshotSel := flag.Int("snapshot", -1, "Migrate only snapshot N (0-based; use -list to see available)")
	skipTLS := flag.Bool("skip-tls-verify", true, "Skip TLS certificate verification")
	flag.Usage = usage
	flag.Parse()

	if *datastore == "" && *localDir == "" && !*listMode {
		die("-datastore, -local-store, or -list is required")
	}
	if *changerDevice != "" && *tapeDevice == "" {
		die("-changer requires -tape (the tape drive device, e.g. /dev/nst0)")
	}
	if len(flag.Args()) == 0 && *tapeDevice == "" {
		die("at least one BKF path or -tape device is required")
	}

	cfg := bkf2pxar.Config{
		PBSURL:        *pbsURL,
		Datastore:     *datastore,
		Namespace:     *namespace,
		AuthToken:     *authToken,
		SkipTLS:       *skipTLS,
		BackupID:      *backupID,
		ArchiveName:   *archiveName,
		LocalDir:      *localDir,
		TapeDevice:    *tapeDevice,
		ChangerDevice: *changerDevice,
		DriveIndex:    *driveIndex,
		Sources:       flag.Args(),
		Verbose:       *verbose,
		Compress:      *compress,
		Spanning:      *spanning,
		SnapshotSel:   *snapshotSel,
	}

	if *listMode {
		snapshots, err := bkf2pxar.ListSnapshots(context.Background(), cfg)
		printSnapshots(snapshots)
		if err != nil {
			log.Fatalf("list failed: %v", err)
		}
		return
	}

	if profPath := os.Getenv("BKF2PXAR_CPUPROFILE"); profPath != "" {
		f, err := os.Create(profPath)
		if err != nil {
			log.Fatalf("cpu profile: %v", err)
		}
		_ = pprof.StartCPUProfile(f)
		// Flush the profile on SIGINT/SIGTERM so `timeout` (which sends SIGTERM)
		// produces a usable file; Go's default handlers exit without defers.
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sig
			pprof.StopCPUProfile()
			_ = f.Close()
			os.Exit(0)
		}()
	}

	stats, err := bkf2pxar.Run(context.Background(), cfg)
	if err != nil {
		log.Fatalf("conversion failed: %v", err)
	}

	dur := time.Since(stats.StartTime).Round(time.Second)
	if stats.Snapshots > 1 {
		fmt.Fprintf(os.Stderr, "Done: %d snapshots → %d files, %d dirs, %s in %s\n",
			stats.Snapshots, stats.Files, stats.Dirs,
			humanBytes(stats.Bytes), dur)
	} else {
		fmt.Fprintf(os.Stderr, "Done: host=%s backup-id=%s → %d files, %d dirs, %s in %s\n",
			stats.Host, stats.BackupID, stats.Files, stats.Dirs,
			humanBytes(stats.Bytes), dur)
	}
}

func printSnapshots(snapshots []bkf2pxar.Snapshot) {
	if len(snapshots) == 0 {
		fmt.Fprintln(os.Stderr, "No snapshots found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tSOURCE\tMACHINE\tVOLUME\tBACKUP TIME\tOWNER") //nolint:errcheck

	for _, s := range snapshots {
		vol := s.VolumeName
		if vol == "" {
			vol = "-"
		}
		timeStr := "-"
		if !s.BackupTime.IsZero() {
			timeStr = s.BackupTime.Format("2006-01-02 15:04 MST")
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\t%s\t%s\n", //nolint:errcheck
			s.Index, s.SourceFile, s.MachineName, vol, timeStr, s.Owner)
	}
	_ = w.Flush()

	for _, s := range snapshots {
		if s.Truncated {
			fmt.Fprintf(os.Stderr, "WARNING: %s (snapshot %d) spans multiple media  -  data is incomplete. Re-run with -spanning and all tapes/files.\n", s.SourceFile, s.Index)
		}
	}
	fmt.Fprintf(os.Stderr, "\n%d snapshot(s). Use -snapshot N to migrate a specific one.\n", len(snapshots))
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage of bkf2pxar:
Usage: bkf2pxar [flags] <path...>

Converts Backup Exec .bkf files (or LTO tapes) into pxar archives and uploads
to Proxmox Backup Server via the PBS backup protocol.

Backup ID and time are derived from the BKF metadata (machine name + SSET
create time). The volume root directory is flattened to the pxar root.

Each SSET (backup set) becomes its own PBS backup point. Use -list to see
available snapshots, and -snapshot N to migrate only one.

Input modes:
  - .bkf files or directories of .bkf files (positional args)
  - a single tape drive:  -tape /dev/nst0
  - a robotic tape library: -changer /dev/sg1 -tape /dev/nst0
    (loads/unloads tapes automatically; follows media-set spanning)

Use -changer with a SCSI Medium Changer device to migrate a whole magazine
unattended. Cleaning tapes (CLN*/CCL* barcodes) are skipped automatically.

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
