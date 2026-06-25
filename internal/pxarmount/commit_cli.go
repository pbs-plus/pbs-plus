package pxarmount

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
)

func RunCommitSubcommand() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")
	pbsURL := fs.String("pbs-url", "", "PBS server URL")
	datastoreName := fs.String("datastore", "", "PBS datastore name")
	authToken := fs.String("token", "", "PBS API token")
	namespace := fs.String("ns", "", "PBS namespace")
	backupType := fs.String("backup-type", "host", "Backup type")
	backupID := fs.String("backup-id", "", "Backup ID")
	detach := fs.Bool("detach", false, "Run commit in background; use 'attach' to watch progress")

	if err := fs.Parse(os.Args[2:]); err != nil {
		log.Error(err, "")
	} //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	tok := *authToken
	if tok == "" {
		tok = token.ReadLocal()
	}

	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n",
		*pbsURL, *datastoreName, tok, *namespace, *backupType, *backupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error sending command: %v\n", err)
		os.Exit(1)
	}

	if *detach {
		if _, err := fmt.Fprintln(conn, "DETACH"); err != nil {
			fmt.Fprintf(os.Stderr, "  \u2717 error sending detach: %v\n", err)
			os.Exit(1)
		}

		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			fmt.Fprintf(os.Stderr, "  \u2717 error: no response from daemon\n")
			os.Exit(1)
		}
		resp := scanner.Text()
		if after, ok := strings.CutPrefix(resp, "ERR "); ok {
			fmt.Fprintf(os.Stderr, "  \u2717 %s\n", after)
			os.Exit(1)
		}
		if after, ok := strings.CutPrefix(resp, "JOB "); ok {
			jobID := after
			fmt.Fprintf(os.Stderr, "  Commit started in background (job %s)\n", jobID)
			fmt.Fprintf(os.Stderr, "  Use 'pxar-mount attach --socket %s' to watch progress\n", *socketPath)
			return
		}
		fmt.Fprintf(os.Stderr, "  \u2717 unexpected response: %s\n", resp)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Committing snapshot...\n")

	go func() {
		<-sigCh
		display.stop()
		if _, err := fmt.Fprintln(conn, "DETACH"); err != nil {
			log.Error(err, "")
		}
		fmt.Fprintf(os.Stderr, "\r  ↗ Commit detached  -  use 'pxar-mount attach --socket %s' to watch\n", *socketPath)
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if processMonitorLine(line, display) {
			continue
		}
		return
	}
	fmt.Fprintf(os.Stderr, "  \u2717 error: no response from daemon\n")
	os.Exit(1)
}

func RunAttachSubcommand() {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")

	if err := fs.Parse(os.Args[2:]); err != nil {
		log.Error(err, "")
	} //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount attach --socket <path>\n\n")
		fmt.Fprintf(os.Stderr, "Connects to a running background commit and streams progress.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	monPath := *socketPath + ".monitor"
	conn, err := net.DialTimeout("unix", monPath, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		os.Exit(1)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		return
	}
	first := scanner.Text()
	if first == "IDLE" {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		fmt.Fprintf(os.Stderr, "  Use 'pxar-mount logs --socket %s' to view the last commit.\n", *socketPath)
		return
	}

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Attached to commit progress...\n")

	for processMonitorLine(first, display) {
		if !scanner.Scan() {
			break
		}
	}
}

func RunLogsSubcommand() {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")

	if err := fs.Parse(os.Args[2:]); err != nil {
		log.Error(err, "")
	} //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount logs --socket <path>\n\n")
		fmt.Fprintf(os.Stderr, "Shows the output of the last commit.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	logPath := *socketPath + ".log"
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "  No commit logs available.\n")
		} else {
			fmt.Fprintf(os.Stderr, "  \u2717 error reading log file %s: %v\n", logPath, err)
		}
		os.Exit(1)
	}

	if len(data) == 0 {
		fmt.Fprintf(os.Stderr, "  No commit logs available.\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "  --- last commit ---\n")
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	display := NewProgressDisplay(os.Stderr)
	for scanner.Scan() {
		processMonitorLine(scanner.Text(), display)
	}
}

func processMonitorLine(line string, display *ProgressDisplay) bool {
	if strings.HasPrefix(line, "PROGRESS ") {
		display.Update(line)
		return true
	}
	if strings.HasPrefix(line, "OK ") {
		display.Done(line)
		return false
	}
	if strings.HasPrefix(line, "ERR ") {
		display.Error(line)
		return false
	}
	fmt.Fprintf(os.Stderr, "  %s\n", line)
	return true
}
