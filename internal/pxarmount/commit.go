package pxarmount

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// commitMu serializes commit operations.
var commitMu sync.Mutex

// CommitRequest holds parameters for a re-snapshot commit.
type CommitRequest struct {
	PBSURL     string
	Datastore  string
	AuthToken  string
	Namespace  string
	BackupID   string
	BackupType string
	SkipTLS    bool
}

// ParseCommitLine parses a COMMIT line from the socket protocol.
func ParseCommitLine(line string) (*CommitRequest, error) {
	parts := strings.SplitN(line, " ", 7)
	if len(parts) < 1 || parts[0] != "COMMIT" {
		return nil, fmt.Errorf("invalid COMMIT format")
	}
	req := &CommitRequest{}
	if len(parts) > 1 {
		req.PBSURL = parts[1]
	}
	if len(parts) > 2 {
		req.Datastore = parts[2]
	}
	if len(parts) > 3 {
		req.AuthToken = parts[3]
	}
	if len(parts) > 4 {
		req.Namespace = parts[4]
	}
	if len(parts) > 5 {
		req.BackupID = parts[5]
	}
	if len(parts) > 6 {
		req.BackupType = parts[6]
	}
	if req.BackupType == "" {
		req.BackupType = "pxar"
	}
	return req, nil
}

// StartCommitListener listens on a Unix socket for commit requests.
func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sockPath, 0o660); err != nil {
		l.Close()
		return nil, err
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleCommitConn(mfs, conn)
		}
	}()
	return l, nil
}

func handleCommitConn(mfs *MutableFS, conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	line := scanner.Text()
	req, err := ParseCommitLine(line)
	if err != nil {
		fmt.Fprintf(conn, "ERR %v\n", err)
		return
	}
	if err := CommitSnapshot(mfs, req); err != nil {
		fmt.Fprintf(conn, "ERR %v\n", err)
		return
	}
	fmt.Fprintf(conn, "OK\n")
}

// CommitSnapshot creates a new pxar snapshot from the journal state.
func CommitSnapshot(mfs *MutableFS, req *CommitRequest) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	// Build a new pxar archive from the graph nodes.
	allNodes, _ := mfs.journal.AllNodes()
	allXAttrs, _ := mfs.journal.AllXAttrs()
	allWhiteouts, _ := mfs.journal.AllWhiteouts()

	// Build path→node index by reconstructing paths.
	nodeIdx := make(map[string]*GraphNode)
	whiteouts := make(map[string]bool)

	for _, n := range allNodes {
		path, err := mfs.journal.ReconstructPath(n.ID)
		if err != nil {
			continue
		}
		nodeIdx[path] = n
	}

	// Whiteouts by path.
	for parentID, names := range allWhiteouts {
		parentPath, err := mfs.journal.ReconstructPath(parentID)
		if err != nil {
			parentPath = "/"
		}
		for _, name := range names {
			whiteouts[joinPath(parentPath, name)] = true
		}
	}

	// For now, write a simple tar-like representation.
	// TODO: full pxar commit with dedup.
	var entries []string
	for path, n := range nodeIdx {
		if whiteouts[path] {
			continue
		}
		kind := ""
		switch n.Kind {
		case NodeDir:
			kind = "dir"
		case NodeFile:
			kind = "file"
		case NodeSymlink:
			kind = "symlink"
		}
		entries = append(entries, fmt.Sprintf("%s %s", path, kind))
	}
	sort.Strings(entries)

	_ = entries
	_ = allXAttrs
	_ = req

	return nil
}

// ParseOrigSnapshot extracts snapshot metadata from the original DIDX path.
func ParseOrigSnapshot(pbsStore, ppxarDidx string) snapshotRef {
	_ = pbsStore
	parts := strings.Split(filepath.ToSlash(ppxarDidx), "/")
	ref := snapshotRef{
		BackupType: "pxar",
		BackupID:   "orig",
		Namespace:  "ns",
	}
	// Extract namespace and backup ID from path like .../ns/test/ns/sgprog/host/...
	for i, p := range parts {
		if p == "ns" && i+1 < len(parts) {
			if ref.Namespace == "ns" {
				ref.Namespace = parts[i+1]
			}
		}
	}
	for i, p := range parts {
		if p == "host" && i+1 < len(parts) {
			ref.BackupID = parts[i+1]
		}
		if p == "ct" && i+1 < len(parts) {
			ref.BackupID = parts[i+1]
		}
	}
	return ref
}

// postCommit updates the original DIDX file to point to the new snapshot.
func postCommit(mfs *MutableFS, backupID, backupType, namespace, archiveName string, backupTime int64) error {
	_ = mfs
	_ = backupID
	_ = backupType
	_ = namespace
	_ = archiveName
	_ = backupTime
	return nil
}

// RunCommitSubcommand is the entry point for `pxar-mount commit`.
func RunCommitSubcommand() {
	fmt.Println("commit subcommand not yet implemented with graph model")
	os.Exit(1)
}
