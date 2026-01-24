//go:build linux

package pxar

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type PxarReader struct {
	conn     net.Conn
	mu       sync.Mutex
	enc      cbor.EncMode
	dec      cbor.DecMode
	cmd      *exec.Cmd
	task     *tasks.RestoreTask
	loggerCh chan string

	closed atomic.Bool

	FileCount   int64
	FolderCount int64
	TotalBytes  int64

	lastAccessTime  int64
	lastBytesTime   int64
	lastFileCount   int64
	lastFolderCount int64
	lastTotalBytes  int64
}

type PxarReaderStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}

func (r *PxarReader) GetStats() PxarReaderStats {
	// Get the current time in nanoseconds.
	currentTime := time.Now().UnixNano()

	// Atomically load the current counters.
	currentFileCount := atomic.LoadInt64(&r.FileCount)
	currentFolderCount := atomic.LoadInt64(&r.FolderCount)
	totalAccessed := currentFileCount + currentFolderCount

	// Swap out the previous access statistics.
	lastATime := atomic.SwapInt64(&r.lastAccessTime, currentTime)
	lastFileCount := atomic.SwapInt64(&r.lastFileCount, currentFileCount)
	lastFolderCount := atomic.SwapInt64(&r.lastFolderCount, currentFolderCount)

	// Calculate the elapsed time in seconds.
	elapsed := float64(currentTime-lastATime) / 1e9
	var accessSpeed float64
	if elapsed > 0 {
		accessDelta := (currentFileCount + currentFolderCount) - (lastFileCount + lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	// Similarly, for byte counters (if you're tracking totalBytes elsewhere).
	currentTotalBytes := atomic.LoadInt64(&r.TotalBytes)
	lastBTime := atomic.SwapInt64(&r.lastBytesTime, currentTime)
	lastTotalBytes := atomic.SwapInt64(&r.lastTotalBytes, currentTotalBytes)

	secDiff := float64(currentTime-lastBTime) / 1e9
	var bytesSpeed float64
	if secDiff > 0 {
		bytesSpeed = float64(currentTotalBytes-lastTotalBytes) / secDiff
	}

	return PxarReaderStats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
	}
}

func NewPxarReader(ctx context.Context, socketPath, pbsStore, namespace, snapshot string, proxmoxTask *tasks.RestoreTask) (*PxarReader, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(pbsStore)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore: %w", err)
	}

	snapSplit := strings.Split(snapshot, "/")
	if len(snapSplit) != 3 {
		return nil, fmt.Errorf("invalid snapshot string (expected type/id/time): %s", snapshot)
	}

	backupType := snapSplit[0]
	snapshotId := snapSplit[1]
	timestampRaw := snapSplit[2]

	unixTime, err := strconv.ParseInt(timestampRaw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid unix timestamp in snapshot: %w", err)
	}

	t := time.Unix(unixTime, 0).UTC()
	snapshotTime := t.Format(time.RFC3339)

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path,
		namespace,
		backupType,
		snapshotId,
		snapshotTime,
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build pxar paths: %w", err)
	}

	if !isSplit {
		return nil, fmt.Errorf(".pxar.didx found, only split archives are supported for now")
	}

	cmd, err := runSocket(ctx, socketPath, dsInfo.Path, mpxarPath, ppxarPath, "")
	if err != nil {
		return nil, fmt.Errorf("failed to serve socket: %w", err)
	}

	exp := time.NewTimer(5 * time.Second)
	interval := time.NewTicker(500 * time.Millisecond)
	defer interval.Stop()

	var conn net.Conn
	for {
		var err error
		select {
		case <-exp.C:
			return nil, fmt.Errorf("failed to connect to socket: %w", err)
		case <-interval.C:
		}
		conn, err = net.Dial("unix", socketPath)
		if err == nil && conn != nil {
			break
		}
	}

	encMode, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR encoder: %w", err)
	}

	decMode, err := cbor.DecOptions{}.DecMode()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR decoder: %w", err)
	}

	loggerCh := make(chan string, 100)

	reader := &PxarReader{
		conn:     conn,
		enc:      encMode,
		dec:      decMode,
		cmd:      cmd,
		task:     proxmoxTask,
		loggerCh: loggerCh,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case log, ok := <-loggerCh:
				if !ok {
					return
				}
				proxmoxTask.WriteString(log)
			}
		}
	}()

	return reader, nil
}

func runSocket(ctx context.Context, socketPath, pbsStore, mpxarPath, ppxarPath, keyFile string) (*exec.Cmd, error) {
	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0755); err != nil {
		syslog.L.Error(err).WithMessage("pxar-socket: failed to create socket directory").Write()
		return nil, err
	}

	args := []string{
		"--socket", socketPath,
		"--pbs-store", pbsStore,
		"--mpxar-didx", mpxarPath,
		"--ppxar-didx", ppxarPath,
	}

	if keyFile != "" {
		args = append(args, "--keyfile", keyFile)
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/pxar-socket-api", args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("pxar-socket: StdoutPipe failed").Write()
		return nil, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("pxar-socket: StderrPipe failed").Write()
		return nil, err
	}

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	go func() {
		for scanner.Scan() {
			syslog.L.Info().
				WithMessage(scanner.Text()).Write()
		}
		if err := scanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("pxar-socket: stdout scanner error").WithField("error", err.Error()).Write()
		}
	}()

	go func() {
		for errScanner.Scan() {
			syslog.L.Error(errors.New(errScanner.Text())).
				Write()
		}
		if err := errScanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("pxar-socket: stderr scanner error").WithField("error", err.Error()).Write()
		}
	}()

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).WithMessage("pxar-socket: cmd.Start failed").Write()
		return nil, err
	}
	syslog.L.Info().WithMessage("pxar-socket: child started").
		WithField("pid", cmd.Process.Pid).
		WithField("args", strings.Join(args, " ")).
		Write()

	return cmd, nil
}

func (c *PxarReader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}

	if c.loggerCh != nil {
		close(c.loggerCh)
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	if c.cmd != nil {
		c.cmd.Cancel()
	}
	return nil
}

func (c *PxarReader) sendRequest(reqVariant string, reqData any) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Rust expects externally tagged enum: {"Variant": payload}
	// Even for no-arg variants, use {"GetRoot": nil}
	reqMap := map[string]any{reqVariant: reqData}

	reqBytes, err := c.enc.Marshal(reqMap)
	if err != nil {
		return nil, err
	}

	length := uint32(len(reqBytes))
	if err := binary.Write(c.conn, binary.LittleEndian, length); err != nil {
		return nil, err
	}

	if _, err := c.conn.Write(reqBytes); err != nil {
		return nil, err
	}

	var respLength uint32
	if err := binary.Read(c.conn, binary.LittleEndian, &respLength); err != nil {
		return nil, err
	}

	respData := make([]byte, respLength)
	if _, err := io.ReadFull(c.conn, respData); err != nil {
		return nil, err
	}

	var resp Response
	if err := c.dec.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}

	if errData, ok := resp["Error"]; ok {
		if errMap, ok := errData.(map[any]any); ok {
			if errno, ok := errMap["errno"].(int64); ok {
				return nil, syscall.Errno(errno)
			}
		}
		return nil, fmt.Errorf("pxar-socket error")
	}

	return resp, nil
}

func (c *PxarReader) GetRoot() (*EntryInfo, error) {
	c.task.WriteString("get root of source")

	resp, err := c.sendRequest("GetRoot", nil)
	if err != nil {
		return nil, err
	}

	entryData, ok := resp["Entry"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	entryMap, ok := entryData.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid entry data")
	}

	infoData, ok := entryMap["info"]
	if !ok {
		return nil, fmt.Errorf("missing info field")
	}

	infoBytes, err := c.enc.Marshal(infoData)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode info: %w", err)
	}

	var info EntryInfo
	if err := c.dec.Unmarshal(infoBytes, &info); err != nil {
		return nil, fmt.Errorf("failed to decode info: %w", err)
	}

	return &info, nil
}

func (c *PxarReader) LookupByPath(path string) (*EntryInfo, error) {
	c.task.WriteString(fmt.Sprintf("looking up path: %s", path))

	reqData := map[string]any{
		"path": []byte(path),
	}

	resp, err := c.sendRequest("LookupByPath", reqData)
	if err != nil {
		return nil, err
	}

	return extractEntryInfo(c, resp)
}

func (c *PxarReader) ReadDir(entryEnd uint64) ([]EntryInfo, error) {
	resp, err := c.sendRequest("ReadDir", map[string]any{"entry_end": entryEnd})
	if err != nil {
		return nil, err
	}

	dirData, ok := resp["DirEntries"].(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid response")
	}

	entriesBytes, _ := c.enc.Marshal(dirData["entries"])
	var entries []EntryInfo
	if err := c.dec.Unmarshal(entriesBytes, &entries); err != nil {
		return nil, err
	}

	for i := range entries {
		if entries[i].IsDir() {
			select {
			case c.loggerCh <- fmt.Sprintf("restoring entries of dir: %s", entries[i].Name()):
			default:
			}

			atomic.AddInt64(&c.FolderCount, 1)
		} else {
			atomic.AddInt64(&c.FileCount, 1)
		}
	}
	return entries, nil
}

func (c *PxarReader) GetAttr(entryStart, entryEnd uint64) (*EntryInfo, error) {
	reqData := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}

	resp, err := c.sendRequest("GetAttr", reqData)
	if err != nil {
		return nil, err
	}

	entry, err := extractEntryInfo(c, resp)
	if err != nil {
		return nil, err
	}

	if entry.IsDir() {
		atomic.AddInt64(&c.FolderCount, 1)
	} else {
		atomic.AddInt64(&c.FileCount, 1)
	}

	return entry, nil
}

func (c *PxarReader) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	reqData := map[string]any{
		"content_start": contentStart,
		"content_end":   contentEnd,
		"offset":        offset,
		"size":          size,
	}

	resp, err := c.sendRequest("Read", reqData)
	if err != nil {
		return nil, err
	}

	dataResp, ok := resp["Data"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	dataMap, ok := dataResp.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid data response")
	}

	data, ok := dataMap["data"].([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid data field")
	}

	atomic.AddInt64(&c.TotalBytes, int64(len(data)))

	return data, nil
}

func (c *PxarReader) ReadLink(entryStart, entryEnd uint64) ([]byte, error) {
	reqData := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}

	resp, err := c.sendRequest("ReadLink", reqData)
	if err != nil {
		return nil, err
	}

	symlinkResp, ok := resp["Symlink"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	symlinkMap, ok := symlinkResp.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid symlink response")
	}

	target, ok := symlinkMap["target"].([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid target field")
	}

	return target, nil
}

func (c *PxarReader) ListXAttrs(entryStart, entryEnd uint64) (map[string][]byte, error) {
	reqData := map[string]any{
		"entry_start": entryStart,
		"entry_end":   entryEnd,
	}

	resp, err := c.sendRequest("ListXAttrs", reqData)
	if err != nil {
		return nil, err
	}

	xattrsResp, ok := resp["XAttrs"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	xattrsMap, ok := xattrsResp.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid xattrs response")
	}

	xattrsData, ok := xattrsMap["xattrs"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid xattrs field")
	}

	result := make(map[string][]byte)
	for _, item := range xattrsData {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			continue
		}
		name, ok1 := pair[0].([]byte)
		value, ok2 := pair[1].([]byte)
		if ok1 && ok2 {
			result[string(name)] = value
		}
	}

	return result, nil
}

func extractEntryInfo(c *PxarReader, resp Response) (*EntryInfo, error) {
	entryData, ok := resp["Entry"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	entryMap, ok := entryData.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid entry data")
	}

	infoData, ok := entryMap["info"]
	if !ok {
		return nil, fmt.Errorf("missing info field")
	}

	infoBytes, err := c.enc.Marshal(infoData)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode info: %w", err)
	}

	var info EntryInfo
	if err := c.dec.Unmarshal(infoBytes, &info); err != nil {
		return nil, fmt.Errorf("failed to decode info: %w", err)
	}

	return &info, nil
}
