//go:build linux

package verification

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	sha256simd "github.com/minio/sha256-simd"

	"github.com/pbs-plus/pbs-plus/internal/agent/verification"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

type readerFunc func([]byte) (int, error)

func (rf readerFunc) Read(p []byte) (int, error) { return rf(p) }

type fileEntry struct {
	Path         string
	ContentStart uint64
	ContentEnd   uint64
	Size         int64
}

type verifyState struct {
	fs *vfs.LocalFS
}

func (vs *verifyState) Close() error {
	if vs.fs != nil {
		return vs.fs.Close()
	}
	return nil
}

// openArchive opens the pxar archive for the given snapshot, returning a
// verifyState that can be used for both file enumeration and content extraction.
func (v *verificationJob) openArchive(backup coredb.Backup, snap *snapshotInfo) (*verifyState, error) {
	dsInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore info: %w", err)
	}

	t := time.Unix(snap.BackupTime, 0).UTC()
	snapshotTime := t.Format(time.RFC3339)

	var fileName string
	for _, f := range snap.Files {
		if strings.HasSuffix(f, ".mpxar.didx") || strings.HasSuffix(f, ".pxar.didx") {
			fileName = f
			break
		}
	}
	if fileName == "" {
		return nil, fmt.Errorf("no pxar archive found in snapshot")
	}

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path,
		backup.Namespace,
		snap.BackupType,
		snap.BackupID,
		snapshotTime,
		fileName,
	)
	if err != nil {
		return nil, err
	}

	chunkStore, err := datastore.NewChunkStore(dsInfo.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk store: %w", err)
	}
	chunkSource := datastore.NewChunkStoreSource(chunkStore)

	var archiveReader transfer.ArchiveReader

	if isSplit {
		metaData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata index: %w", err)
		}
		payloadData, err := os.ReadFile(ppxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload index: %w", err)
		}

		splitReader, err := transfer.NewSplitReader(metaData, payloadData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create split reader: %w", err)
		}
		archiveReader = splitReader
	} else {
		idxData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read index: %w", err)
		}
		reader, err := transfer.NewChunkedReader(idxData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunked reader: %w", err)
		}
		archiveReader = reader
	}

	fs := vfs.NewLocalFS(archiveReader)

	return &verifyState{fs: fs}, nil
}

// sampleFiles walks the pxar archive to enumerate files, then returns a sample
// based on the configured strategy (random, systematic, or stratified).

// verifyFile verifies a single file by comparing the agent's SHA-256 hash
// of the live file against the hash of the same file extracted from the
// stored pxar archive.
func (v *verificationJob) verifyFile(
	ctx context.Context,
	agentTCP *arpc.StreamPipe,
	vs *verifyState,
	file fileEntry,
	backup coredb.Backup,
) coredb.VerificationFileResult {
	result := coredb.VerificationFileResult{
		Path:   file.Path,
		Size:   file.Size,
		Status: "error",
	}

	rootPath := backup.Target.GetAgentHostPath()
	relPath := strings.TrimPrefix(file.Path, "/")
	if backup.Subpath != "" {
		subpath := strings.TrimPrefix(backup.Subpath, "/")
		relPath = filepath.Join(subpath, relPath)
	}
	agentPath := filepath.Join(rootPath, relPath)

	// Run agent hash and archive hash concurrently
	type agentResult struct {
		resp verification.VerifyFileResp
		err  error
	}
	type archiveResult struct {
		hash [32]byte
		err  error
	}

	agentCh := make(chan agentResult, 1)
	archiveCh := make(chan archiveResult, 1)

	go func() {
		var resp verification.VerifyFileResp
		fileCtx, fileCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer fileCancel()
		err := agentTCP.Call(fileCtx, "verify_chunk_file", verification.VerifyFileReq{FilePath: agentPath}, &resp)
		agentCh <- agentResult{resp: resp, err: err}
	}()

	go func() {
		hash, err := extractFileHash(ctx, vs, file)
		archiveCh <- archiveResult{hash: hash, err: err}
	}()

	agentRes := <-agentCh
	if agentRes.err != nil {
		result.Status = "skipped"
		result.Message = fmt.Sprintf("agent call failed: %v", agentRes.err)
		<-archiveCh // drain
		return result
	}
	if agentRes.resp.Error != "" {
		result.Status = "skipped"
		result.Message = agentRes.resp.Error
		<-archiveCh // drain
		return result
	}

	archiveRes := <-archiveCh
	if archiveRes.err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("failed to extract file from archive: %v", archiveRes.err)
		return result
	}

	// Compare hashes
	if agentRes.resp.SHA256 != archiveRes.hash {
		result.Status = "failed"
		result.Message = fmt.Sprintf("SHA-256 mismatch: agent=%x, archive=%x", agentRes.resp.SHA256, archiveRes.hash)
		return result
	}

	result.Status = "ok"
	result.Message = "verified"
	return result
}

// extractFileHash reads a file's content from the pxar archive and returns
// its SHA-256 hash. It respects context cancellation.

// extractFileHash reads a file's content from the pxar archive and returns
// its SHA-256 hash. It respects context cancellation.
func extractFileHash(ctx context.Context, vs *verifyState, file fileEntry) ([32]byte, error) {
	rc, err := vs.fs.ReadContentReader(file.ContentStart, file.ContentEnd)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to open content reader for [%d, %d): %w", file.ContentStart, file.ContentEnd, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	h := sha256simd.New()
	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)

	_, err = io.CopyBuffer(h, readerFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return rc.Read(p)
		}
	}), *bufp)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to read file content: %w", err)
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
