package pxarmount

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

func createFlatTestArchive(t *testing.T, fileCount int) (string, string, string) {
	t.Helper()
	dir := t.TempDir()

	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}

	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "flat",
	})
	if err != nil {
		t.Fatal(err)
	}

	writer := transfer.NewSessionWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")

	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	fileMeta := pxar.FileMetadata(0o644).Build()
	for i := range fileCount {
		content := []byte(fmt.Sprintf("content of file %04d", i))
		if err := writer.WriteEntry(&pxar.Entry{
			Path:     fmt.Sprintf("file_%04d.txt", i),
			Kind:     pxar.KindFile,
			Metadata: fileMeta,
			FileSize: uint64(len(content)),
		}, content); err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}

	return dir, filepath.Join(dir, "root.mpxar.didx"), filepath.Join(dir, "root.ppxar.didx")
}

// TestReadDirFullReadsInOffsetOrder pins ascending reads: goodbye tables are hash-ordered, so default traversal seeks randomly.
func TestReadDirFullReadsInOffsetOrder(t *testing.T) {
	storeDir, metaPath, payloadPath := createFlatTestArchive(t, 200)
	pxarFS := openTestArchive(t, storeDir, metaPath, payloadPath)

	entries, err := pxarFS.ReadDirFull(RootInode, nil)
	if err != nil {
		t.Fatalf("ReadDirFull: %v", err)
	}
	if len(entries) != 200 {
		t.Fatalf("got %d entries, want 200", len(entries))
	}

	backwards := 0
	for i := 1; i < len(entries); i++ {
		if entries[i].entryStart < entries[i-1].entryStart {
			backwards++
		}
	}
	if backwards != 0 {
		t.Fatalf("%d of %d entries were read out of offset order", backwards, len(entries)-1)
	}
}

func TestReadDirFullEntryCacheCoversAllFiles(t *testing.T) {
	storeDir, metaPath, payloadPath := createFlatTestArchive(t, 64)
	pxarFS := openTestArchive(t, storeDir, metaPath, payloadPath)

	cache := make(map[uint64]*pxar.Entry)
	entries, err := pxarFS.ReadDirFull(RootInode, cache)
	if err != nil {
		t.Fatalf("ReadDirFull: %v", err)
	}
	for _, e := range entries {
		cached, ok := cache[e.entryStart]
		if !ok {
			t.Fatalf("%s: entry not cached at offset %d", e.name, e.entryStart)
		}
		if cached.FileName() != e.name {
			t.Fatalf("offset %d: cached %q, want %q", e.entryStart, cached.FileName(), e.name)
		}
	}
}
