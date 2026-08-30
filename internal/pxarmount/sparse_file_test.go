package pxarmount

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
)

type sparseCaptureWriter struct {
	trackingWriter
	data []byte
}

func (w *sparseCaptureWriter) WriteEntryReader(_ *pxar.Entry, reader io.Reader, _ uint64) error {
	data, err := io.ReadAll(reader)
	w.data = data
	return err
}

func TestSparseCopyUpDefersSourceReadAndFeedsCommit(t *testing.T) {
	store, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, store, metaPath, payloadPath)
	root := t.TempDir()
	journalPath := filepath.Join(root, "journal")
	backingPath := filepath.Join(root, "overlay")
	journal, err := OpenJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	mfs := NewMutableFS(pxarFS, journal, backingPath)
	path := "/file_root.txt"
	resolved, status := mfs.resolve(path)
	if status != fuse.OK {
		t.Fatalf("resolve: %s", status)
	}
	if err := mfs.copyUp(resolved); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(mfs.mutablePath(path))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, make([]byte, len("root file content"))) {
		t.Fatalf("copy-up read source payload: %q", raw)
	}
	resolved, status = mfs.resolve(path)
	if status != fuse.OK || !resolved.Node.SparseData || resolved.Node.LowerSize != uint64(len(raw)) {
		t.Fatalf("sparse node = %#v, status = %s", resolved.Node, status)
	}
	fd, err := syscall.Open(mfs.mutablePath(path), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh := mfs.registerFh(mfs.newFh(fd, path, resolved.Inode, resolved))
	written, status := mfs.Write(nil, &fuse.WriteIn{
		NodeId: resolved.Inode,
		Fh:     fh,
		Offset: 5,
	}, []byte("PATCH"))
	if status != fuse.OK || written != 5 {
		t.Fatalf("write = %d, %s", written, status)
	}
	if status := mfs.Flush(nil, &fuse.FlushIn{NodeId: resolved.Inode, Fh: fh}); status != fuse.OK {
		t.Fatalf("flush: %s", status)
	}
	readBuffer := make([]byte, len(raw))
	readResult, status := mfs.Read(nil, &fuse.ReadIn{
		NodeId: resolved.Inode,
		Fh:     fh,
	}, readBuffer)
	if status != fuse.OK {
		t.Fatalf("read: %s", status)
	}
	readData, status := readResult.Bytes(readBuffer)
	readResult.Done()
	if status != fuse.OK {
		t.Fatalf("read result: %s", status)
	}
	want := []byte("root PATCHcontent")
	if !bytes.Equal(readData, want) {
		t.Fatalf("mounted content = %q, want %q", readData, want)
	}
	writer := &sparseCaptureWriter{}
	walk := &commitWalkState{
		mfs:          mfs,
		writer:       writer,
		backedHashes: make(map[string]uint64),
	}
	if err := walk.writeBackedFile("file_root.txt", path, pxar.FileMetadata(0o644).Build()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(writer.data, want) {
		t.Fatalf("commit content = %q, want %q", writer.data, want)
	}
	raw, err = os.ReadFile(mfs.mutablePath(path))
	if err != nil {
		t.Fatal(err)
	}
	wantRaw := make([]byte, len(raw))
	copy(wantRaw[5:], "PATCH")
	if !bytes.Equal(raw, wantRaw) {
		t.Fatalf("overlay contains copied lower data: %q", raw)
	}
	if err := journal.Sync(); err != nil {
		t.Fatal(err)
	}
	mfs.Close()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	mfs = NewMutableFS(pxarFS, journal, backingPath)
	defer mfs.Close()
	reader, _, err := mfs.openBackedFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	restartedData, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restartedData, want) {
		t.Fatalf("restarted content = %q, want %q", restartedData, want)
	}
}

func TestSparseTruncateRegrowDoesNotRevealLowerTail(t *testing.T) {
	store, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, store, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)
	path := "/file_root.txt"
	resolved, status := mfs.resolve(path)
	if status != fuse.OK {
		t.Fatalf("resolve: %s", status)
	}
	setSize := func(size uint64) {
		t.Helper()
		status := mfs.SetAttr(nil, &fuse.SetAttrIn{
			NodeId: resolved.Inode,
			Valid:  fuse.FATTR_SIZE,
			Size:   size}, &fuse.AttrOut{})
		if status != fuse.OK {
			t.Fatalf("truncate to %d: %s", size, status)
		}
	}
	setSize(4)
	setSize(uint64(len("root file content")))
	reader, _, err := mfs.openBackedFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, len("root file content"))
	copy(want, "root")
	if !bytes.Equal(data, want) {
		t.Fatalf("truncate/regrow content = %q, want %q", data, want)
	}
}

func TestDataExtentsMergeAndTrim(t *testing.T) {
	extents := addDataExtent(nil, 20, 30)
	extents = addDataExtent(extents, 0, 10)
	extents = addDataExtent(extents, 10, 20)
	extents = addDataExtent(extents, 40, 50)
	want := []dataExtent{{Start: 0, End: 30}, {Start: 40, End: 50}}
	if len(extents) != len(want) || extents[0] != want[0] || extents[1] != want[1] {
		t.Fatalf("merged extents = %#v, want %#v", extents, want)
	}
	extents = trimDataExtents(extents, 45)
	want[1].End = 45
	if len(extents) != len(want) || extents[0] != want[0] || extents[1] != want[1] {
		t.Fatalf("trimmed extents = %#v, want %#v", extents, want)
	}
}

func TestSparseNodeCodecBackwardCompatibility(t *testing.T) {
	node := &GraphNode{
		ID:          7,
		Kind:        NodeFile,
		HasData:     true,
		SparseData:  true,
		LowerSize:   1024,
		DataExtents: []dataExtent{{Start: 4, End: 12}, {Start: 20, End: 32}},
	}
	decoded := decodeNode(encodeNode(node), node.ID)
	if !decoded.SparseData || decoded.LowerSize != node.LowerSize || len(decoded.DataExtents) != 2 || decoded.DataExtents[1] != node.DataExtents[1] {
		t.Fatalf("decoded sparse node = %#v", decoded)
	}
	legacy := encodeNode(&GraphNode{Kind: NodeFile, HasData: true})
	legacy = legacy[:len(legacy)-13]
	decoded = decodeNode(legacy, node.ID)
	if decoded.SparseData || len(decoded.DataExtents) != 0 {
		t.Fatalf("legacy node decoded as sparse: %#v", decoded)
	}
}
