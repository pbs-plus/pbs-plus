// Command pxar-mount mounts a PBS pxar archive via FUSE.
// It replaces the Rust-based pxar-direct-mount binary with a pure Go implementation
// using the github.com/pbs-plus/pxar library.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/fusefs"
	"github.com/pbs-plus/pxar/transfer"
)

func main() {
	pbsStore := flag.String("pbs-store", "", "PBS datastore root path")
	mpxarDidx := flag.String("mpxar-didx", "", "Path to metadata dynamic index (.mpxar.didx)")
	ppxarDidx := flag.String("ppxar-didx", "", "Path to payload dynamic index (.ppxar.didx)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *pbsStore == "" || *mpxarDidx == "" || *ppxarDidx == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount --pbs-store <path> --mpxar-didx <path> --ppxar-didx <path> [--verbose] <mountpoint>\n")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}
	mountPoint := args[0]

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: datastore=%s metadata=%s payload=%s mount=%s\n",
			*pbsStore, *mpxarDidx, *ppxarDidx, mountPoint)
	}

	// Open the PBS datastore chunk store
	store, err := datastore.NewChunkStore(*pbsStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening chunk store: %v\n", err)
		os.Exit(1)
	}

	source := datastore.NewChunkStoreSource(store)

	// Read dynamic index files
	metaData, err := os.ReadFile(*mpxarDidx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading metadata index: %v\n", err)
		os.Exit(1)
	}
	payloadData, err := os.ReadFile(*ppxarDidx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading payload index: %v\n", err)
		os.Exit(1)
	}

	// Parse indexes to get sizes
	metaIdx, err := datastore.ReadDynamicIndex(metaData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing metadata index: %v\n", err)
		os.Exit(1)
	}
	payloadIdx, err := datastore.ReadDynamicIndex(payloadData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing payload index: %v\n", err)
		os.Exit(1)
	}

	// Create lazy chunked readers for both metadata and payload streams
	metaReader := transfer.NewChunkedReadSeeker(metaIdx, source, 32)
	payloadReader := transfer.NewChunkedReadSeeker(payloadIdx, source, 64)

	defer metaReader.Close()
	defer payloadReader.Close()

	// Create the pxar accessor session
	// fusefs.Session needs a single io.ReadSeeker, but we have split archive.
	// For the FUSE mount, we reconstruct the full pxar stream eagerly for simplicity.
	_ = metaReader
	_ = payloadReader

	// Alternative: reconstruct full stream into memory
	metaBuf, err := reconstructStream(metaIdx, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reconstructing metadata stream: %v\n", err)
		os.Exit(1)
	}

	session, err := fusefs.NewSession(&seekBuffer{buf: metaBuf}, int64(len(metaBuf)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating pxar session: %v\n", err)
		os.Exit(1)
	}
	defer session.Close()

	// Mount via go-fuse
	rawFS := &pxarRawFS{session: session}
	server, err := fuse.NewServer(rawFS, mountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: []string{"ro", "default_permissions"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: serving at %s\n", mountPoint)
	}

	// Handle signals for clean unmount
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		if *verbose {
			fmt.Fprintf(os.Stderr, "pxar-mount: received signal, unmounting\n")
		}
		_ = server.Unmount()
	}()

	server.Serve()
}

// seekBuffer wraps a byte slice to implement io.ReadSeeker.
type seekBuffer struct {
	buf    []byte
	offset int64
}

func (s *seekBuffer) Read(p []byte) (int, error) {
	if s.offset >= int64(len(s.buf)) {
		return 0, io.EOF
	}
	n := copy(p, s.buf[s.offset:])
	s.offset += int64(n)
	return n, nil
}

func (s *seekBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.offset = offset
	case io.SeekCurrent:
		s.offset += offset
	case io.SeekEnd:
		s.offset = int64(len(s.buf)) + offset
	}
	if s.offset < 0 {
		s.offset = 0
	}
	return s.offset, nil
}

// reconstructStream rebuilds a complete stream from a dynamic index.
func reconstructStream(idx *datastore.DynamicIndexReader, source datastore.ChunkSource) ([]byte, error) {
	restorer := datastore.NewRestorer(source)
	buf := make([]byte, 0, idx.IndexBytes())
	w := &sliceWriter{buf: buf}
	if err := restorer.RestoreFile(idx, w); err != nil {
		return nil, err
	}
	return w.buf, nil
}

type sliceWriter struct {
	buf []byte
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// pxarRawFS adapts fusefs.FileSystem to fuse.RawFileSystem.
type pxarRawFS struct {
	fuse.RawFileSystem
	session *fusefs.Session
}

func (fs *pxarRawFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *pxarRawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	inode, attr, err := fs.session.Lookup(header.NodeId, name)
	if err != nil {
		return fuse.ToStatus(err)
	}

	fillEntryOut(inode, attr, out)
	return fuse.OK
}

func (fs *pxarRawFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	attr, err := fs.session.Getattr(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}

	fillAttrOut(attr, out)
	return fuse.OK
}

func (fs *pxarRawFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *pxarRawFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	entries, err := fs.session.Readdir(input.NodeId, input.Offset)
	if err != nil {
		return fuse.ToStatus(err)
	}

	for _, e := range entries {
		if ok := out.AddDirEntry(fuse.DirEntry{
			Name: e.Name,
			Ino:  e.Ino,
			Mode: e.Mode,
		}); !ok {
			break
		}
	}

	return fuse.OK
}

func (fs *pxarRawFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if err := fs.session.Open(input.NodeId, input.Flags); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *pxarRawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	n, err := fs.session.Read(input.NodeId, buf, int64(input.Offset))
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func (fs *pxarRawFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	target, err := fs.session.Readlink(header.NodeId)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return []byte(target), fuse.OK
}

func (fs *pxarRawFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	names, err := fs.session.ListXAttr(header.NodeId)
	if err != nil {
		return 0, fuse.ToStatus(err)
	}

	// Build the null-separated list
	size := 0
	for _, name := range names {
		size += len(name) + 1
	}

	if dest == nil {
		return uint32(size), fuse.OK
	}

	if uint32(len(dest)) < uint32(size) {
		return 0, fuse.Status(syscall.ERANGE)
	}

	pos := 0
	for _, name := range names {
		copy(dest[pos:], name)
		pos += len(name)
		dest[pos] = 0
		pos++
	}
	return uint32(size), fuse.OK
}

func (fs *pxarRawFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	val, err := fs.session.GetXAttr(header.NodeId, attr)
	if err != nil {
		return 0, fuse.ToStatus(err)
	}

	if dest == nil {
		return uint32(len(val)), fuse.OK
	}

	if uint32(len(dest)) < uint32(len(val)) {
		return 0, fuse.Status(syscall.ERANGE)
	}

	copy(dest, val)
	return uint32(len(val)), fuse.OK
}

func (fs *pxarRawFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	stat, err := fs.session.Statfs()
	if err != nil {
		return fuse.ToStatus(err)
	}
	out.Blocks = stat.Blocks
	out.Bsize = uint32(stat.Bsize)
	out.Bfree = stat.Bfree
	out.Bavail = stat.Bavail
	out.Files = stat.Files
	out.Ffree = stat.Ffree
	out.NameLen = uint32(stat.Namelen)
	out.Frsize = uint32(stat.Frsize)
	return fuse.OK
}

func (fs *pxarRawFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	if err := fs.session.Access(input.NodeId, input.Mask); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *pxarRawFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	fs.session.Release(input.NodeId)
}

func (fs *pxarRawFS) Forget(nodeID, nlookup uint64) {
	fs.session.Forget(nodeID, nlookup)
}

// Helper functions

func fillEntryOut(inode uint64, attr fusefs.Attr, out *fuse.EntryOut) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, attr)
}

func fillAttrOut(attr fusefs.Attr, out *fuse.AttrOut) {
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, attr)
}

func fillAttr(out *fuse.Attr, attr fusefs.Attr) {
	out.Ino = attr.Ino
	out.Size = attr.Size
	out.Blocks = attr.Blocks
	out.Atime = attr.Atime
	out.Mtime = attr.Mtime
	out.Ctime = attr.Ctime
	out.Atimensec = attr.Atimensec
	out.Mtimensec = attr.Mtimensec
	out.Ctimensec = attr.Ctimensec
	out.Mode = attr.Mode
	out.Nlink = attr.Nlink
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Rdev = attr.Rdev
	out.Blksize = attr.Blksize
}
