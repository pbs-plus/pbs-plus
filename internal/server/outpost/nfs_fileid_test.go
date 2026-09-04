package outpost

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
)

// stackSimFS numbers inodes per relative path like two PxarFS stacks do.
type stackSimFS struct {
	billy.Filesystem
	billy.Change
}

func (s *stackSimFS) simIno(path string) uint64 {
	if path == "" || path == "/" || path == "." {
		return 1
	}
	var h uint64 = 14695981039346656037
	for _, b := range []byte(path) {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h % 1000
}

type simFileInfo struct {
	os.FileInfo
	ino uint64
}

func (s *simFileInfo) Sys() any {
	st := &syscall.Stat_t{}
	if s.FileInfo.Mode().IsDir() {
		st.Mode = syscall.S_IFDIR | 0o755
	} else {
		st.Mode = syscall.S_IFREG | 0o644
	}
	st.Size = s.FileInfo.Size()
	st.Ino = s.ino
	return st
}

func (s *stackSimFS) wrap(fi os.FileInfo, path string) os.FileInfo {
	if fi == nil {
		return nil
	}
	return &simFileInfo{FileInfo: fi, ino: s.simIno(filepath.Join("/", path))}
}

func (s *stackSimFS) Lstat(p string) (os.FileInfo, error) {
	fi, err := s.Filesystem.Lstat(p)
	if err != nil {
		return nil, err
	}
	return s.wrap(fi, p), nil
}

func (s *stackSimFS) Stat(p string) (os.FileInfo, error) {
	fi, err := s.Filesystem.Stat(p)
	if err != nil {
		return nil, err
	}
	return s.wrap(fi, p), nil
}

func (s *stackSimFS) ReadDir(p string) ([]os.FileInfo, error) {
	entries, err := s.Filesystem.ReadDir(p)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, len(entries))
	for i, e := range entries {
		out[i] = s.wrap(e, s.Join(p, e.Name()))
	}
	return out, nil
}

// TestNFSMultiShareFileidCollision proves per-stack inode numbering collides wire fileids.
func TestNFSMultiShareFileidCollision(t *testing.T) {
	shareA := &stackSimFS{Filesystem: memfs.New()}
	shareB := &stackSimFS{Filesystem: memfs.New()}
	_, _ = shareA.Create("/random1.txt")
	fB, _ := shareB.Create("/hello.txt")
	_, _ = fB.Write([]byte("hello-e2e"))

	inst := startTestNFSInstance(t)
	if err := inst.Attach(Attachment{Name: "share-a", FS: shareA}); err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "share-b", FS: shareB}); err != nil {
		t.Fatal(err)
	}

	rootA := mountLookup(t, inst, "/share-a")
	rootB := mountLookup(t, inst, "/share-b")
	fmt.Printf("root fileid share-a=%d share-b=%d fsid-a=%d fsid-b=%d\n",
		rootA.Fileid, rootB.Fileid, rootA.FSID, rootB.FSID)
	if rootA.Fileid == rootB.Fileid {
		t.Errorf("root fileids collide across shares: %d == %d", rootA.Fileid, rootB.Fileid)
	}
}

func mountLookup(t *testing.T, inst *nfsInstance, share string) *nfsc.Fattr {
	t.Helper()
	addr := inst.ln.Addr().String()
	client, err := rpc.DialTCP("tcp", addr, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	mnt := &nfsc.Mount{Client: client}
	target, err := mnt.Mount(share, rpc.AuthNull)
	if err != nil {
		t.Fatal(err)
	}
	fi, _, err := target.Lookup(".")
	if err != nil {
		t.Fatal(err)
	}
	fattr, ok := fi.(*nfsc.Fattr)
	if !ok {
		t.Fatalf("lookup returned %T, want *Fattr", fi)
	}
	return fattr
}

func startTestNFSInstance(t *testing.T) *nfsInstance {
	t.Helper()
	inst, err := nfsDriver{}.Start(context.Background(), Outpost{Name: "coll", Type: TypeNFS, ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Stop() })
	return inst.(*nfsInstance)
}
