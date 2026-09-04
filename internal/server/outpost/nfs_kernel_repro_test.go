package outpost

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
)

// TestKernelNFSOutpostRepro reproduces the CI vfs-outpost failure with a real kernel
// client; gated behind KERNEL_NFS_REPRO=1 because it needs CAP_SYS_ADMIN.
func TestKernelNFSOutpostRepro(t *testing.T) {
	if os.Getenv("KERNEL_NFS_REPRO") == "" {
		t.Skip("set KERNEL_NFS_REPRO=1 (privileged container required)")
	}

	base := t.TempDir()
	port := freeTCPPort(t)

	storeA := reproArchive(t, filepath.Join(base, "storeA"), map[string]string{
		"random1.txt": "random-one",
		"random2.txt": "random-two",
	})
	storeB := reproArchive(t, filepath.Join(base, "storeB"), map[string]string{
		"hello.txt": "hello init\n",
	})

	overlayA := filepath.Join(base, "overlayA")
	overlayB := filepath.Join(base, "overlayB")
	_ = os.MkdirAll(overlayA, 0o700)
	_ = os.MkdirAll(overlayB, 0o700)

	stackA := reproStack(t, storeA, overlayA)

	stackB1 := reproStack(t, storeB, overlayB)
	var eo fuse.EntryOut
	mi := &fuse.MkdirIn{Mode: 0o755}
	mi.NodeId = 1
	if st := stackB1.MFS.Mkdir(nil, mi, "made-in-rw", &eo); st != fuse.OK {
		t.Fatalf("mkdir on rw session: %s", st)
	}
	stackB1.Close()

	stackB := reproStack(t, storeB, overlayB)

	inst := reproServe(t, port)
	defer inst.Stop()

	if err := inst.Attach(Attachment{Name: "share-b", FS: pxarmount.NewNFSFilesystem(stackB.Raw, true)}); err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "share-a", FS: pxarmount.NewNFSFilesystem(stackA.Raw, true)}); err != nil {
		t.Fatal(err)
	}

	reproProbe(t, net.JoinHostPort("127.0.0.1", port), "share-a")
	reproProbe(t, net.JoinHostPort("127.0.0.1", port), "share-b")

	if os.Getenv("KERNEL_NFS_MOUNT") == "" {
		return
	}

	mntA := filepath.Join(base, "clientA")
	mntB := filepath.Join(base, "clientB")
	_ = os.MkdirAll(mntA, 0o755)
	_ = os.MkdirAll(mntB, 0o755)
	addr := net.JoinHostPort("127.0.0.1", port)

	reproMount(t, addr, "share-a", mntA)
	defer exec.Command("umount", mntA).Run()
	reproMount(t, addr, "share-b", mntB)
	defer exec.Command("umount", mntB).Run()

	for _, c := range []struct{ name, dir string }{
		{"client A", mntA}, {"client B", mntB},
	} {
		out, err := exec.Command("ls", "-la", c.dir).CombinedOutput()
		fmt.Printf("--- %s (%s) ---\n%s err=%v\n", c.name, c.dir, out, err)
	}

	reproCat(t, mntA, "random1.txt")
	reproCat(t, mntB, "hello.txt")

	vols, _ := os.ReadFile("/proc/fs/nfsfs/volumes")
	fmt.Printf("--- /proc/fs/nfsfs/volumes ---\n%s\n", vols)
}

func reproProbe(t *testing.T, addr, share string) {
	t.Helper()
	client, err := rpc.DialTCP("tcp", addr, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	mnt := &nfsc.Mount{Client: client}
	target, err := mnt.Mount(share, rpc.AuthNull)
	if err != nil {
		fmt.Printf("[%s] mount: err=%v\n", share, err)
		return
	}
	fmt.Printf("[%s] mount: ok\n", share)
	if mask, err := target.Access(".", 0x3f); err != nil {
		fmt.Printf("[%s] access(.): err=%v\n", share, err)
	} else {
		fmt.Printf("[%s] access(.): mask=%#x\n", share, mask)
	}
	if _, _, err := target.Lookup(".."); err != nil {
		fmt.Printf("[%s] lookup(..): err=%v\n", share, err)
	} else {
		fmt.Printf("[%s] lookup(..): ok\n", share)
	}
	if entries, err := target.ReadDirPlus("."); err != nil {
		fmt.Printf("[%s] readdirplus(.): err=%v\n", share, err)
	} else {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		fmt.Printf("[%s] readdirplus(.): %v\n", share, names)
	}
}

func reproMount(t *testing.T, addr, share, mnt string) {
	t.Helper()
	_, port, _ := net.SplitHostPort(addr)
	opts := "nfsvers=3,proto=tcp,mountproto=tcp,port=" + port + ",mountport=" + port + ",nolock,noacl,ro,soft,timeo=50,retrans=2"
	cmd := exec.Command("mount", "-t", "nfs", "-o", opts, addr+":/"+share, mnt)
	if os.Getenv("STRACE_MOUNT") != "" {
		cmd = exec.Command("strace", "-f", "-o", "/work/strace-"+share+".out", "mount", "-t", "nfs", "-o", opts, addr+":/"+share, mnt)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("mount %s: %v\n%s", share, err, out)
	}
}

func reproCat(t *testing.T, dir, name string) {
	t.Helper()
	out, err := exec.Command("cat", filepath.Join(dir, name)).CombinedOutput()
	fmt.Printf("--- cat %s/%s ---\n%s err=%v\n", dir, name, out, err)
}

func freeTCPPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

func reproServe(t *testing.T, port string) *nfsInstance {
	t.Helper()
	d := nfsDriver{}
	inst, err := d.Start(context.Background(), Outpost{
		Name:       "repro",
		ListenAddr: net.JoinHostPort("0.0.0.0", port),
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	return inst.(*nfsInstance)
}

func reproStack(t *testing.T, storeDir, overlay string) *pxarmount.Stack {
	t.Helper()
	stack, err := pxarmount.BuildStack(pxarmount.MountConfig{
		PBSStore:   storeDir,
		Reader:     reproReader(t, storeDir),
		BackingDir: overlay,
	})
	if err != nil {
		t.Fatalf("BuildStack: %v", err)
	}
	return stack
}

func reproReader(t *testing.T, storeDir string) *transfer.SplitReader {
	t.Helper()
	meta, err := os.ReadFile(filepath.Join(storeDir, "root.mpxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(storeDir, "root.ppxar.didx"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := datastore.NewChunkStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := transfer.NewSplitReader(meta, payload, datastore.NewChunkStoreSource(store))
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func reproArchive(t *testing.T, storeDir string, files map[string]string) string {
	t.Helper()
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(storeDir, config, false)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   filepath.Base(storeDir),
	})
	if err != nil {
		t.Fatal(err)
	}

	writer := transfer.NewSessionWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		fileMeta := pxar.FileMetadata(0o644).Build()
		if err := writer.WriteEntry(&pxar.Entry{
			Path:     name,
			Kind:     pxar.KindFile,
			Metadata: fileMeta,
			FileSize: uint64(len(content)),
		}, []byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}
	return storeDir
}
