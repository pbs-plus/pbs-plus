package pxarmount

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

type oneShotJournalFault struct {
	op    errorfs.Op
	armed atomic.Bool
}

func (f *oneShotJournalFault) MaybeError(op errorfs.Op, _ string) error {
	if op == f.op && f.armed.CompareAndSwap(true, false) {
		return errorfs.ErrInjected
	}
	return nil
}

type syncFaultFS struct {
	vfs.FS
	fault *oneShotJournalFault
}

type syncFaultFile struct {
	vfs.File
	fault *oneShotJournalFault
}

func (fs syncFaultFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return syncFaultFile{File: f, fault: fs.fault}, nil
}

func (fs syncFaultFS) Create(name string) (vfs.File, error) {
	return fs.wrap(fs.FS.Create(name))
}

func (fs syncFaultFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.Open(name, opts...))
}

func (fs syncFaultFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenReadWrite(name, opts...))
}

func (fs syncFaultFS) OpenDir(name string) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenDir(name))
}

func (fs syncFaultFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return fs.wrap(fs.FS.ReuseForWrite(oldname, newname))
}

func (f syncFaultFile) inject() error {
	if f.fault.armed.CompareAndSwap(true, false) {
		return errorfs.ErrInjected
	}
	return nil
}

func (f syncFaultFile) Sync() error {
	if err := f.inject(); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f syncFaultFile) SyncData() error {
	if err := f.inject(); err != nil {
		return err
	}
	return f.File.SyncData()
}

func (f syncFaultFile) SyncTo(length int64) (bool, error) {
	if err := f.inject(); err != nil {
		return false, err
	}
	return f.File.SyncTo(length)
}

// TestJournalFaultProcess is the subprocess killed by Pebble's fatal WAL path.
func TestJournalFaultProcess(t *testing.T) {
	dir := os.Getenv("PXARMOUNT_FAULT_DIR")
	if dir == "" {
		t.Skip()
	}
	op, err := strconv.Atoi(os.Getenv("PXARMOUNT_FAULT_OP"))
	if err != nil {
		t.Fatal(err)
	}
	fault := &oneShotJournalFault{op: errorfs.Op(op)}
	base := errorfs.Wrap(vfs.Default, fault)
	var fs vfs.FS = base
	if fault.op == errorfs.OpFileSync {
		fs = syncFaultFS{FS: base, fault: fault}
	}
	journal, err := openJournal(dir, fs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.EnsureNodePath("/candidate.bin", &GraphNode{Kind: NodeFile, Mode: 0o100644, HasData: true}, false); err != nil {
		t.Fatal(err)
	}
	fault.armed.Store(true)
	if err := journal.Sync(); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

// TestJournalErrorFSAtomicity crashes Pebble at WAL write and sync boundaries,
// then verifies recovery exposes either the old graph or the full new graph.
func TestJournalNodeIDSurvivesAutomaticDrain(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := journal.EnsureNodePath("/first.bin", &GraphNode{Kind: NodeFile, Mode: 0o100644, HasData: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	journal.drainAllLocked()
	journal.abandonForTest()

	reopened, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	secondID, err := reopened.EnsureNodePath("/second.bin", &GraphNode{Kind: NodeFile, Mode: 0o100644, HasData: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	if secondID <= firstID {
		t.Fatalf("node ID after reopen = %d, want > %d", secondID, firstID)
	}
	first, err := reopened.GetNode(firstID)
	if err != nil || first == nil {
		t.Fatalf("first node was replaced: node=%v err=%v", first, err)
	}
}

func TestJournalErrorFSAtomicity(t *testing.T) {
	for _, op := range []errorfs.Op{errorfs.OpFileWrite, errorfs.OpFileSync} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			dir := t.TempDir()
			journal, err := OpenJournal(dir)
			if err != nil {
				t.Fatal(err)
			}
			baselineID, err := journal.EnsureNodePath("/baseline.bin", &GraphNode{Kind: NodeFile, Mode: 0o100644, HasData: true}, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestJournalFaultProcess$", "-test.count=1")
			cmd.Env = append(os.Environ(),
				"PXARMOUNT_FAULT_DIR="+dir,
				"PXARMOUNT_FAULT_OP="+strconv.Itoa(int(op)),
			)
			if err := cmd.Run(); err == nil {
				t.Fatal("fault subprocess survived injected WAL failure")
			}

			reopened, err := OpenJournal(dir)
			if err != nil {
				t.Fatalf("reopen after fault: %v", err)
			}
			defer reopened.Close()
			gotID, _, _, _, err := reopened.ResolvePath("/baseline.bin")
			if err != nil || gotID != baselineID {
				t.Fatalf("baseline after fault = (%d, %v), want (%d, nil)", gotID, err, baselineID)
			}
			candidateID, _, _, _, err := reopened.ResolvePath("/candidate.bin")
			if err != nil {
				t.Fatal(err)
			}
			if candidateID != 0 {
				node, err := reopened.GetNode(candidateID)
				if err != nil || node == nil || !node.HasData {
					t.Fatalf("candidate graph is torn: id=%d node=%v err=%v", candidateID, node, err)
				}
			}
		})
	}
}
