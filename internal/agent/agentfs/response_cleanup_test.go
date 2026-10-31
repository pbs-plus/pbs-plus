package agentfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
)

func TestReadAtAbortedResponseReleasesHandle(t *testing.T) {
	for _, mode := range []string{"standard", "mmap"} {
		t.Run(mode, func(t *testing.T) {
			s := &Server{readMode: mode, handles: safemap.New[uint64, *FileHandle]()}
			fh := &FileHandle{fileSize: 4096}
			s.handles.Set(1, fh)
			payload, err := cbor.Marshal(fswire.ReadAtReq{HandleID: 1, Length: 4096})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := s.handleReadAt(&arpc.Request{Context: context.Background(), Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Cleanup == nil {
				t.Fatal("response does not own handle cleanup")
			}
			if !fh.beginClose() {
				t.Fatal("could not begin close")
			}
			resp.Cleanup()
			if !fh.waitForOps(time.Second) {
				t.Fatal("aborted response retained an active operation")
			}
		})
	}
}

func TestReadDirFailureReleasesHandle(t *testing.T) {
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, "entry"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewDirReader(f, path)
	if err != nil {
		if closeErr := f.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	s := &Server{handles: safemap.New[uint64, *FileHandle]()}
	fh := &FileHandle{dirReader: reader}
	s.handles.Set(1, fh)
	payload, err := cbor.Marshal(fswire.ReadDirReq{HandleID: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.handleReadDir(&arpc.Request{Context: ctx, Payload: payload})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
	if !fh.beginClose() || !fh.waitForOps(time.Second) {
		t.Fatal("failed directory read retained an active operation")
	}
}
