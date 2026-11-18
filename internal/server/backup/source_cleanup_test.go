//go:build linux

package backup

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

type emptyMountRPC struct {
	opened chan string
	closed chan string
}

func (m *emptyMountRPC) Backup(a *mountrpc.BackupArgs, r *mountrpc.BackupReply) error {
	m.opened <- a.MountID
	r.Status = 200
	return nil
}

func (m *emptyMountRPC) S3Backup(a *mountrpc.S3BackupArgs, r *mountrpc.BackupReply) error {
	m.opened <- a.MountID
	r.Status = 200
	return nil
}

func (m *emptyMountRPC) ARPCCleanup(a *mountrpc.CleanupArgs, r *mountrpc.CleanupReply) error {
	m.closed <- a.MountID
	r.Status = 200
	return nil
}

func (m *emptyMountRPC) S3Cleanup(a *mountrpc.CleanupArgs, r *mountrpc.CleanupReply) error {
	return m.ARPCCleanup(a, r)
}

func TestEmptySourceReleasesOwnedMount(t *testing.T) {
	app, job := raceTestHarness(t, nil)
	base, socket := conf.AgentMountBasePath, conf.MountSocketPath
	conf.AgentMountBasePath = t.TempDir()
	conf.MountSocketPath = filepath.Join(conf.AgentMountBasePath, "mount.sock")
	t.Cleanup(func() { conf.AgentMountBasePath, conf.MountSocketPath = base, socket })
	if err := os.Mkdir(filepath.Join(conf.AgentMountBasePath, job.ID), 0700); err != nil {
		t.Fatal(err)
	}
	m := &emptyMountRPC{make(chan string, 1), make(chan string, 4)}
	srv := rpc.NewServer()
	if err := srv.RegisterName(mountrpc.ServiceName, m); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", conf.MountSocketPath)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			srv.ServeConn(conn)
		}
	}()
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
		<-done
	})
	for _, target := range []coredb.Target{
		{Name: "agent", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessAgent},
		{Name: "s3", Type: coredb.TargetTypeS3, S3Info: &coredb.S3Url{Endpoint: "store"}},
	} {
		t.Run(target.Name, func(t *testing.T) {
			job.Target = target
			b := &backupJob{app: app, job: job, logger: log.WithScope(log.Scope{})}
			_, err := b.start(t.Context(), jobs.ActivityInfo{})
			b.cleanup()
			if !errors.Is(err, jobs.ErrMountEmpty) {
				t.Fatalf("start: got %v, want ErrMountEmpty", err)
			}
			if len(m.opened) != 1 {
				t.Fatalf("acquired %d mounts, want one", len(m.opened))
			}
			opened := <-m.opened
			if len(m.closed) != 1 {
				t.Fatalf("released %d mounts, want one", len(m.closed))
			}
			if closed := <-m.closed; opened == "" || opened != closed {
				t.Fatalf("acquired %q but released %q", opened, closed)
			}
		})
	}
}
