//go:build linux

package pxarmount

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

// nfsHandleLimit caps the file handles go-nfs keeps alive. Handles are the NFS
// equivalent of FUSE inodes: a client that loses one gets ESTALE, so this is
// sized to cover a full restore walk rather than a working set.
const nfsHandleLimit = 1 << 20

// nfsMountTimeout bounds the loopback mount attempt so a missing nfs client
// module fails the unit instead of hanging the session forever.
const nfsMountTimeout = 30 * time.Second

// NFSServer owns an NFSv3 export of a pxar mount and the loopback mount that
// makes it visible at a POSIX path.
type NFSServer struct {
	listener   net.Listener
	mountPoint string
	done       chan error
	mounted    bool
}

// ServeNFSAsync exports rawFS over NFSv3 on loopback and mounts it at
// cfg.MountPoint. Serving continues in the background until Close.
func ServeNFSAsync(cfg MountConfig, rawFS fuse.RawFileSystem, readOnly bool) (*NFSServer, error) {
	rawFS.Init(nil)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("nfs listen on loopback: %w", err)
	}

	billyFS := NewNFSFilesystem(rawFS, readOnly)
	handler := nfshelper.NewCachingHandler(
		nfshelper.NewNullAuthHandler(billyFS),
		nfsHandleLimit,
	)

	srv := &NFSServer{
		listener:   listener,
		mountPoint: cfg.MountPoint,
		done:       make(chan error, 1),
	}

	go func() {
		err := nfs.Serve(listener, handler)
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		srv.done <- err
	}()

	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "  nfs: serving %s on %s\n", cfg.MountPoint, listener.Addr())
	}

	if err := srv.mountLoopback(cfg, readOnly); err != nil {
		if cerr := listener.Close(); cerr != nil {
			log.Error(cerr, "")
		}
		return nil, err
	}
	srv.mounted = true
	return srv, nil
}

func (s *NFSServer) Wait() error { return <-s.done }

func (s *NFSServer) mountLoopback(cfg MountConfig, readOnly bool) error {
	addr, ok := s.listener.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("nfs listener is not tcp: %s", s.listener.Addr())
	}

	opts := []string{
		"nfsvers=3",
		"proto=tcp",
		"mountproto=tcp",
		"port=" + strconv.Itoa(addr.Port),
		"mountport=" + strconv.Itoa(addr.Port),
		"nolock",
		"noacl",
		"hard",
		"intr",
		"rsize=1048576",
		"wsize=1048576",
	}
	if readOnly {
		opts = append(opts, "ro")
	} else {
		opts = append(opts, "rw")
	}

	host := addr.IP.String()

	if err := os.MkdirAll(cfg.MountPoint, 0o755); err != nil {
		return fmt.Errorf("nfs mountpoint %s: %w", cfg.MountPoint, err)
	}

	cmd := exec.Command("mount", "-t", "nfs",
		"-o", strings.Join(opts, ","),
		host+":/", cfg.MountPoint)

	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("nfs mount %s: %w", cfg.MountPoint, err)
	}
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("nfs mount %s: %w", cfg.MountPoint, err)
		}
	case <-time.After(nfsMountTimeout):
		if kerr := cmd.Process.Kill(); kerr != nil {
			log.Error(kerr, "")
		}
		return fmt.Errorf("nfs mount %s timed out after %s", cfg.MountPoint, nfsMountTimeout)
	}

	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "  nfs: mounted %s:/ at %s\n", host, cfg.MountPoint)
	}
	return nil
}

// Close unmounts the loopback mount and stops accepting NFS connections. The
// unmount runs first so in-flight client operations drain against a live
// server rather than blocking on a dead socket.
func (s *NFSServer) Close() error {
	var unmountErr error
	if s.mounted {
		unmountErr = unmountNFS(s.mountPoint)
		s.mounted = false
	}
	if err := s.listener.Close(); err != nil && unmountErr == nil {
		unmountErr = err
	}
	return unmountErr
}

func unmountNFS(mountPoint string) error {
	if err := exec.Command("umount", mountPoint).Run(); err == nil {
		return nil
	}
	if err := exec.Command("umount", "-f", "-l", mountPoint).Run(); err == nil {
		return nil
	}
	return fmt.Errorf("failed to unmount %s", mountPoint)
}
