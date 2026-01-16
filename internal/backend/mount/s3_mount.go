//go:build linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	rpcmount "github.com/pbs-plus/pbs-plus/internal/backend/rpc"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	vfssessions "github.com/pbs-plus/pbs-plus/internal/store/vfs"
)

type S3Mount struct {
	BackupId  string
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	Prefix    string
	UseSSL    bool
	Path      string
	isEmpty   bool
}

func S3FSMount(ctx context.Context, storeInstance *store.Store, backup types.Backup, target types.Target) (*S3Mount, error) {
	// Parse target information
	parsedS3 := target.Path.GetPathInfo().S3Url

	s3Mount := &S3Mount{
		BackupId:  backup.ID,
		Endpoint:  parsedS3.Endpoint,
		AccessKey: parsedS3.AccessKey,
		Bucket:    parsedS3.Bucket,
		Region:    parsedS3.Region,
		UseSSL:    parsedS3.UseSSL,
		Prefix:    backup.Subpath,
	}

	s3Mount.Path = filepath.Join(constants.AgentMountBasePath, backup.ID)
	s3Mount.Unmount() // Ensure clean mount point

	// Create mount directory if it doesn't exist
	err := os.MkdirAll(s3Mount.Path, 0700)
	if err != nil {
		s3Mount.CloseMount()
		return nil, fmt.Errorf("error creating directory \"%s\" -> %w", s3Mount.Path, err)
	}

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	errCleanup := func() {
		s3Mount.CloseMount()
		s3Mount.Unmount()
	}

	args := &rpcmount.S3BackupArgs{
		BackupId:     backup.ID,
		Endpoint:     parsedS3.Endpoint,
		AccessKey:    parsedS3.AccessKey,
		Bucket:       parsedS3.Bucket,
		Region:       parsedS3.Region,
		UseSSL:       parsedS3.UseSSL,
		UsePathStyle: parsedS3.IsPathStyle,
		Prefix:       backup.Subpath,
	}
	var reply rpcmount.BackupReply

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", constants.MountSocketPath)
	if err != nil {
		errCleanup()
		return nil, fmt.Errorf("failed to reach backup RPC: %w", err)
	} else {
		rpcClient := rpc.NewClient(conn)
		err = rpcClient.Call("MountRPCService.S3Backup", args, &reply)
		rpcClient.Close()
		if err != nil {
			errCleanup()
			return nil, err
		}
		if reply.Status != 200 {
			errCleanup()
			return nil, errors.New(reply.Message)
		}
	}

	isAccessible := false
	checkTimeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

checkLoop:
	for {
		select {
		case <-ctx.Done():
			errCleanup()
			return nil, ctx.Err()
		case <-checkTimeout:
			break checkLoop
		case <-ticker.C:
			if entries, err := os.ReadDir(s3Mount.Path); err == nil {
				isAccessible = true
				s3Mount.isEmpty = len(entries) == 0
				break checkLoop
			}
		}
	}
	if !isAccessible {
		errCleanup()
		return nil, fmt.Errorf("mounted directory not accessible after timeout")
	}
	return s3Mount, nil
}

func (a *S3Mount) Unmount() {
	if a.Path == "" {
		return
	}

	umount := exec.Command("fusermount", "-uz", a.Path)
	umount.Env = os.Environ()
	err := umount.Run()
	if err != nil {
		umount = exec.Command("umount", "-lf", a.Path)
		umount.Env = os.Environ()
		err = umount.Run()
		if err != nil {
			return
		}
	}

	_ = os.RemoveAll(a.Path)
}

func (a *S3Mount) CloseMount() {
	vfssessions.DisconnectSession(a.Endpoint + "|" + a.BackupId)
}

func (a *S3Mount) IsEmpty() bool {
	return a.isEmpty
}
