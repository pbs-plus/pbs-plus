//go:build linux

package mount

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	rpcmount "github.com/pbs-plus/pbs-plus/internal/backend/rpc"
	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type S3Mount struct {
	JobId     string
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

func S3FSMount(storeInstance *store.Store, job types.Job, target types.Target) (*S3Mount, error) {
	// Parse target information
	parsedS3, err := s3url.Parse(target.Path)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 url \"%s\" -> %w", target.Path, err)
	}

	s3Mount := &S3Mount{
		JobId:     job.ID,
		Endpoint:  parsedS3.Endpoint,
		AccessKey: parsedS3.AccessKey,
		Bucket:    parsedS3.Bucket,
		Region:    parsedS3.Region,
		UseSSL:    parsedS3.UseSSL,
		Prefix:    job.Subpath,
	}

	s3Mount.Path = filepath.Join(constants.AgentMountBasePath, job.ID)
	s3Mount.Unmount() // Ensure clean mount point

	// Create mount directory if it doesn't exist
	err = os.MkdirAll(s3Mount.Path, 0700)
	if err != nil {
		return nil, fmt.Errorf("error creating directory \"%s\" -> %w", s3Mount.Path, err)
	}

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	errCleanup := func() {
		s3Mount.Unmount()
	}

	args := &rpcmount.S3BackupArgs{
		JobId:        job.ID,
		Endpoint:     parsedS3.Endpoint,
		AccessKey:    parsedS3.AccessKey,
		Bucket:       parsedS3.Bucket,
		Region:       parsedS3.Region,
		UseSSL:       parsedS3.UseSSL,
		UsePathStyle: parsedS3.IsPathStyle,
		Prefix:       job.Subpath,
	}
	var reply rpcmount.BackupReply

	conn, err := net.DialTimeout("unix", constants.MountSocketPath, 5*time.Minute)
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

	store.DisconnectSession(a.Endpoint + "|" + a.JobId)

	_ = os.RemoveAll(a.Path)
}

func (a *S3Mount) IsEmpty() bool {
	return a.isEmpty
}
