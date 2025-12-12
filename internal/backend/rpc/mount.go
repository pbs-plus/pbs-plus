//go:build linux
// +build linux

package rpcmount

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
	arpcmount "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc/mount"
	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3"
	s3mount "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/mount"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type BackupArgs struct {
	JobId          string
	TargetHostname string
	Drive          string
}

type S3BackupArgs struct {
	JobId        string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Bucket       string
	Region       string
	Prefix       string
	UseSSL       bool
	UsePathStyle bool
	Path         string
}

type BackupReply struct {
	Status     int
	Message    string
	BackupMode string
}

type StatusArgs struct {
	JobId          string
	TargetHostname string
}

type StatusReply struct {
	Connected bool
}

type VFSStatusArgs struct {
	Key string
}

type CleanupArgs struct {
	JobId          string
	TargetHostname string
	Drive          string
}

type CleanupReply struct {
	Status  int
	Message string
}

type WarnCountArgs struct {
	JobId string
}

type WarnCountReply struct {
	Count int
}

type MountRPCService struct {
	ctx           context.Context
	Store         *store.Store
	jobCtxCancels *safemap.Map[string, context.CancelFunc]
}

func (s *MountRPCService) Backup(args *BackupArgs, reply *BackupReply) error {
	syslog.L.Info().
		WithMessage("Received backup request").
		WithFields(map[string]any{
			"jobId":  args.JobId,
			"target": args.TargetHostname,
			"drive":  args.Drive,
		}).Write()

	// Retrieve the job from the database.
	job, err := s.Store.Database.GetJob(args.JobId)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get job from id"
		return fmt.Errorf("backup: %w", err)
	}

	// Create a context with a 2-minute timeout.
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Retrieve the ARPC session for the target.
	arpcSess, exists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !exists {
		reply.Status = 500
		reply.Message = "unable to reach target"
		return errors.New(reply.Message)
	}

	// Prepare the backup request (using the types.BackupReq structure).
	backupReq := types.BackupReq{
		Drive:      args.Drive,
		JobId:      args.JobId,
		SourceMode: job.SourceMode,
		ReadMode:   job.ReadMode,
	}

	// Call the target's backup method via ARPC.
	respMsg, err := arpcSess.CallMessage(ctx, "backup", &backupReq)
	if err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = err.Error()
		return errors.New(reply.Message)
	}

	// Parse the backup response message (format: "backupMode|namespace").
	backupRespSplit := strings.Split(respMsg, "|")
	backupMode := backupRespSplit[0]

	// If a namespace is provided in the backup response, update the job.
	if len(backupRespSplit) == 2 && backupRespSplit[1] != "" {
		job.Namespace = backupRespSplit[1]
		if err := s.Store.Database.UpdateJob(nil, job); err != nil {
			syslog.L.Error(err).WithField("namespace", backupRespSplit[1]).Write()
		}
	}

	// Retrieve or initialize an ARPCFS instance.
	// The child session key is "targetHostname|jobId".
	childKey := args.TargetHostname + "|" + args.JobId
	arpcFSRPC, exists := s.Store.ARPCAgentsManager.GetStreamPipe(childKey)
	if !exists {
		reply.Status = 500
		reply.Message = "unable to reach child target"
		return errors.New(reply.Message)
	}

	jobCtx, jobCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.JobId, jobCancel)

	arpcFS := arpcfs.NewARPCFS(jobCtx, arpcFSRPC, args.TargetHostname, job, backupMode)
	if arpcFS == nil {
		reply.Status = 500
		reply.Message = "failed to send create ARPCFS"
		return errors.New(reply.Message)
	}

	// Set up the local mount path.
	mntPath := filepath.Join(constants.AgentMountBasePath, args.JobId)

	if err := arpcmount.Mount(arpcFS, mntPath); err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = fmt.Sprintf("failed to create fuse connection for target -> %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	store.CreateARPCFSMount(childKey, arpcFS)

	// Set the reply values.
	reply.Status = 200
	reply.Message = backupMode + "|" + job.Namespace
	reply.BackupMode = backupMode

	syslog.L.Info().
		WithMessage("Backup successful").
		WithFields(map[string]any{
			"jobId":  args.JobId,
			"mount":  mntPath,
			"backup": backupMode,
		}).Write()

	return nil
}

func (s *MountRPCService) S3Backup(args *S3BackupArgs, reply *BackupReply) error {
	syslog.L.Info().
		WithMessage("Received S3 backup request").
		WithFields(map[string]any{
			"jobId":    args.JobId,
			"endpoint": args.Endpoint,
			"bucket":   args.Bucket,
			"prefix":   args.Prefix,
		}).Write()

	// Retrieve the job from the database.
	job, err := s.Store.Database.GetJob(args.JobId)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get job from id"
		return fmt.Errorf("backup: %w", err)
	}

	secretKey, err := s.Store.Database.GetS3Secret(job.Target)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get secret key of target"
		return fmt.Errorf("backup: %w", err)
	}

	childKey := args.Endpoint + "|" + args.JobId

	jobCtx, jobCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.JobId, jobCancel)

	s3FS := s3fs.NewS3FS(jobCtx, job, args.Endpoint, args.AccessKey, secretKey, args.Bucket, args.Region, args.Prefix, args.UseSSL)
	if s3FS == nil {
		reply.Status = 500
		reply.Message = "failed to send create S3FS"
		return errors.New(reply.Message)
	}

	store.CreateS3FSMount(childKey, s3FS)

	// Set up the local mount path.
	mntPath := filepath.Join(constants.AgentMountBasePath, args.JobId)

	if err := s3mount.Mount(s3FS, mntPath); err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = fmt.Sprintf("Failed to create fuse connection for target -> %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	// Set the reply values.
	reply.Status = 200
	reply.Message = job.Namespace

	syslog.L.Info().
		WithMessage("Backup successful").
		WithFields(map[string]any{
			"jobId":    args.JobId,
			"mount":    mntPath,
			"endpoint": args.Endpoint,
			"bucket":   args.Bucket,
			"prefix":   args.Prefix,
		}).Write()

	return nil
}

func (s *MountRPCService) Cleanup(args *CleanupArgs, reply *CleanupReply) error {
	syslog.L.Info().
		WithMessage("Received cleanup request").
		WithFields(map[string]any{
			"jobId":  args.JobId,
			"target": args.TargetHostname,
			"drive":  args.Drive,
		}).Write()

	// Create a 30-second timeout context.
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Try to acquire an ARPC session for the target.
	arpcSess, exists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !exists {
		reply.Status = 500
		reply.Message = "failed to send closure request to target"
		return fmt.Errorf("cleanup: unable to reach target for job %s", args.JobId)
	}

	// Create a cleanup request (using the BackupReq type).
	cleanupReq := types.BackupReq{
		Drive: args.Drive,
		JobId: args.JobId,
	}

	ctxCancel, ok := s.jobCtxCancels.GetAndDel(args.JobId)
	if ok {
		ctxCancel()
	}

	// Instruct the target to perform its cleanup.
	_, err := arpcSess.CallMessage(ctx, "cleanup", &cleanupReq)
	if err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = err.Error()
		return errors.New(reply.Message)
	}

	reply.Status = 200
	reply.Message = "Cleanup successful"

	syslog.L.Info().
		WithMessage("Cleanup successful").
		WithField("jobId", args.JobId).
		Write()

	return nil
}

func (s *MountRPCService) Status(args *StatusArgs, reply *StatusReply) error {
	syslog.L.Info().
		WithMessage("Received status request").
		WithFields(map[string]any{
			"jobId":  args.JobId,
			"target": args.TargetHostname,
		}).Write()

	// Retrieve the ARPC session for the target.
	_, exists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !exists {
		reply.Connected = false
		return nil
	}

	childKey := args.TargetHostname + "|" + args.JobId
	_, exists = s.Store.ARPCAgentsManager.GetStreamPipe(childKey)
	if !exists {
		reply.Connected = false
		return nil
	}

	reply.Connected = true
	return nil
}

func StartRPCServer(watcher chan struct{}, ctx context.Context, socketPath string, storeInstance *store.Store) error {
	// Remove any stale socket file.
	_ = os.RemoveAll(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", socketPath, err)
	}

	service := &MountRPCService{
		ctx:           ctx,
		Store:         storeInstance,
		jobCtxCancels: safemap.New[string, context.CancelFunc](),
	}

	// Register the RPC service.
	if err := rpc.Register(service); err != nil {
		return fmt.Errorf("failed to register rpc service: %v", err)
	}

	// Start accepting connections.
	ready := make(chan struct{})

	go func() {
		if watcher != nil {
			defer close(watcher)
		}
		close(ready)
		rpc.Accept(listener)
	}()

	syslog.L.Info().
		WithMessage("RPC server listening").
		WithField("socket", socketPath).
		Write()

	<-ready

	return nil
}

func RunRPCServer(ctx context.Context, socketPath string, storeInstance *store.Store) error {
	watcher := make(chan struct{}, 1)
	err := StartRPCServer(watcher, ctx, socketPath, storeInstance)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		syslog.L.Info().
			WithMessage("rpc mount server shutting down due to context cancellation").
			WithField("socket", socketPath).
			Write()
		_ = os.Remove(socketPath)
	case <-watcher:
		syslog.L.Info().
			WithMessage("rpc mount server shut down unexpectedly").
			WithField("socket", socketPath).
			Write()
	}

	return nil
}
