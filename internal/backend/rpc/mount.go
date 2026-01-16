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
	vfssessions "github.com/pbs-plus/pbs-plus/internal/store/vfs"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type BackupArgs struct {
	BackupId       string
	TargetHostname string
	Drive          string
}

type S3BackupArgs struct {
	BackupId     string
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
	BackupId       string
	TargetHostname string
}

type StatusReply struct {
	Connected bool
}

type VFSStatusArgs struct {
	Key string
}

type CleanupArgs struct {
	BackupId       string
	TargetHostname string
	Drive          string
}

type CleanupReply struct {
	Status  int
	Message string
}

type WarnCountArgs struct {
	BackupId string
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
			"backupId": args.BackupId,
			"target":   args.TargetHostname,
			"drive":    args.Drive,
		}).Write()

	// Retrieve the backup from the database.
	backup, err := s.Store.Database.GetBackup(args.BackupId)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
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
		BackupId:   args.BackupId,
		SourceMode: backup.SourceMode,
		ReadMode:   backup.ReadMode,
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

	// If a namespace is provided in the backup response, update the backup.
	if len(backupRespSplit) == 2 && backupRespSplit[1] != "" {
		backup.Namespace = backupRespSplit[1]
		if err := s.Store.Database.UpdateBackup(nil, backup); err != nil {
			syslog.L.Error(err).WithField("namespace", backupRespSplit[1]).Write()
		}
	}

	backupCtx, backupCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.BackupId, backupCancel)

	arpcFS := arpcfs.NewARPCFS(backupCtx, s.Store.ARPCAgentsManager, backup.GetStreamID(), args.TargetHostname, backup, backupMode)
	if arpcFS == nil {
		reply.Status = 500
		reply.Message = "failed to send create ARPCFS"
		return errors.New(reply.Message)
	}

	// Set up the local mount path.
	mntPath := filepath.Join(constants.AgentMountBasePath, args.BackupId)

	if err := arpcmount.Mount(arpcFS, mntPath); err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = fmt.Sprintf("failed to create fuse connection for target -> %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	vfssessions.CreateARPCFSMount(backup.GetStreamID(), arpcFS)

	// Set the reply values.
	reply.Status = 200
	reply.Message = backupMode + "|" + backup.Namespace
	reply.BackupMode = backupMode

	syslog.L.Info().
		WithMessage("Mounting successful").
		WithFields(map[string]any{
			"backupId": args.BackupId,
			"mount":    mntPath,
			"backup":   backupMode,
		}).Write()

	return nil
}

func (s *MountRPCService) S3Backup(args *S3BackupArgs, reply *BackupReply) error {
	syslog.L.Info().
		WithMessage("Received S3 backup request").
		WithFields(map[string]any{
			"backupId": args.BackupId,
			"endpoint": args.Endpoint,
			"bucket":   args.Bucket,
			"prefix":   args.Prefix,
		}).Write()

	// Retrieve the backup from the database.
	backup, err := s.Store.Database.GetBackup(args.BackupId)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}

	secretKey, err := s.Store.Database.GetS3Secret(backup.Target)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get secret key of target"
		return fmt.Errorf("backup: %w", err)
	}

	backupCtx, backupCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.BackupId, backupCancel)

	s3FS := s3fs.NewS3FS(backupCtx, backup, args.Endpoint, args.AccessKey, secretKey, args.Bucket, args.Region, args.Prefix, args.UseSSL)
	if s3FS == nil {
		reply.Status = 500
		reply.Message = "failed to send create S3FS"
		return errors.New(reply.Message)
	}

	// Set up the local mount path.
	mntPath := filepath.Join(constants.AgentMountBasePath, args.BackupId)

	if err := s3mount.Mount(s3FS, mntPath); err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = fmt.Sprintf("Failed to create fuse connection for target -> %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	vfssessions.CreateS3FSMount(backup.GetStreamID(), s3FS)

	// Set the reply values.
	reply.Status = 200
	reply.Message = backup.Namespace

	syslog.L.Info().
		WithMessage("Mounting successful").
		WithFields(map[string]any{
			"backupId": args.BackupId,
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
			"backupId": args.BackupId,
			"target":   args.TargetHostname,
			"drive":    args.Drive,
		}).Write()

	childKey := args.TargetHostname + "|" + args.BackupId
	vfssessions.DisconnectSession(childKey)

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	ctxCancel, ok := s.jobCtxCancels.GetAndDel(args.BackupId)
	if ok {
		ctxCancel()
	}

	arpcSess, exists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !exists {
		syslog.L.Info().
			WithMessage("Target unreachable, assuming cleanup successful.").
			WithField("jobId", args.BackupId).
			Write()
		reply.Status = 200
		reply.Message = "Target unreachable, assuming cleanup successful."
		return nil
	}

	cleanupReq := types.BackupReq{
		Drive:    args.Drive,
		BackupId: args.BackupId,
	}

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
		WithField("backupId", args.BackupId).
		Write()

	return nil
}

func (s *MountRPCService) Status(args *StatusArgs, reply *StatusReply) error {
	syslog.L.Info().
		WithMessage("Received status request").
		WithFields(map[string]any{
			"backupId": args.BackupId,
			"target":   args.TargetHostname,
		}).Write()

	// Retrieve the ARPC session for the target.
	_, exists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !exists {
		reply.Connected = false
		return nil
	}

	childKey := args.TargetHostname + "|" + args.BackupId
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
