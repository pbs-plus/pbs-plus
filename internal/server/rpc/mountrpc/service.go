//go:build linux

package rpc

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
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	s3fs "github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

type BackupArgs struct {
	BackupID       string
	TargetHostname string
	Drive          string
}

type S3BackupArgs struct {
	BackupID     string
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
	BackupID       string
	TargetHostname string
}

type StatusReply struct {
	Connected bool
}

type VFSStatusArgs struct {
	Key string
}

type CleanupArgs struct {
	BackupID       string
	TargetHostname string
	Drive          string
}

type CleanupReply struct {
	Status  int
	Message string
}

type WarnCountArgs struct {
	BackupID string
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
	log.Info("received backup request")

	backup, err := s.Store.Database.GetBackup(args.BackupID)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Retrieve the ARPC session for the target (QUIC preferred, TCP fallback).
	var respMsg string
	if qPipe, ok := s.Store.ARPCAgentsManager.GetQuicPipe(args.TargetHostname); ok {
		backupReq := types.BackupReq{
			Drive:      args.Drive,
			BackupID:   args.BackupID,
			SourceMode: backup.SourceMode,
			ReadMode:   backup.ReadMode,
		}

		s.Store.ARPCAgentsManager.Expect(backup.GetStreamID())

		var err error
		respMsg, err = qPipe.CallMessage(ctx, "backup", &backupReq)
		if err != nil {
			log.Error(err, "")
			reply.Status = 500
			reply.Message = err.Error()
			return errors.New(reply.Message)
		}
	} else if tcpPipe, ok := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname); ok {
		backupReq := types.BackupReq{
			Drive:      args.Drive,
			BackupID:   args.BackupID,
			SourceMode: backup.SourceMode,
			ReadMode:   backup.ReadMode,
		}

		s.Store.ARPCAgentsManager.Expect(backup.GetStreamID())

		var err error
		respMsg, err = tcpPipe.CallMessage(ctx, "backup", &backupReq)
		if err != nil {
			log.Error(err, "")
			reply.Status = 500
			reply.Message = err.Error()
			return errors.New(reply.Message)
		}
	} else {
		reply.Status = 500
		reply.Message = "unable to reach target"
		return errors.New(reply.Message)
	}
	if err != nil {
		log.Error(err, "")
		reply.Status = 500
		reply.Message = err.Error()
		return errors.New(reply.Message)
	}

	backupRespSplit := strings.Split(respMsg, "|")
	backupMode := backupRespSplit[0]

	if len(backupRespSplit) == 2 && backupRespSplit[1] != "" {
		backup.Namespace = backupRespSplit[1]
		if err := s.Store.Database.UpdateBackup(nil, backup); err != nil {
			log.Error(err, "", "namespace", backupRespSplit[1])
		}
	}

	backupCtx, backupCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.BackupID, backupCancel)

	arpcFS := arpcfs.NewARPCFS(backupCtx, s.Store.ARPCAgentsManager, backup.GetStreamID(), args.TargetHostname, backup, backupMode)
	if arpcFS == nil {
		reply.Status = 500
		reply.Message = "failed to send create ARPCFS"
		return errors.New(reply.Message)
	}

	mntPath := filepath.Join(conf.AgentMountBasePath, args.BackupID)

	if err := arpcfs.MountARPC(arpcFS, mntPath); err != nil {
		log.Error(err, "")
		reply.Status = 500
		reply.Message = fmt.Sprintf("mount: fuse connection failed: %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	sessions.NewARPCFSMount(backup.GetStreamID(), arpcFS)

	reply.Status = 200
	reply.Message = backupMode + "|" + backup.Namespace
	reply.BackupMode = backupMode
	log.Info("mounting successful")

	return nil
}

func (s *MountRPCService) S3Backup(args *S3BackupArgs, reply *BackupReply) error {
	log.Info("received S3 backup request")

	backup, err := s.Store.Database.GetBackup(args.BackupID)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}

	secretKey, err := s.Store.Database.GetS3Secret(backup.Target.Name)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get secret key of target"
		return fmt.Errorf("backup: %w", err)
	}

	backupCtx, backupCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.BackupID, backupCancel)

	s3FS := s3fs.NewS3FS(backupCtx, backup, args.Endpoint, args.AccessKey, secretKey, args.Bucket, args.Region, args.Prefix, args.UseSSL)
	if s3FS == nil {
		reply.Status = 500
		reply.Message = "failed to send create S3FS"
		return errors.New(reply.Message)
	}

	mntPath := filepath.Join(conf.AgentMountBasePath, args.BackupID)

	if err := s3fs.MountS3(s3FS, mntPath); err != nil {
		log.Error(err, "")
		reply.Status = 500
		reply.Message = fmt.Sprintf("mount: fuse connection failed: %v", err)
		return fmt.Errorf("backup: %w", err)
	}

	sessions.NewS3FSMount(backup.GetStreamID(), s3FS)

	reply.Status = 200
	reply.Message = backup.Namespace
	log.Info("mounting successful")

	return nil
}

func (s *MountRPCService) ARPCCleanup(args *CleanupArgs, reply *CleanupReply) error {
	log.Info("received cleanup request")

	childKey := args.TargetHostname + "|" + args.BackupID
	sessions.DisconnectSession(childKey)

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	ctxCancel, ok := s.jobCtxCancels.GetAndDel(args.BackupID)
	if ok {
		ctxCancel()
	}

	s.Store.ARPCAgentsManager.NotExpect(childKey)

	// Try QUIC first, then TCP fallback
	qSess, qExists := s.Store.ARPCAgentsManager.GetQuicPipe(args.TargetHostname)
	tSess, tExists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	if !qExists && !tExists {
		log.Info("target unreachable, assuming cleanup successful.",
			"jobID", args.BackupID)

		reply.Status = 200
		reply.Message = "Target unreachable, assuming cleanup successful."
		return nil
	}

	cleanupReq := types.BackupReq{
		Drive:    args.Drive,
		BackupID: args.BackupID,
	}

	if qExists {
		_, err := qSess.CallMessage(ctx, "cleanup", &cleanupReq)
		if err != nil {
			log.Error(err, "")
			reply.Status = 500
			reply.Message = err.Error()
			return errors.New(reply.Message)
		}
	} else {
		_, err := tSess.CallMessage(ctx, "cleanup", &cleanupReq)
		if err != nil {
			log.Error(err, "")
			reply.Status = 500
			reply.Message = err.Error()
			return errors.New(reply.Message)
		}
	}

	reply.Status = 200
	reply.Message = "Cleanup successful"
	log.Info("cleanup successful",
		"backupID", args.BackupID)

	return nil
}

func (s *MountRPCService) Status(args *StatusArgs, reply *StatusReply) error {
	log.Info("received status request")

	_, qExists := s.Store.ARPCAgentsManager.GetQuicPipe(args.TargetHostname)
	_, tExists := s.Store.ARPCAgentsManager.GetStreamPipe(args.TargetHostname)
	controlOk := qExists || tExists
	if !controlOk {
		reply.Connected = false
		return nil
	}

	childKey := args.TargetHostname + "|" + args.BackupID
	_, exists := s.Store.ARPCAgentsManager.GetStreamPipe(childKey)
	if !exists {
		reply.Connected = false
		return nil
	}

	reply.Connected = true
	return nil
}

func StartRPCServer(watcher chan<- struct{}, ctx context.Context, socketPath string, storeInstance *store.Store) error {
	if err := os.RemoveAll(socketPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", socketPath, err)
	}

	service := &MountRPCService{
		ctx:           ctx,
		Store:         storeInstance,
		jobCtxCancels: safemap.New[string, context.CancelFunc](),
	}

	if err := rpc.Register(service); err != nil {
		return fmt.Errorf("failed to register rpc service: %w", err)
	}

	ready := make(chan struct{})

	go func() {
		if watcher != nil {
			defer close(watcher)
		}
		close(ready)
		rpc.Accept(listener)
	}()
	log.Info("rPC server listening",
		"socket", socketPath)

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
		log.Info("rpc mount server shutting down due to context cancellation",
			"socket", socketPath)

		if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	case <-watcher:
		log.Info("rpc mount server shut down unexpectedly",
			"socket", socketPath)

	}

	return nil
}
