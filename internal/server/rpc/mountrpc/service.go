//go:build linux

package mountrpc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/rpcserver"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	s3fs "github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

const ServiceName = "MountRPCService"

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

type Service struct {
	ctx           context.Context
	Store         *application.Runtime
	jobCtxCancels *safemap.Map[string, context.CancelFunc]
}

func (s *Service) Backup(args *BackupArgs, reply *BackupReply) error {
	log.Info("received backup request")

	backup, err := s.Store.CoreDB.GetBackup(args.BackupID)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()

	// Retrieve the ARPC session for the target (QUIC preferred, TCP fallback).
	var respMsg string
	if qPipe, ok := s.Store.Agents.GetQuicPipe(args.TargetHostname); ok {
		backupReq := fswire.BackupReq{
			Drive:      args.Drive,
			BackupID:   args.BackupID,
			SourceMode: backup.SourceMode,
			ReadMode:   backup.ReadMode,
		}

		s.Store.Agents.Expect(backup.GetStreamID())

		var err error
		respMsg, err = qPipe.CallMessage(ctx, "backup", &backupReq)
		if err != nil {
			log.Error(err, "")
			reply.Status = 500
			reply.Message = err.Error()
			return errors.New(reply.Message)
		}
	} else if tcpPipe, ok := s.Store.Agents.GetStreamPipe(args.TargetHostname); ok {
		backupReq := fswire.BackupReq{
			Drive:      args.Drive,
			BackupID:   args.BackupID,
			SourceMode: backup.SourceMode,
			ReadMode:   backup.ReadMode,
		}

		s.Store.Agents.Expect(backup.GetStreamID())

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
		if err := s.Store.CoreDB.UpdateBackup(nil, backup); err != nil {
			log.Error(err, "", "namespace", backupRespSplit[1])
		}
	}

	backupCtx, backupCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.BackupID, backupCancel)

	arpcFS := arpcfs.NewARPCFS(backupCtx, s.Store.Agents, backup.GetStreamID(), args.TargetHostname, backup, backupMode)
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

func (s *Service) S3Backup(args *S3BackupArgs, reply *BackupReply) error {
	log.Info("received S3 backup request")

	backup, err := s.Store.CoreDB.GetBackup(args.BackupID)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}

	secretKey, err := s.Store.CoreDB.GetS3Secret(backup.Target.Name)
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

func (s *Service) ARPCCleanup(args *CleanupArgs, reply *CleanupReply) error {
	log.Info("received cleanup request")

	childKey := args.TargetHostname + "|" + args.BackupID
	sessions.DisconnectSession(childKey)

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	ctxCancel, ok := s.jobCtxCancels.GetAndDel(args.BackupID)
	if ok {
		ctxCancel()
	}

	s.Store.Agents.NotExpect(childKey)

	// Try QUIC first, then TCP fallback
	qSess, qExists := s.Store.Agents.GetQuicPipe(args.TargetHostname)
	tSess, tExists := s.Store.Agents.GetStreamPipe(args.TargetHostname)
	if !qExists && !tExists {
		log.Info("target unreachable, assuming cleanup successful.",
			"jobID", args.BackupID)

		reply.Status = 200
		reply.Message = "Target unreachable, assuming cleanup successful."
		return nil
	}

	cleanupReq := fswire.BackupReq{
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

func (s *Service) Status(args *StatusArgs, reply *StatusReply) error {
	log.Info("received status request")

	_, qExists := s.Store.Agents.GetQuicPipe(args.TargetHostname)
	_, tExists := s.Store.Agents.GetStreamPipe(args.TargetHostname)
	controlOk := qExists || tExists
	if !controlOk {
		reply.Connected = false
		return nil
	}

	childKey := args.TargetHostname + "|" + args.BackupID
	_, exists := s.Store.Agents.GetStreamPipe(childKey)
	if !exists {
		reply.Connected = false
		return nil
	}

	reply.Connected = true
	return nil
}

func RunServer(ctx context.Context, socketPath string, app *application.Runtime) error {
	return rpcserver.Run(ctx, socketPath, ServiceName, &Service{
		ctx:           ctx,
		Store:         app,
		jobCtxCancels: safemap.New[string, context.CancelFunc](),
	})
}
