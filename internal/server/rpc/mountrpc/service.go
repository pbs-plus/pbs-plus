//go:build linux

package mountrpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/rpcserver"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

const ServiceName = "MountRPCService"

type BackupArgs struct {
	BackupID       string
	TargetHostname string
	Drive          string
	MountID        string
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
	MountID      string
}

type BackupReply struct {
	Status     int
	Message    string
	BackupMode string
}

type StatusArgs struct {
	BackupID       string
	TargetHostname string
	MountID        string
}

// StatusReply exposes Activity, a monotonic filesystem-operation count, because Connected alone cannot distinguish a working run from a wedged one.
type StatusReply struct {
	Connected bool
	Activity  int64
}

type CleanupArgs struct {
	BackupID       string
	TargetHostname string
	Drive          string
	MountID        string
}

type CleanupReply struct {
	Status  int
	Message string
}

type Service struct {
	ctx   context.Context
	Store *application.Runtime
}

type agentCaller interface {
	CallMessage(context.Context, string, any) (string, error)
}

func (s *Service) agent(host string) agentCaller {
	if pipe, ok := s.Store.Agents.GetQuicPipe(host); ok {
		return pipe
	}
	if pipe, ok := s.Store.Agents.GetStreamPipe(host); ok {
		return pipe
	}
	return nil
}

func (s *Service) reserve(streamID, owner string, reply *BackupReply) (*sessions.FSMount, error) {
	if owner == "" {
		reply.Status = 400
		reply.Message = "mount ownership id is required"
		return nil, errors.New(reply.Message)
	}
	m, err := sessions.ReserveMount(s.ctx, streamID, owner)
	if err != nil {
		reply.Status = 409
		reply.Message = err.Error()
	}
	return m, err
}

func (s *Service) finishSetup(m *sessions.FSMount, reply *BackupReply) {
	m.FinishSetup()
	if reply.Status == 200 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 30*time.Second)
	defer cancel()
	if err := m.Disconnect(ctx); err != nil {
		log.Error(err, "failed mount cleanup retained for retry")
	}
}

func removeMountDirectory(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Service) Backup(args *BackupArgs, reply *BackupReply) error {
	backup, err := s.Store.CoreDB.GetBackup(args.BackupID)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get backup from id"
		return fmt.Errorf("backup: %w", err)
	}
	streamID := backup.GetStreamID()
	m, err := s.reserve(streamID, args.MountID, reply)
	if err != nil {
		return err
	}
	defer s.finishSetup(m, reply)
	backupCtx := m.Context()
	mntPath := filepath.Join(conf.AgentMountBasePath, args.BackupID)
	m.SetCleanup(func(ctx context.Context) error {
		if caller := s.agent(args.TargetHostname); caller != nil {
			if _, err := caller.CallMessage(ctx, "cleanup", &fswire.BackupReq{Drive: args.Drive, BackupID: args.BackupID}); err != nil {
				return err
			}
		}
		s.Store.Agents.NotExpect(streamID)
		return removeMountDirectory(mntPath)
	})
	caller := s.agent(args.TargetHostname)
	if caller == nil {
		reply.Status = 500
		reply.Message = "unable to reach target"
		return errors.New(reply.Message)
	}
	ctx, cancel := context.WithTimeout(backupCtx, 5*time.Minute)
	defer cancel()
	s.Store.Agents.Expect(streamID)
	respMsg, err := caller.CallMessage(ctx, "backup", &fswire.BackupReq{
		Drive: args.Drive, BackupID: args.BackupID, SourceMode: backup.SourceMode, ReadMode: backup.ReadMode,
	})
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return err
	}
	parts := strings.SplitN(respMsg, "|", 2)
	backupMode := parts[0]
	if len(parts) == 2 && parts[1] != "" {
		backup.Namespace = parts[1]
		if err := s.Store.CoreDB.UpdateBackupNamespace(backup.ID, backup.Namespace); err != nil {
			log.Error(err, "failed to update backup namespace")
		}
	}
	if err := os.MkdirAll(mntPath, 0700); err != nil {
		return err
	}
	fs := arpcfs.NewARPCFS(backupCtx, s.Store.Agents, streamID, args.TargetHostname, backup, backupMode)
	if fs == nil {
		return errors.New("failed to create ARPC filesystem")
	}
	if err := m.AttachARPCFS(fs); err != nil {
		return errors.Join(err, fs.Unmount(ctx))
	}
	if err := arpcfs.MountARPC(fs, mntPath); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	reply.Status = 200
	reply.Message = backupMode + "|" + backup.Namespace
	reply.BackupMode = backupMode
	return nil
}

func (s *Service) S3Backup(args *S3BackupArgs, reply *BackupReply) error {
	backup, err := s.Store.CoreDB.GetBackup(args.BackupID)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	secretKey, err := s.Store.CoreDB.GetS3Secret(backup.Target.Name)
	if err != nil {
		return fmt.Errorf("secret: %w", err)
	}
	m, err := s.reserve(backup.GetStreamID(), args.MountID, reply)
	if err != nil {
		return err
	}
	defer s.finishSetup(m, reply)
	backupCtx := m.Context()
	mntPath := filepath.Join(conf.AgentMountBasePath, args.BackupID)
	m.SetCleanup(func(context.Context) error {
		return removeMountDirectory(mntPath)
	})
	if err := os.MkdirAll(mntPath, 0700); err != nil {
		return err
	}
	fs := s3fs.NewS3FS(backupCtx, backup, args.Endpoint, args.AccessKey, secretKey, args.Bucket, args.Region, args.Prefix, args.UseSSL, args.UsePathStyle)
	if fs == nil {
		return errors.New("failed to create S3 filesystem")
	}
	if err := m.AttachS3FS(fs); err != nil {
		return errors.Join(err, fs.Unmount(s.ctx))
	}
	if err := s3fs.MountS3(fs, mntPath); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	reply.Status = 200
	reply.Message = backup.Namespace
	return nil
}

func (s *Service) ARPCCleanup(args *CleanupArgs, reply *CleanupReply) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 30*time.Second)
	defer cancel()
	if args.MountID == "" {
		return errors.New("mount ownership id is required")
	}
	if err := sessions.DisconnectSession(ctx, args.TargetHostname+"|"+args.BackupID, args.MountID); err != nil {
		return err
	}
	reply.Status = 200
	reply.Message = "Cleanup successful"
	return nil
}

func (s *Service) S3Cleanup(args *CleanupArgs, reply *CleanupReply) error {
	return s.ARPCCleanup(args, reply)
}

func (s *Service) Status(args *StatusArgs, reply *StatusReply) error {
	childKey := args.TargetHostname + "|" + args.BackupID
	if !sessions.OwnsMount(childKey, args.MountID) || s.agent(args.TargetHostname) == nil {
		return nil
	}
	_, reply.Connected = s.Store.Agents.GetStreamPipe(childKey)
	if fs := sessions.GetSessionARPCFS(childKey); fs != nil {
		reply.Activity = fs.FileCount.Value() + fs.FolderCount.Value() + fs.TotalBytes.Value()
	}
	return nil
}

func RunServer(ctx context.Context, socketPath string, app *application.Runtime) error {
	vfs.ReapStaleMemcachedSockets()
	return rpcserver.Run(ctx, socketPath, ServiceName, &Service{ctx: ctx, Store: app})
}
