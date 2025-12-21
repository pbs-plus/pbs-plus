package rpcmount

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type RestoreArgs struct {
	RestoreId      string
	TargetHostname string
}

type RestoreReply struct {
	Status  int
	Message string
}

func (s *MountRPCService) Restore(args *RestoreArgs, reply *RestoreReply) error {
	syslog.L.Info().
		WithMessage("Received restore request").
		WithFields(map[string]any{
			"restoreId": args.RestoreId,
			"target":    args.TargetHostname,
		}).Write()

	// Retrieve the job from the database.
	restore, err := s.Store.Database.GetRestore(args.RestoreId)
	if err != nil {
		reply.Status = 404
		reply.Message = "unable to get restore job from id"
		return fmt.Errorf("restore: %w", err)
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

	// Prepare the restore request (using the types.RestoreReq structure).
	restoreReq := types.RestoreReq{
		RestoreId: restore.ID,
		SrcPath:   restore.SrcPath,
		DestPath:  restore.DestPath,
	}

	// Call the target's restore method via ARPC.
	_, err = arpcSess.CallMessage(ctx, "restore", &restoreReq)
	if err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = err.Error()
		return errors.New(reply.Message)
	}

	// The child session key is "targetHostname|restoreId|restore".
	childKey := args.TargetHostname + "|" + args.RestoreId + "|restore"

	agentRPC, exists := s.Store.ARPCAgentsManager.GetStreamPipe(childKey)
	if !exists {
		reply.Status = 500
		reply.Message = "unable to reach child target"
		return errors.New(reply.Message)
	}

	_, jobCancel := context.WithCancel(s.ctx)
	s.jobCtxCancels.Set(args.RestoreId, jobCancel)

	socketPath := filepath.Join(constants.RestoreSocketPath, strings.ReplaceAll(childKey, "|", "-")+".sock")
	reader, err := pxar.NewPxarReader(socketPath, restore.Store, restore.Namespace, restore.Snapshot)
	if err != nil {
		reply.Status = 500
		reply.Message = "failed to create pxar reader"
		return errors.New(reply.Message)
	}

	srv := pxar.NewRemoteServer(reader)
	agentRPC.SetRouter(*srv.Router())

	store.CreatePxarReader(childKey, reader)
	// TODO: ensure to disconnect on cleanup

	_, err = agentRPC.CallMessage(ctx, "server_ready", &restoreReq)
	if err != nil {
		syslog.L.Error(err).Write()
		reply.Status = 500
		reply.Message = err.Error()
		return errors.New(reply.Message)
	}

	reply.Status = 200
	reply.Message = "success"

	syslog.L.Info().
		WithMessage("Restore request sent").
		WithFields(map[string]any{
			"restoreId": args.RestoreId,
		}).Write()

	return nil
}
