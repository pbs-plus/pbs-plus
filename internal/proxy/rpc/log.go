//go:build linux
// +build linux

package rpcmount

import (
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type LogArgs struct {
	JobId   string
	Message string
	Error   error
	Level   string
	Fields  map[string]interface{}
}

type LogReply struct {
	Status int
}

func (s *MountRPCService) Log(args *LogArgs, reply *LogReply) error {
	switch args.Level {
	case "info":
		syslog.L.Info().WithMessage(args.Message).WithFields(args.Fields).WithJob(args.JobId).Write()
	case "warn":
		syslog.L.Warn().WithMessage(args.Message).WithFields(args.Fields).WithJob(args.JobId).Write()
	case "error":
		syslog.L.Error(args.Error).WithMessage(args.Message).WithFields(args.Fields).WithJob(args.JobId).Write()
	default:
		syslog.L.Info().WithMessage(args.Message).WithFields(args.Fields).WithJob(args.JobId).Write()
	}

	reply.Status = 200

	return nil
}
