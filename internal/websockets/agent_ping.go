//go:build linux

package websockets

import (
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func (server *Server) AgentPing(agentTarget *types.Target) bool {
	splittedName := strings.Split(agentTarget.Name, " - ")
	agentHostname := splittedName[0]

	return server.IsClientConnected(agentHostname)
}
