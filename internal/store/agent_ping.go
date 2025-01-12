//go:build linux

package store

import (
	"strings"
)

func (storeInstance *Store) AgentPing(agentTarget *Target) bool {
	splittedName := strings.Split(agentTarget.Name, " - ")
	agentHostname := splittedName[0]

	if storeInstance.WSHub == nil {
		return false
	}

	return storeInstance.WSHub.IsClientConnected(agentHostname)
}
