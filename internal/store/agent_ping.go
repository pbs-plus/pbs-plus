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

	storeInstance.WSHub.ClientsMux.RLock()
	if client, ok := storeInstance.WSHub.Clients[agentHostname]; ok && client != nil {
		storeInstance.WSHub.ClientsMux.RUnlock()
		return true
	}
	storeInstance.WSHub.ClientsMux.RUnlock()

	return false
}
