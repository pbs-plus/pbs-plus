//go:build linux

package store

import (
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

func (storeInstance *Store) AgentPing(agentTarget *Target) (bool, error) {
	splittedName := strings.Split(agentTarget.Name, " - ")
	agentHostname := splittedName[0]

	broadcast, err := storeInstance.WSHub.SendCommandWithBroadcast(agentHostname, websockets.Message{
		Type:    "ping",
		Content: "ping",
	})
	if err != nil {
		return false, err
	}

	listener := broadcast.Subscribe()
	defer broadcast.CancelSubscription(listener)

	for {
		select {
		case resp := <-listener:
			if resp.Type == "ping" && resp.Content == "pong" {
				return true, nil
			}
		case <-time.After(time.Second * 2):
			return false, nil
		}
	}
}
