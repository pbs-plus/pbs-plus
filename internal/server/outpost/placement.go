//go:build linux

package outpost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sync"
)

type Placement interface {
	Listen(ctx context.Context, o Outpost) (net.Listener, error)
}

var ErrAgentPlacementUnavailable = errors.New("agent-hosted outposts are not available")

type localPlacement struct{}

func (localPlacement) Listen(ctx context.Context, o Outpost) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", o.ListenAddr)
}

var (
	placementMu    sync.RWMutex
	agentPlacement Placement
)

func SetAgentPlacement(p Placement) {
	placementMu.Lock()
	agentPlacement = p
	placementMu.Unlock()
}

func placementOf(o Outpost) (Placement, error) {
	if o.Agent == "" {
		return localPlacement{}, nil
	}
	placementMu.RLock()
	p := agentPlacement
	placementMu.RUnlock()
	if p == nil {
		return nil, ErrAgentPlacementUnavailable
	}
	return p, nil
}

func openListener(ctx context.Context, o Outpost) (net.Listener, error) {
	p, err := placementOf(o)
	if err != nil {
		return nil, err
	}
	return p.Listen(ctx, o)
}

var agentNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)

// agentCapable lists the types that serve attached snapshots through their own listener.
var agentCapable = map[string]bool{
	TypeNFS:      true,
	TypeS3Export: true,
}

func validatePlacement(o Outpost) error {
	if o.Agent == "" {
		return nil
	}
	if !agentCapable[o.Type] {
		return fmt.Errorf("%s outposts cannot be placed on an agent", o.Type)
	}
	if !agentNameRe.MatchString(o.Agent) {
		return fmt.Errorf("invalid agent name %q", o.Agent)
	}
	return nil
}
