//go:build linux

package application

import (
	"context"
	"strings"
	"time"

	reqTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
)

// TargetService encapsulates target-related business logic.
type TargetService struct {
	db        *database.Database
	agentsMgr *arpc.AgentsManager
}

// NewTargetService creates a TargetService.
func NewTargetService(db *database.Database, agentsMgr *arpc.AgentsManager) *TargetService {
	return &TargetService{db: db, agentsMgr: agentsMgr}
}

// ListTargets returns all targets.
func (s *TargetService) ListTargets() ([]database.Target, error) {
	return s.db.GetAllTargets()
}

// TargetStatusResult holds the result of a single target status check.
type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

// CheckTargetStatus checks connectivity for all agent targets concurrently.
// Returns partial results even if some checks fail.
func (s *TargetService) CheckTargetStatus(
	ctx context.Context,
	targets []database.Target,
	checkStatus bool,
	timeout time.Duration,
) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	sem := make(chan struct{}, 20)

	done := make(chan struct{})
	go func() {
		for i, target := range targets {
			sem <- struct{}{}
			go func(idx int, tgt database.Target) {
				defer func() { <-sem }()

				result := TargetStatusResult{Index: idx}
				if !tgt.IsAgent() {
					results[idx] = result
					return
				}

				arpcSess, ok := s.agentsMgr.GetStreamPipe(tgt.GetHostname())
				if !ok {
					results[idx] = result
					return
				}

				result.AgentVersion = arpcSess.GetVersion()

				if checkStatus {
					timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()

					respMsg, err := arpcSess.CallMessage(
						timeoutCtx,
						"target_status",
						&reqTypes.TargetStatusReq{Drive: tgt.VolumeID},
					)
					if err == nil && strings.HasPrefix(respMsg, "reachable") {
						result.ConnectionStatus = true
						if parts := strings.Split(respMsg, "|"); len(parts) > 1 {
							result.AgentVersion = parts[1]
						}
					} else if err != nil {
						result.Error = err
					}
				}

				results[idx] = result
			}(i, target)
		}
		// Drain semaphore to wait for all goroutines
		for range len(targets) {
			sem <- struct{}{}
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		return results
	case <-done:
		return results
	}
}
