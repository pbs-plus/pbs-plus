//go:build linux

package backup

import (
	"context"

	"github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

type Manager struct {
	*jobs.Manager
}

func NewManager(ctx context.Context) *Manager {
	return &Manager{
		Manager: jobs.NewManager(ctx, utils.MaxConcurrentClients, 100, true),
	}
}
