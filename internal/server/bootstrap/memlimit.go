package bootstrap

import (
	"context"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func setMemLimit(ctx context.Context) {
	if _, err := memlimit.Set(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem),
		),
		memlimit.WithRefreshInterval(ctx, time.Minute),
	); err != nil {
		log.Error(err, "")
	}
}
