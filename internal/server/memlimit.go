package server

import (
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func init() {
	if _, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem),
		),
		memlimit.WithRefreshInterval(1*time.Minute),
	); err != nil {
		log.Error(err, "")
	}
}
