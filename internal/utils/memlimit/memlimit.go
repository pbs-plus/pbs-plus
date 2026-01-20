package memlimit

import (
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

func init() {
	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(memlimit.FromCgroup),
		memlimit.WithRefreshInterval(1*time.Minute),
	)
	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
		memlimit.WithRefreshInterval(1*time.Minute),
	)
	memlimit.SetGoMemLimit(0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.Limit(1024*1024), 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroup, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupV1, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupHybrid, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupV2, 0.9)
}
