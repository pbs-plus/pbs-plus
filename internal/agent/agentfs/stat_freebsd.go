//go:build freebsd

package agentfs

import (
	"golang.org/x/sys/unix"
)

func getBirthTime(st *unix.Stat_t) int64 {
	return int64(st.Btim.Sec)
}
