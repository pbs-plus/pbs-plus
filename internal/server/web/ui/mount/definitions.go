package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// Definitions renders the snapshot-mount datastore tree panel.
func Definitions() []js.Value {
	return []js.Value{mountPanel}
}
