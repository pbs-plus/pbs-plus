// Package ui defines the ExtJS additions injected into Proxmox Backup Server.
package ui

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
	"github.com/pbs-plus/pbs-plus/internal/server/web/ui/management"
	"github.com/pbs-plus/pbs-plus/internal/server/web/ui/mount"
	"github.com/pbs-plus/pbs-plus/internal/server/web/ui/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/web/ui/tape"
	"github.com/pbs-plus/pbs-plus/internal/server/web/ui/verification"
)

func Render() []byte {
	items := management.Definitions()
	items = append(items, mount.Definitions()...)
	items = append(items, verification.Definitions()...)
	items = append(items, mtf.Definitions()...)
	items = append(items, tape.Definitions()...)
	items = append(items, coreViews...)
	return js.Render(items...)
}
