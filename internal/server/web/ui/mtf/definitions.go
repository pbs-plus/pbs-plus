package mtf

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// Definitions renders every MTF ExtJS definition: models, grids and the job window.
func Definitions() []js.Value {
	items := append([]js.Value{}, mtfModels...)
	items = append(items, mtfChangerGrid, mtfDriveGrid)
	items = append(items, mtfJobPanel, mtfInventoryPanel, mtfMappingPanel)
	items = append(items, mtfJobEdit)
	return items
}
