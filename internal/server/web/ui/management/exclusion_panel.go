package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var exclusionPanel = js.Panel{
	Name: "PBS.D2DManagement.ExclusionPanel", XType: "pbsDiskExclusionPanel",
	Store:     js.Store{StoreID: "proxmox-disk-exclusions", Model: "pbs-model-exclusions", APIPath: "/api2/json/d2d/exclusion", Sorters: "name"},
	Listeners: js.Listeners{ItemDblClick: "onEdit"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":            js.OpenEditWindow("PBS.D2DManagement.ExclusionEditWindow", ""),
		"onEdit":           js.OpenEditWindow("PBS.D2DManagement.ExclusionEditWindow", "path"),
		"removeExclusions": js.ConfirmRemove("/api2/extjs/config/d2d-exclusion/", "encodePathValue(rec.getId())", "Remove selected entries?"),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Remove", Handler: "removeExclusions", Disabled: true, EnableFn: js.EnableOnSelection},
	},
	Columns: []js.Column{
		{Text: "Path", DataIndex: "path", Flex: 1},
		{Text: "Comment", DataIndex: "comment", Flex: 2},
	},
}
