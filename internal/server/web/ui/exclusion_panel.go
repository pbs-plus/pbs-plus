package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var exclusionPanel = js.Panel{
	Name: "PBS.D2DManagement.ExclusionPanel", XType: "pbsDiskExclusionPanel",
	Store:     js.Store{StoreID: "proxmox-disk-exclusions", Model: "pbs-model-exclusions", APIPath: "/api2/json/d2d/exclusion", Sorters: "name"},
	Listeners: js.Listeners{ItemDblClick: "onEdit"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":            openEditWindow("PBS.D2DManagement.ExclusionEditWindow", ""),
		"onEdit":           openEditWindow("PBS.D2DManagement.ExclusionEditWindow", "path"),
		"removeExclusions": confirmRemove("/api2/extjs/config/d2d-exclusion/", "encodePathValue(rec.getId())", "Remove selected entries?"),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Remove", Handler: "removeExclusions", Disabled: true, EnableFn: enableOnSelection},
	},
	Columns: []js.Column{
		{Text: "Path", DataIndex: "path", Flex: 1},
		{Text: "Comment", DataIndex: "comment", Flex: 2},
	},
}
