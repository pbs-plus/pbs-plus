package mtf

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var mtfDriveGrid = js.Panel{
	Name: "PBS.MtfManagement.DriveGrid", XType: "pbsMtfDriveGrid",
	Store:     js.Store{StoreID: "proxmox-tape-drives", Model: "pbs-model-drives", APIPath: "/api2/json/tape/drive", Sorters: "name", GroupField: "changer", Proxy: js.ProxyProxmox, QueryParamNull: true},
	Grouping:  &js.Grouping{HeaderTemplate: js.GroupHeader("Drive"), FormatName: js.Func("changer", `if (!changer) return gettext("Standalone Drives"); return Ext.String.format(gettext("Changer {0}"), changer);`)},
	Listeners: js.Listeners{ItemDblClick: "showStatus"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":      js.OpenEditWindow("PBS.TapeManagement.DriveEditWindow", ""),
		"onEdit":     js.EditSelection("PBS.TapeManagement.DriveEditWindow", "driveid", "name", "autoLoad"),
		"showStatus": js.OpenStatusPage("Drive"),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Status", Handler: "showStatus", Disabled: true, IconCls: "fa fa-window-restore"},
		{StandardRemoveBaseURL: "/api2/extjs/config/drive", Callback: "reload"},
	},
	Columns: []js.Column{
		{Text: "Name", DataIndex: "name", Flex: 1}, {Text: "Path", DataIndex: "path", Flex: 2},
		{Text: "Vendor", DataIndex: "vendor", Flex: 1}, {Text: "Model", DataIndex: "model", Flex: 1},
		{Text: "Serial", DataIndex: "serial", Flex: 1},
		{Text: "Drive Number", DataIndex: "changer-drivenum", Renderer: js.Func("value, mD, record", `return record.data.changer ? value : "";`)},
	},
}
