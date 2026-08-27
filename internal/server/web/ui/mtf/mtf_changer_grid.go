package mtf

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var mtfChangerGrid = js.Panel{
	Name: "PBS.MtfManagement.ChangerGrid", XType: "pbsMtfChangerGrid",
	Store:     js.Store{StoreID: "proxmox-tape-changers", Model: "pbs-model-changers", APIPath: "/api2/json/tape/changer", Sorters: "name", Proxy: js.ProxyProxmox, QueryParamNull: true},
	Listeners: js.Listeners{ItemDblClick: "onDblClick"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":      js.OpenEditWindow("PBS.TapeManagement.ChangerEditWindow", ""),
		"onEdit":     js.EditSelection("PBS.TapeManagement.ChangerEditWindow", "changerid", "name", "autoLoad"),
		"showStatus": js.OpenStatusPage("Changer"),
		"onDblClick": js.Func("", `this.showStatus();`),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Status", Handler: "showStatus", Disabled: true, IconCls: "fa fa-window-restore"},
		{StandardRemoveBaseURL: "/api2/extjs/config/changer", Callback: "reload"},
	},
	Columns: []js.Column{
		{Text: "Name", DataIndex: "name", Flex: 1}, {Text: "Path", DataIndex: "path", Flex: 2},
		{Text: "Vendor", DataIndex: "vendor", Flex: 1}, {Text: "Model", DataIndex: "model", Flex: 1},
		{Text: "Serial", DataIndex: "serial", Flex: 1}, {Text: "Import/Export Slots", DataIndex: "export-slots", Flex: 1},
	},
}
