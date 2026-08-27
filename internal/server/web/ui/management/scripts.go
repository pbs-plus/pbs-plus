package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const scriptAPIPath = "/api2/json/d2d/script"

var scriptModel = js.Model{
	Name:       "pbs-model-scripts",
	Fields:     js.Fields("path", "description", "job_count", "target_count"),
	IDProperty: "path",
}

var scriptSelector = js.Selector{
	Name:         "PBS.form.D2DScriptSelector",
	XType:        "pbsD2DScriptSelector",
	DisplayField: "path",
	ValueField:   "path",
	APIPath:      scriptAPIPath,
	Sorters:      "path",
	AllowBlank:   new(true),
	AutoSelect:   new(false),
	ListWidth:    450,
	ListColumns: []js.Column{
		{Text: "Path", DataIndex: "path", Flex: 3, Sortable: new(true), Renderer: "Ext.String.htmlEncode"},
		{Text: "Description", DataIndex: "description", Flex: 3, Sortable: new(true), Renderer: "Ext.String.htmlEncode"},
	},
}

var scriptPanel = js.Panel{
	Name:    "PBS.D2DManagement.ScriptPanel",
	XType:   "pbsDiskScriptPanel",
	StateID: "grid-disk-backup-scripts-v1",
	Store: js.Store{
		StoreID: "proxmox-disk-scripts",
		Model:   "pbs-model-scripts",
		APIPath: scriptAPIPath,
		Sorters: "path",
	},
	Listeners: js.Listeners{ItemDblClick: "onEdit"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":  js.OpenEditWindow("PBS.D2DManagement.ScriptEditWindow", ""),
		"onEdit": js.OpenEditWindow("PBS.D2DManagement.ScriptEditWindow", "path"),
		"removeScripts": js.ConfirmRemove(
			"/api2/extjs/config/d2d-script/",
			"encodePathValue(rec.getId())",
			"Remove selected entries?",
		),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)},
		js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Remove", Handler: "removeScripts", Disabled: true, EnableFn: js.EnableOnSelection},
	},
	Columns: []js.Column{
		{Text: "Path", DataIndex: "path", Flex: 2},
		{Text: "Description", DataIndex: "description", Flex: 1},
		{Text: "Job Count", DataIndex: "job_count", Flex: 1},
		{Text: "Target Count", DataIndex: "target_count", Flex: 1},
	},
}

var scriptEditWindow = js.EditWindow{
	Name:      "PBS.D2DManagement.ScriptEditWindow",
	XType:     "pbsScriptEditWindow",
	Subject:   "Script",
	Width:     "80%",
	Resizable: true,
	IsCreate:  true,
	IsAdd:     true,
	CBindData: js.PathKeyedURL("/api2/extjs/config/d2d-script"),
	Items: js.Items(
		js.Field{
			XType:      js.XDisplayEditField,
			Label:      "Description",
			Name:       "description",
			Renderer:   "Ext.htmlEncode",
			AllowBlank: new(true),
			Editable:   new(true),
		},
		js.Field{
			XType:  js.XFieldContainer,
			Label:  "Script Content",
			Layout: "anchor",
			Items:  js.Items(js.CodeMirrorField("script", "shell", 400)),
		},
	),
	Methods: map[string]js.Raw{
		"getValues": js.Func("", `
			let values = this.callParent();
			let editor = this.down("#scriptEditor");
			if (editor && editor.codeMirror) {
				values.script = editor.codeMirror.getValue();
			}
			return values;
		`),
		"setValues": js.Func("values", `
			this.callParent([values]);
			let editor = this.down("#scriptEditor");
			if (editor && editor.codeMirror && values.script) {
				editor.codeMirror.setValue(values.script);
			}
		`),
	},
}
