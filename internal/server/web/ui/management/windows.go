package management

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var windows = []js.Value{
	js.EditWindow{
		Name: "PBS.D2DManagement.ExclusionEditWindow", XType: "pbsExclusionEditWindow",
		Subject: "Disk Backup Global Path Exclusion", IsCreate: true, IsAdd: true,
		CBindData: js.PathKeyedURL("/api2/extjs/config/d2d-exclusion"),
		Items: js.Items(
			js.Field{XType: js.XDisplayEditField, Label: "Path", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true},
			js.Field{XType: "proxmoxtextfield", Label: "Comment", Name: "comment", DeleteEmptyWhenNotCreate: true},
		),
	},
	js.EditWindow{
		Name: "PBS.D2DManagement.TokenEditWindow", XType: "pbsTokenEditWindow",
		Subject: "Agent Bootstrap Token", IsCreate: true, IsAdd: true,
		CBindData: js.PathKeyedURL("/api2/extjs/config/d2d-token"),
		Items: js.Items(
			js.Field{XType: js.XDisplayEditField, Label: "Duration", Name: "duration", Renderer: "Ext.htmlEncode", AllowBlank: new(true), EmptyText: "24h", EditableWhenCreate: true},
			js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Value: js.T("Format: Use a Go duration string (e.g., '2h', '30m', '1h30m'). Use '0' for a token that never expires.")},
			js.Field{XType: js.XDisplayEditField, Label: "Comment", Name: "comment", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true},
		),
	},
	js.EditWindow{
		Name: "PBS.D2DManagement.TargetEditWindow", XType: "pbsTargetEditWindow",
		Subject: "Disk Backup Target", IsCreate: true, IsAdd: true,
		CBindData: js.PathKeyedURL("/api2/extjs/config/d2d-target"),
		Items: js.Items(
			js.Field{XType: js.XDisplayEditField, Label: "Name", Name: "name", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true},
			js.Field{XType: js.XDisplayEditField, Label: "Path", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true},
			js.Field{XType: "pbsD2DScriptSelector", Label: "Mount Script", Name: "mount_script"},
		),
	},
	js.EditWindow{
		Name: "PBS.D2DManagement.TargetS3Secret", XType: "pbsTargetS3Secret", Extend: "PBS.plusWindow.Create",
		Subject: "Set Target S3 Secret Key", PixelWidth: 400, NotResizable: true, IsCreate: true, Method: "POST",
		CBindData: js.Func("initialConfig", `
			let contentid = initialConfig.contentid;
			this.url = "/api2/extjs/config/d2d-target/" + encodeURIComponent(encodePathValue(contentid)) + "/s3-secret";
			return {};
		`),
		Items: js.Items(js.Panel{Extend: js.ExtInputPanel, Padding: 10, Items: js.Items(
			js.Field{XType: "proxmoxtextfield", Name: "secret", Label: "Secret Key", InputType: "password", AllowBlank: new(false), EmptyText: "Enter S3 Secret Key"},
		)}),
	},
}
