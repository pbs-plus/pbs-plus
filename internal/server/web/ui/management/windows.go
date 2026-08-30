package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

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
		CBindData: js.Func("initialConfig", `
			let contentid = initialConfig.contentid;
			this.isCreate = !contentid;
			this.url = contentid
				? "/api2/extjs/config/d2d-target/" + encodeURIComponent(encodePathValue(contentid))
				: "/api2/extjs/config/d2d-target";
			this.method = contentid ? "PUT" : "POST";
			return { targetKind: initialConfig.targetKind || "filesystem" };
		`),
		Items: js.Items(
			js.Field{XType: js.XDisplayEditField, Label: "Name", Name: "name", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true},
			js.Field{XType: js.XDisplayField, Label: "Type", Name: "kind", SubmitValue: true,
				Renderer: js.Func("kind", `
					return { filesystem: "Filesystem", s3: "S3" }[kind] || kind;
				`),
				CBind: js.Obj{"value": "{targetKind}"}, ChangeFn: js.Func("field, kind", `
					let window = field.up("window");
					if (!window) {
						return;
					}
					["filesystem", "s3"].forEach(function (candidate) {
						let group = window.down("#" + candidate + "TargetFields");
						let active = candidate === kind;
						group.setHidden(!active);
						group.setDisabled(!active);
					});
				`)},
			js.Field{XType: js.XFieldContainer, ItemID: "filesystemTargetFields", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XDisplayField, Label: "Access", Name: "access", Value: "local", SubmitValue: true},
				js.Field{XType: js.XDisplayEditField, Label: "Path", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true,
					EmptyText: "/mnt/backup"},
			)},
			js.Field{XType: js.XFieldContainer, ItemID: "s3TargetFields", Layout: "anchor", Hidden: true, Disabled: true, Items: js.Items(
				js.Field{XType: js.XDisplayEditField, Label: "S3 URL", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true,
					EmptyText: "https://access-key@endpoint/bucket"},
			)},
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
