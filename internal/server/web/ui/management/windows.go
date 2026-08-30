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
					return { filesystem: "Filesystem", s3: "S3", postgresql: "PostgreSQL", mysql: "MySQL / MariaDB" }[kind] || kind;
				`),
				CBind: js.Obj{"value": "{targetKind}"}, ChangeFn: js.Func("field, kind", `
					let window = field.up("window");
					if (!window) {
						return;
					}
					["filesystem", "s3", "postgresql", "mysql"].forEach(function (candidate) {
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
				js.Field{XType: "pbsD2DScriptSelector", Label: "Mount Script", Name: "mount_script"},
			)},
			js.Field{XType: js.XFieldContainer, ItemID: "s3TargetFields", Layout: "anchor", Hidden: true, Disabled: true, Items: js.Items(
				js.Field{XType: js.XDisplayEditField, Label: "S3 URL", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true,
					EmptyText: "https://access-key@endpoint/bucket"},
			)},
			js.Field{XType: js.XFieldContainer, ItemID: "postgresqlTargetFields", Layout: "anchor", Hidden: true, Disabled: true, Items: js.Items(
				js.Field{XType: "proxmoxtextfield", Label: "Host", Name: "database_host", AllowBlank: new(false)},
				js.Field{XType: js.XIntegerField, Label: "Port", Name: "database_port", Value: 5432, MinValue: 1, MaxValue: 65535, AllowBlank: new(false)},
				js.Field{XType: "proxmoxtextfield", Label: "Username", Name: "database_username", AllowBlank: new(false)},
				js.Field{XType: js.XKVComboBox, Label: "TLS Mode", Name: "database_tls_mode", Value: "prefer", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"disable", "Disable"}, js.Arr{"allow", "Allow"}, js.Arr{"prefer", "Prefer"}, js.Arr{"require", "Require"}, js.Arr{"verify-ca", "Verify CA"}, js.Arr{"verify-full", "Verify full"},
				}},
				js.Field{XType: "proxmoxtextfield", Label: "CA Certificate", Name: "database_ca_certificate", EmptyText: "/etc/ssl/certs/database-ca.pem"},
				js.Field{XType: js.XDisplayField, Name: "database_default_client_family", Value: "postgresql", SubmitValue: true, Hidden: true},
				js.Field{XType: "pbsD2DDatabaseClientSelector", Label: "Client Version", Name: "database_default_client_dir", AllowBlank: new(false), CBind: js.Obj{"engine": "{targetKind}"}},
				js.Field{XType: "proxmoxtextfield", Label: "Password", Name: "database_password", InputType: "password", AllowBlank: new(true), EmptyText: "Leave blank to keep the current password", CBind: js.Obj{"allowBlank": "{!isCreate}"}},
			)},
			js.Field{XType: js.XFieldContainer, ItemID: "mysqlTargetFields", Layout: "anchor", Hidden: true, Disabled: true, Items: js.Items(
				js.Field{XType: js.XKVComboBox, Label: "Server", Name: "database_variant", Value: "mysql", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"mysql", "MySQL"}, js.Arr{"mariadb", "MariaDB"},
				}},
				js.Field{XType: "proxmoxtextfield", Label: "Host", Name: "database_host", AllowBlank: new(false)},
				js.Field{XType: js.XIntegerField, Label: "Port", Name: "database_port", Value: 3306, MinValue: 1, MaxValue: 65535, AllowBlank: new(false)},
				js.Field{XType: "proxmoxtextfield", Label: "Username", Name: "database_username", AllowBlank: new(false)},
				js.Field{XType: js.XKVComboBox, Label: "TLS Mode", Name: "database_tls_mode", Value: "preferred", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"disabled", "Disabled"}, js.Arr{"preferred", "Preferred"}, js.Arr{"required", "Required"}, js.Arr{"verify_ca", "Verify CA"}, js.Arr{"verify_identity", "Verify identity"},
				}},
				js.Field{XType: "proxmoxtextfield", Label: "CA Certificate", Name: "database_ca_certificate", EmptyText: "/etc/ssl/certs/database-ca.pem"},
				js.Field{XType: js.XKVComboBox, Label: "Client Family", Name: "database_default_client_family", Value: "mysql", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"mysql", "MySQL"}, js.Arr{"mariadb", "MariaDB"},
				}},
				js.Field{XType: "pbsD2DDatabaseClientSelector", Label: "Client Version", Name: "database_default_client_dir", AllowBlank: new(false), CBind: js.Obj{"engine": "{targetKind}"}},
				js.Field{XType: "proxmoxtextfield", Label: "Password", Name: "database_password", InputType: "password", AllowBlank: new(true), EmptyText: "Leave blank to keep the current password", CBind: js.Obj{"allowBlank": "{!isCreate}"}},
			)},
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
	js.EditWindow{
		Name: "PBS.D2DManagement.TargetDatabasePassword", XType: "pbsTargetDatabasePassword", Extend: "PBS.plusWindow.Create",
		Subject: "Set Database Password", PixelWidth: 400, NotResizable: true, IsCreate: true, Method: "POST",
		CBindData: js.Func("initialConfig", `
			let contentid = initialConfig.contentid;
			this.url = "/api2/extjs/config/d2d-target/" + encodeURIComponent(encodePathValue(contentid)) + "/database-password";
			return {};
		`),
		Items: js.Items(js.Panel{Extend: js.ExtInputPanel, Padding: 10, Items: js.Items(
			js.Field{XType: "proxmoxtextfield", Name: "password", Label: "Password", InputType: "password", AllowBlank: new(false)},
		)}),
	},
}
