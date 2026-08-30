package management

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func targetEditData(kind string) js.Raw {
	return js.Func("initialConfig", fmt.Sprintf(`
		let contentid = initialConfig.contentid;
		this.isCreate = !contentid;
		this.autoLoad = !!contentid;
		this.url = contentid
			? "/api2/extjs/config/d2d-target/" + encodeURIComponent(encodePathValue(contentid))
			: "/api2/extjs/config/d2d-target";
		this.method = contentid ? "PUT" : "POST";
		return { targetKind: %q };
	`, kind))
}

func targetNameField() js.Field {
	return js.Field{
		XType: js.XDisplayEditField, Label: "Name", Name: "name", Renderer: "Ext.htmlEncode",
		AllowBlank: new(false), EditableWhenCreate: true, EmptyText: "Unique target name",
	}
}

func targetKindField(kind string) js.Field {
	return js.Field{XType: js.XHiddenField, Name: "kind", Value: kind}
}

func targetEditWindow(name string, xtype js.XType, subject, kind string, width int, column1, column2 js.Arr) js.EditWindow {
	return js.EditWindow{
		Name: name, XType: xtype, Subject: subject, IsCreate: true, IsAdd: true,
		PixelWidth: width, BodyPadding: new(0), FieldDefaults: js.Obj{"labelWidth": 125},
		CBindData: targetEditData(kind),
		Items: js.Items(js.Panel{
			Extend: js.ExtInputPanel, BodyPadding: 12,
			Column1: column1, Column2: column2,
		}),
	}
}

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
	targetEditWindow(
		"PBS.D2DManagement.TargetFilesystemEditWindow", "pbsFilesystemTargetEditWindow", "Filesystem Target", "filesystem", 560,
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Target", Layout: "anchor", Items: js.Items(
				targetNameField(), targetKindField("filesystem"),
				js.Field{XType: js.XHiddenField, Name: "access", Value: "local"},
				js.Field{XType: js.XDisplayEditField, Label: "Path", Name: "path", Renderer: "Ext.htmlEncode", AllowBlank: new(false), EditableWhenCreate: true, EmptyText: "/mnt/backup"},
			)},
		),
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Mount", Layout: "anchor", Items: js.Items(
				js.Field{XType: "pbsD2DScriptSelector", Label: "Mount Script", Name: "mount_script", EmptyText: "Optional"},
				js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Value: js.T("The path must be available on this PBS server. Use a mount script only when the storage needs setup before a job runs.")},
			)},
		),
	),
	targetEditWindow(
		"PBS.D2DManagement.TargetS3EditWindow", "pbsS3TargetEditWindow", "S3 Target", "s3", 760,
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Location", Layout: "anchor", Items: js.Items(
				targetNameField(), targetKindField("s3"),
				js.Field{XType: js.XProxmoxTextField, Label: "Endpoint", Name: "s3_endpoint", AllowBlank: new(false), EmptyText: "s3.us-east-1.amazonaws.com"},
				js.Field{XType: js.XProxmoxTextField, Label: "Bucket", Name: "s3_bucket", AllowBlank: new(false), EmptyText: "pbs-archive"},
				js.Field{XType: js.XProxmoxTextField, Label: "Region", Name: "s3_region", AllowBlank: new(true), EmptyText: "Optional for custom endpoints"},
				js.Field{XType: js.XCheckbox, Label: "TLS", Name: "s3_use_ssl", BoxLabel: "Use HTTPS", Checked: new(true), InputValue: "true", UncheckedValue: "false"},
				js.Field{XType: js.XCheckbox, Label: "Addressing", Name: "s3_path_style", BoxLabel: "Use path-style bucket addressing", Checked: new(true), InputValue: "true", UncheckedValue: "false",
					AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Enable for MinIO and most S3-compatible appliances. Disable for virtual-hosted AWS-style bucket URLs.")}},
			)},
		),
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Credentials", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XProxmoxTextField, Label: "Access Key", Name: "s3_access_key", AllowBlank: new(false), EmptyText: "Access key ID"},
				js.Field{XType: js.XProxmoxTextField, Label: "Secret Key", Name: "s3_secret_key", InputType: "password", AllowBlank: new(true), EmptyText: "Leave blank to keep the current key", CBind: js.Obj{"allowBlank": "{!isCreate}"}},
				js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Value: js.T("Credentials are encrypted at rest. Object prefixes are selected per backup job with the Subpath field.")},
			)},
		),
	),
	targetEditWindow(
		"PBS.D2DManagement.TargetPostgreSQLEditWindow", "pbsPostgreSQLTargetEditWindow", "PostgreSQL Target", "postgresql", 800,
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Connection", Layout: "anchor", Items: js.Items(
				targetNameField(), targetKindField("postgresql"),
				js.Field{XType: js.XProxmoxTextField, Label: "Host", Name: "database_host", AllowBlank: new(false), EmptyText: "postgres.example.com"},
				js.Field{XType: js.XIntegerField, Label: "Port", Name: "database_port", Value: 5432, MinValue: 1, MaxValue: 65535, AllowBlank: new(false)},
				js.Field{XType: js.XProxmoxTextField, Label: "Username", Name: "database_username", AllowBlank: new(false), EmptyText: "Backup role"},
				js.Field{XType: js.XProxmoxTextField, Label: "Password", Name: "database_password", InputType: "password", AllowBlank: new(true), EmptyText: "Leave blank to keep the current password", CBind: js.Obj{"allowBlank": "{!isCreate}"}},
			)},
		),
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "TLS", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XKVComboBox, Label: "SSL Mode", Name: "database_tls_mode", Value: "prefer", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"disable", "Disable"}, js.Arr{"allow", "Allow"}, js.Arr{"prefer", "Prefer"}, js.Arr{"require", "Require"}, js.Arr{"verify-ca", "Verify CA"}, js.Arr{"verify-full", "Verify CA and hostname"},
				}},
				js.Field{XType: js.XProxmoxTextField, Label: "CA Certificate", Name: "database_ca_certificate", EmptyText: "/etc/ssl/certs/database-ca.pem"},
			)},
			js.Field{XType: js.XFieldSet, Title: "Client Tools", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XHiddenField, Name: "database_default_client_family", Value: "postgresql"},
				js.Field{XType: "pbsD2DDatabaseClientSelector", Label: "Client Version", Name: "database_default_client_dir", AllowBlank: new(false), CBind: js.Obj{"engine": "{targetKind}"}},
				js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Value: js.T("Only pg_dump and psql installations discovered on this PBS server are listed.")},
			)},
		),
	),
	targetEditWindow(
		"PBS.D2DManagement.TargetMySQLEditWindow", "pbsMySQLTargetEditWindow", "MySQL / MariaDB Target", "mysql", 800,
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "Connection", Layout: "anchor", Items: js.Items(
				targetNameField(), targetKindField("mysql"),
				js.Field{XType: js.XKVComboBox, Label: "Server Type", Name: "database_variant", Value: "mysql", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"mysql", "MySQL"}, js.Arr{"mariadb", "MariaDB"},
				}, ChangeFn: js.Func("field, value", `
					let family = field.up("window").down("[name=database_default_client_family]");
					if (family && !family.isDirty()) {
						family.setValue(value);
					}
				`)},
				js.Field{XType: js.XProxmoxTextField, Label: "Host", Name: "database_host", AllowBlank: new(false), EmptyText: "mysql.example.com"},
				js.Field{XType: js.XIntegerField, Label: "Port", Name: "database_port", Value: 3306, MinValue: 1, MaxValue: 65535, AllowBlank: new(false)},
				js.Field{XType: js.XProxmoxTextField, Label: "Username", Name: "database_username", AllowBlank: new(false), EmptyText: "Backup user"},
				js.Field{XType: js.XProxmoxTextField, Label: "Password", Name: "database_password", InputType: "password", AllowBlank: new(true), EmptyText: "Leave blank to keep the current password", CBind: js.Obj{"allowBlank": "{!isCreate}"}},
			)},
		),
		js.Items(
			js.Field{XType: js.XFieldSet, Title: "TLS", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XKVComboBox, Label: "TLS Mode", Name: "database_tls_mode", Value: "preferred", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"disabled", "Disabled"}, js.Arr{"preferred", "Preferred"}, js.Arr{"required", "Required"}, js.Arr{"verify-ca", "Verify CA"}, js.Arr{"verify-identity", "Verify CA and hostname"},
				}},
				js.Field{XType: js.XProxmoxTextField, Label: "CA Certificate", Name: "database_ca_certificate", EmptyText: "/etc/ssl/certs/database-ca.pem"},
			)},
			js.Field{XType: js.XFieldSet, Title: "Client Tools", Layout: "anchor", Items: js.Items(
				js.Field{XType: js.XKVComboBox, Label: "Client Family", Name: "database_default_client_family", Value: "mysql", AllowBlank: new(false), ComboItems: js.Arr{
					js.Arr{"mysql", "MySQL"}, js.Arr{"mariadb", "MariaDB"},
				}},
				js.Field{XType: "pbsD2DDatabaseClientSelector", Label: "Client Version", Name: "database_default_client_dir", AllowBlank: new(false), CBind: js.Obj{"engine": "{targetKind}"}},
				js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Value: js.T("Choose the installed mysqldump/mysql or mariadb-dump/mariadb toolchain used for backup and restore.")},
			)},
		),
	),
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
