package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var selectors = []js.Value{
	js.Selector{
		Name: "PBS.form.D2DExclusionSelector", XType: "pbsD2DExclusionSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/exclusion", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 450,
		ListColumns: []js.Column{{Text: "Path", DataIndex: "path", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}},
		Methods:     map[string]js.Raw{"initComponent": js.ChangerExtraParams},
		Value:       js.Raw("null"),
	},
	js.Selector{
		Name: "PBS.form.D2DNamespaceSelector", XType: "pbsD2DNamespaceSelector", Extend: "Ext.form.field.ComboBox",
		DisplayField: "ns", ValueField: "ns",
		Methods: map[string]js.Raw{
			"initComponent": js.Func("", `
				let me = this;
				me.store = Ext.create("Ext.data.Store", {
					model: "pbs-namespaces", autoLoad: !!me.getDatastore(), filters: (rec) => rec.data.ns !== "",
					proxy: { type: "proxmox", timeout: 30 * 1000, url: me.getDatastore() ? "/api2/json/admin/datastore/" + me.getDatastore() + "/namespace" : null },
				});
				me.setDisabled(!me.getDatastore());
				me.callParent();
			`),
			"updateDatastore": js.Func("newDatastore, oldDatastore", `
				if (newDatastore) {
					this.setDisabled(false);
					this.store.getProxy().setUrl("/api2/json/admin/datastore/" + newDatastore + "/namespace");
					this.store.load(); this.validate();
				} else { this.setDisabled(true); }
			`),
		},
		AllowBlank: new(true), AutoSelect: new(true), ConfigNames: []string{"datastore"},
		EmptyText: "Root", Editable: new(true), AnyMatch: new(true), ForceSelection: new(false),
		QueryMode: "local", MatchFieldWidth: new(false), ListMinWidth: 170, ListMaxWidth: 500,
		ListMinHeight: 30, ListEmptyText: js.Raw("'<div class=\"x-grid-empty\">' + gettext(\"No namespaces accessible.\") + '</div>'"),
		Clearable: true, OnChange: js.Func("field, value", `field.triggers.clear.setVisible(value !== "");`),
	},
	js.Selector{
		Name: "PBS.form.D2DSnapshotSelector", XType: "pbsD2DSnapshotSelector", Extend: "Ext.form.field.ComboBox",
		DisplayField: "display", ValueField: "value",
		Methods: map[string]js.Raw{
			"initComponent": js.Func("", `
				let me = this;
				me.store = Ext.create("Ext.data.Store", {
					model: "pbs-model-d2d-snapshots", autoLoad: !!me.getDatastore(),
					sorters: [{ property: "backup-time", direction: "DESC" }],
					proxy: { type: "proxmox", url: me.getDatastore() ? "/api2/json/admin/datastore/" + me.getDatastore() + "/snapshots" : null, extraParams: { "backup-type": "host", ns: me.getNamespace() || null } },
					listeners: { load: function () { let val = me.getValue(); if (val) me.setValue(val); } },
				});
				me.setDisabled(!me.getDatastore());
				me.callParent();
			`),
			"updateDatastore": js.Func("newDatastore", `
				let me = this;
				if (newDatastore) { me.setDisabled(false); me.store.getProxy().setUrl("/api2/json/admin/datastore/" + newDatastore + "/snapshots"); me.store.load(); } else { me.setDisabled(true); me.store.removeAll(); }
			`),
			"updateNamespace": js.Func("newNamespace", `
				let me = this;
				if (me.getDatastore()) { me.store.getProxy().setExtraParam("ns", newNamespace || null); me.store.load(); }
			`),
		},
		ConfigNames: []string{"datastore", "namespace"}, QueryMode: "local", AnyMatch: new(true),
		ForceSelection: new(false), AutoSelect: new(false),
	},
	js.Selector{
		Name: "PBS.form.D2DTargetSelector", XType: "pbsD2DTargetSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/target", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 600,
		ListColumns: []js.Column{
			{Text: "Name", DataIndex: "name", Sortable: new(true), Flex: 2, Renderer: js.Raw("Ext.String.htmlEncode")},
			{Text: "Type", DataIndex: "target_type", Sortable: new(true), Flex: 1, Renderer: js.Func("value", `let icons = { local: '<i class="fa fa-desktop"></i> Local', agent: '<i class="fa fa-server"></i> Agent', s3: '<i class="fa fa-cloud"></i> S3', postgresql: '<i class="fa fa-database"></i> PostgreSQL', mysql: '<i class="fa fa-database"></i> MySQL / MariaDB' }; return icons[value] || Ext.String.htmlEncode(value || "");`)},
			{Text: "Location", DataIndex: "path", Sortable: new(true), Flex: 3, Renderer: js.Func("value, metaData, record", `if (record.get("target_type") === "agent") { return Ext.String.htmlEncode(record.get("volume_name") || record.get("volume_id") || "-"); } if (["postgresql", "mysql"].includes(record.get("kind"))) { return Ext.String.htmlEncode(record.get("database_host") + ":" + record.get("database_port")); } return value ? Ext.String.htmlEncode(value) : "-";`)},
		},
		Methods: map[string]js.Raw{"initComponent": js.ChangerExtraParams},
		Value:   js.Raw("null"), Editable: new(true), ForceSelection: new(true), QueryMode: "local",
		MinChars: 1, FilterPickList: new(true), TypeAhead: new(false),
	},
	js.Selector{
		Name: "PBS.form.D2DDatabaseClientSelector", XType: "pbsD2DDatabaseClientSelector",
		DisplayField: "directory", ValueField: "directory", APIPath: "/api2/json/d2d/database-clients", Sorters: "directory",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 720, ConfigNames: []string{"engine"},
		ListColumns: []js.Column{
			{Text: "Family", DataIndex: "family", Width: 90},
			{Text: "Version", DataIndex: "version", Flex: 2},
			{Text: "Directory", DataIndex: "directory", Flex: 1, Renderer: js.Raw("Ext.String.htmlEncode")},
		},
		Methods: map[string]js.Raw{
			"initComponent": js.Func("", `
				let me = this;
				me.callParent();
				me.getStore().on("load", me.applyEngineFilter, me);
			`),
			"applyEngineFilter": js.Func("", `
				let me = this;
				let store = me.getStore();
				if (!store || typeof store.clearFilter !== "function") {
					return;
				}
				store.clearFilter();
				if (me.getEngine()) {
					store.filterBy((record) => record.get("engine") === me.getEngine());
				}
			`),
			"updateEngine": js.Func("", `this.applyEngineFilter();`),
		},
		Editable: new(false), ForceSelection: new(true), QueryMode: "local", EmptyText: "Select an installed client version",
	},
	js.Selector{
		Name: "PBS.form.D2DTokenSelector", XType: "pbsD2DTokenSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/token", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 450,
		ListColumns: []js.Column{{Text: "Token", DataIndex: "token", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}, {Text: "Comment", DataIndex: "comment", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}},
		Methods:     map[string]js.Raw{"initComponent": js.ChangerExtraParams},
		Value:       js.Raw("null"),
	},
}
