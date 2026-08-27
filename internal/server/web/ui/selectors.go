package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var selectors = []js.Value{
	js.Selector{
		Name: "PBS.form.D2DExclusionSelector", XType: "pbsD2DExclusionSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/exclusion", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 450,
		ListColumns: []js.Column{{Text: "Path", DataIndex: "path", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}},
		Methods: map[string]js.Raw{"initComponent": js.Func("", `
			let me = this;
			me.store.proxy.extraParams = me.changer ? { changer: me.changer } : {};
			me.callParent();
		`)},
		Extra: js.Obj{"value": js.Raw("null")},
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
		Extra: js.Obj{
			"config": js.Obj{"datastore": js.Raw("null")}, "allowBlank": true, "autoSelect": true,
			"emptyText": js.T("Root"), "editable": true, "anyMatch": true, "forceSelection": false,
			"queryMode": "local", "matchFieldWidth": false,
			"listConfig": js.Obj{"minWidth": 170, "maxWidth": 500, "minHeight": 30, "emptyText": js.Raw("'<div class=\"x-grid-empty\">' + gettext(\"No namespaces accessible.\") + '</div>'")},
			"triggers":   js.Obj{"clear": js.Obj{"cls": "pmx-clear-trigger", "weight": -1, "hidden": true, "handler": js.Func("", `this.triggers.clear.setVisible(false); this.setValue("");`)}},
			"listeners":  js.Obj{"change": js.Func("field, value", `field.triggers.clear.setVisible(value !== "");`)},
		},
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
		Extra: js.Obj{"config": js.Obj{"datastore": js.Raw("null"), "namespace": js.Raw("null")}, "queryMode": "local", "anyMatch": true, "forceSelection": false, "autoSelect": false},
	},
	js.Selector{
		Name: "PBS.form.D2DTargetSelector", XType: "pbsD2DTargetSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/target", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 600,
		ListColumns: []js.Column{
			{Text: "Name", DataIndex: "name", Sortable: new(true), Flex: 2, Renderer: js.Raw("Ext.String.htmlEncode")},
			{Text: "Type", DataIndex: "target_type", Sortable: new(true), Flex: 1, Renderer: js.Func("value", `let icons = { local: '<i class="fa fa-desktop"></i> Local', agent: '<i class="fa fa-server"></i> Agent', s3: '<i class="fa fa-cloud"></i> S3' }; return icons[value] || Ext.String.htmlEncode(value || "");`)},
			{Text: "Path / Volume", DataIndex: "path", Sortable: new(true), Flex: 3, Renderer: js.Func("value, metaData, record", `if (record.get("target_type") === "agent") { let volumeName = record.get("volume_name"); let volumeId = record.get("volume_id"); return Ext.String.htmlEncode(volumeName || volumeId || "-"); } return value ? Ext.String.htmlEncode(value) : "-";`)},
		},
		Methods: map[string]js.Raw{"initComponent": js.Func("", `let me = this; me.store.proxy.extraParams = me.changer ? { changer: me.changer } : {}; me.callParent();`)},
		Extra:   js.Obj{"value": js.Raw("null"), "editable": true, "forceSelection": true, "queryMode": "local", "minChars": 1, "filterPickList": true, "typeAhead": false},
	},
	js.Selector{
		Name: "PBS.form.D2DTokenSelector", XType: "pbsD2DTokenSelector",
		DisplayField: "name", ValueField: "name", APIPath: "/api2/json/d2d/token", Sorters: "name",
		AllowBlank: new(false), AutoSelect: new(false), ListWidth: 450,
		ListColumns: []js.Column{{Text: "Token", DataIndex: "token", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}, {Text: "Comment", DataIndex: "comment", Sortable: new(true), Flex: 3, Renderer: js.Raw("Ext.String.htmlEncode")}},
		Methods:     map[string]js.Raw{"initComponent": js.Func("", `let me = this; me.store.proxy.extraParams = me.changer ? { changer: me.changer } : {}; me.callParent();`)},
		Extra:       js.Obj{"value": js.Raw("null")},
	},
}
