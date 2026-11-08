package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var bulkEditWindow = js.Define("PBS.window.D2DBulkJobEdit", js.Obj{
	"extend": "Ext.window.Window",
	"alias":  "widget.pbsD2DBulkJobEdit",

	"modal":       true,
	"width":       620,
	"layout":      "fit",
	"bodyPadding": 10,

	"config": js.Obj{
		"url":         "",
		"jobIds":      js.Raw("[]"),
		"fields":      js.Raw("[]"),
		"commonStore": js.Raw("null"),
	},

	"initComponent": js.Func("", `
		let me = this;
		me.title = Ext.String.format(gettext("Bulk Edit: {0} job(s)"), me.getJobIds().length);
		me.items = [{
			xtype: "form",
			reference: "form",
			border: false,
			defaults: { anchor: "100%" },
			items: [{
				xtype: "displayfield",
				value: gettext("Tick a setting to overwrite it on every selected job. Unticked settings are left as they are."),
			}].concat(me.getFields().map((def) => ({
				xtype: "fieldcontainer",
				layout: "hbox",
				fieldLabel: def.label,
				labelWidth: 150,
				items: [
					{ xtype: "checkbox", itemId: "apply-" + def.name, margin: "0 8 0 0", listeners: { change: () => me.syncFields() } },
					Ext.apply({ name: def.name, flex: 1, disabled: true, allowBlank: true, submitValue: false }, def.field),
				],
			}))),
		}];
		me.buttons = [
			{ text: gettext("Apply"), reference: "applyBtn", disabled: true, handler: () => me.apply() },
			{ text: gettext("Cancel"), handler: () => me.close() },
		];
		me.callParent();
		let store = me.fieldByName("store");
		if (store) {
			store.on("change", () => me.syncFields());
		}
		me.syncFields();
	`),

	"fieldByName": js.Func("name", `
		return this.down("form").down("[name=" + name + "]");
	`),

	"isTicked": js.Func("name", `
		let box = this.down("#apply-" + name);
		return !!box && box.getValue();
	`),

	"syncFields": js.Func("", `
		let me = this;
		let any = false;
		me.getFields().forEach((def) => {
			let field = me.fieldByName(def.name);
			let ticked = me.isTicked(def.name);
			field.setDisabled(!ticked);
			any = any || ticked;
		});
		let ns = me.fieldByName("ns");
		if (ns) {
			let store = me.isTicked("store") ? me.fieldByName("store").getValue() : me.getCommonStore();
			if (store) {
				ns.setDatastore(store);
			}
			me.down("#apply-ns").setDisabled(!store);
		}
		me.down("[reference=applyBtn]").setDisabled(!any);
	`),

	"collectChanges": js.Func("", `
		let me = this;
		let set = {};
		let cleared = [];
		me.getFields().forEach((def) => {
			if (!me.isTicked(def.name)) {
				return;
			}
			let value = me.fieldByName(def.name).getValue();
			if (value === null || value === undefined || value === "") {
				cleared.push(def.name);
			} else {
				set[def.name] = value;
			}
		});
		return { set, cleared };
	`),

	"apply": js.Func("", `
		let me = this;
		let changes = me.collectChanges();
		let params = Ext.apply({ job: me.getJobIds().map((id) => encodePathValue(id)) }, changes.set);
		if (changes.cleared.length) {
			params.delete = changes.cleared;
		}
		PBS.PlusUtils.API2Request({
			url: me.getUrl(),
			method: "PUT",
			params: params,
			waitMsgTarget: me,
			success: () => me.close(),
			failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
		});
	`),
})
