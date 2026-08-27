package mtf

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const mtfMappingURL = "/api2/extjs/config/mtf-mapping"

const noSourceHTML = `'<span style="color:#888">' + gettext("Matches all sources") + '</span>'`

var mtfMappingWindow = js.Func("id", `
	let view = this.getView();
	Ext.create("PBS.plusWindow.Edit", {
		title: id ? gettext("Edit Mapping") : gettext("Add Mapping"),
		method: id ? "PUT" : "POST",
		isCreate: !id,
		autoShow: true,
		width: 600,
		url: id ? "`+mtfMappingURL+`/" + id : "`+mtfMappingURL+`",
		items: {
			xtype: "inputpanel",
			onGetValues: function (values) {
				if (this.up("pbsPlusWindowEdit").isCreate) {
					delete values.delete;
				}
				delete values._pattern_type;
				return values;
			},
			column1: [
				{
					xtype: "textfield",
					name: "name",
					fieldLabel: gettext("Name"),
					allowBlank: false,
					emptyText: gettext("e.g. Windows D-drive backups"),
				},
				{
					xtype: "fieldcontainer",
					fieldLabel: gettext("Source Pattern"),
					layout: { type: "vbox", align: "stretch" },
					defaults: { margin: "0 0 4 0" },
					items: [
						{
							xtype: "combobox",
							name: "_pattern_type",
							submitValue: false,
							hideLabel: true,
							editable: false,
							value: "custom",
							store: [
								["custom", gettext("Custom regex")],
								["win-unc", gettext("Windows UNC path (\\\\HOST\\DRIVE\\...)")],
								["win-local", gettext("Windows local path (DRIVE:\\...)")],
								["unix", gettext("Unix path")],
								["any", gettext("Match all sources")],
							],
							listeners: {
								change: function (cb, val) {
									let win = cb.up("window");
									let patternCt = win.down("#patternBuilderCt");
									let regexField = win.down("field[name=match_regex]");
									let previewEl = win.down("#regexPreview");

									if (val === "custom") {
										patternCt.hide();
										regexField.show();
									} else if (val === "any") {
										patternCt.hide();
										regexField.hide();
										regexField.setValue("");
										if (previewEl) previewEl.setValue(`+noSourceHTML+`);
									} else {
										patternCt.show();
										regexField.hide();
										patternCt.down("#hostField").setVisible(val === "win-unc" || val === "unix");
										patternCt.down("#driveField").setVisible(val === "win-unc" || val === "win-local");
										patternCt.down("#pathField").setVisible(val === "win-unc" || val === "win-local");
									}

									if (val !== "custom" && val !== "any") {
										let host = win.down("#hostField").getValue() || "[^\\\\]+";
										let drive = win.down("#driveField").getValue() || "[A-Z]";
										let path = win.down("#pathField").getValue() || ".*";
										let regex = "";
										if (val === "win-unc") {
											regex = "\\\\\\\\(?P<host>" + host + ")\\\\(?P<drive>" + drive + "):\\\\(" + path + ")";
										} else if (val === "win-local") {
											regex = "(?P<drive>" + drive + "):\\\\(" + path + ")";
										} else if (val === "unix") {
											regex = "/?(?P<host>" + host + ")/" + path;
										}
										regexField.setValue(regex);
										if (previewEl) previewEl.setValue("<code>" + Ext.String.htmlEncode(regex) + "</code>");
									} else if (val === "custom") {
										let current = regexField.getValue();
										if (previewEl) previewEl.setValue(current ? "<code>" + Ext.String.htmlEncode(current) + "</code>" : `+noSourceHTML+`);
									}
								},
							},
						},
						{
							xtype: "container",
							itemId: "patternBuilderCt",
							hidden: true,
							layout: "anchor",
							defaults: { anchor: "100%", hideLabel: true },
							items: [
								{ xtype: "textfield", itemId: "hostField", emptyText: gettext("Host pattern, e.g. SERVER[0-9]+") },
								{ xtype: "textfield", itemId: "driveField", emptyText: gettext("Drive letter pattern, e.g. [D-F]") },
								{ xtype: "textfield", itemId: "pathField", emptyText: gettext("Path pattern, e.g. Backups.*") },
							],
						},
						{
							xtype: "displayfield",
							itemId: "regexPreview",
							hideLabel: true,
							value: `+noSourceHTML+`,
							fieldCls: "x-form-display-field",
						},
					],
				},
				{
					xtype: "textfield",
					name: "match_regex",
					fieldLabel: gettext("Regex"),
					allowBlank: true,
					emptyText: gettext("Empty = match all sources"),
					listeners: {
						change: function (f, val) {
							let previewEl = f.up("window").down("#regexPreview");
							if (previewEl) {
								previewEl.setValue(val ? "<code>" + Ext.String.htmlEncode(val) + "</code>" : `+noSourceHTML+`);
							}
						},
					},
				},
			],
			column2: [
				{
					xtype: "textfield",
					name: "template",
					fieldLabel: gettext("Target Template"),
					allowBlank: false,
					emptyText: "{machine.short}/{drive}",
				},
				{
					xtype: "fieldcontainer",
					fieldLabel: gettext("Available Tokens"),
					layout: "anchor",
					defaults: { anchor: "100%" },
					items: [
						{
							xtype: "displayfield",
							hideLabel: true,
							value: '<span style="color:#888;font-size:11px">' +
								gettext("{machine}, {machine.short}, {machine.label}, {drive}, {label}, $1..$N (regex captures)") +
								'</span>',
						},
					],
				},
				{ xtype: "numberfield", name: "priority", fieldLabel: gettext("Priority"), value: 10, minValue: 0 },
			],
			columnB: [
				{
					xtype: "proxmoxcheckbox",
					name: "is_default",
					fieldLabel: gettext("Default"),
					boxLabel: gettext("Use as fallback when no other mapping matches"),
					value: false,
				},
				{
					xtype: "proxmoxcheckbox",
					name: "enabled",
					fieldLabel: gettext("Enabled"),
					boxLabel: gettext("Enable this mapping rule"),
					value: true,
				},
				{ xtype: "textfield", name: "comment", fieldLabel: gettext("Comment"), width: "100%" },
			],
		},
		listeners: {
			afterrender: function (win) {
				if (!id) return;
				PBS.PlusUtils.API2Request({
					url: "`+mtfMappingURL+`/" + id,
					method: "GET",
					success: function (resp) {
						let data = resp.result.data;
						// The pattern combo must be set before setValues so its change
						// handler settles field visibility against the loaded regex.
						let regex = data.match_regex || "";
						win.down("field[name=_pattern_type]").setValue(regex ? "custom" : "any");
						win.down("form").getForm().setValues(data);
						let previewEl = win.down("#regexPreview");
						if (previewEl) {
							previewEl.setValue(regex ? "<code>" + Ext.String.htmlEncode(regex) + "</code>" : `+noSourceHTML+`);
						}
					},
					failure: function (resp) {
						Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
						win.close();
					},
				});
			},
			destroy: () => view.getStore().rstore.load(),
		},
	}).show();
`)

var mtfMappingPanel = js.Panel{
	Name: "PBS.MtfManagement.MappingPanel", XType: "pbsMtfMappingPanel",
	Title: "Namespace Mappings",
	Store: js.Store{
		StoreID: "pbs-mtf-mapping", Model: "pbs-mtf-mapping", Interval: 5000,
		APIPath: mtfMappingURL,
		SortBy: []js.Sorter{
			{Property: "is_default", Direction: "DESC"},
			{Property: "priority", Direction: "ASC"},
		},
	},
	Listeners: js.Listeners{ItemDblClick: "onEdit"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"openMappingWindow": mtfMappingWindow,
		"onAdd":             js.Func("", `this.openMappingWindow(null);`),
		"onEdit": js.Func("", `
			let selection = this.getView().getSelection();
			if (!selection || selection.length < 1) {
				return;
			}
			this.openMappingWindow(selection[0].get("id"));
		`),
		"removeMapping": js.Func("", `
			let me = this;
			let selection = me.getView().getSelection();
			if (!selection || selection.length < 1) {
				return;
			}
			let id = selection[0].get("id");
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Remove mapping '{0}'?"), selection[0].get("name")),
				function (btn) {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "`+mtfMappingURL+`/" + id,
						method: "DELETE",
						success: () => me.reload(),
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					});
				},
			);
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Edit", Handler: "onEdit", Disabled: true},
		{Text: "Remove", Handler: "removeMapping", Disabled: true},
	},
	Columns: []js.Column{
		{Text: "Default", DataIndex: "is_default", Width: 60, Align: "center", Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle" style="color:#f0ad4e"></i>' : "";
		`)},
		{Text: "Enabled", DataIndex: "enabled", Width: 60, Align: "center", Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle" style="color:green"></i>' : '<i class="fa fa-times-circle" style="color:#bbb"></i>';
		`)},
		{Text: "Name", DataIndex: "name", Flex: 1.5, Sortable: new(true)},
		{Text: "Priority", DataIndex: "priority", Width: 70, Align: "right"},
		{Text: "Source Match", DataIndex: "match_regex", Flex: 1.5, Renderer: js.Func("v", `
			if (!v) return '<span style="color:#888">' + gettext("(any source)") + "</span>";
			return "<code>" + Ext.String.htmlEncode(v) + "</code>";
		`)},
		{Text: "Target Template", DataIndex: "template", Flex: 1.5, Renderer: js.Func("v", `return "<code>" + Ext.String.htmlEncode(v) + "</code>";`)},
		{Text: "Comment", DataIndex: "comment", Flex: 1, Renderer: "Ext.String.htmlEncode"},
	},
}
