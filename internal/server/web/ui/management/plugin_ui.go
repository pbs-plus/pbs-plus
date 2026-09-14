package management

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var pluginUI = js.Raw(`
PBS.D2DManagement.PluginForms = {
	definitions: null,

	field: function(spec, isCreate, prefix) {
		let common = {
			fieldLabel: spec.label,
			name: (prefix || "config.") + spec.key,
			pluginFieldKey: spec.key,
			allowBlank: !spec.required,
			value: spec.default,
			anchor: "100%",
		};
		if (spec.help) {
			common.autoEl = { tag: "div", "data-qtip": Ext.String.htmlEncode(spec.help) };
		}
		if (spec.control === "group") {
			return {
				xtype: "fieldset",
				title: spec.label,
				pluginFieldKey: spec.key,
				layout: "anchor",
				items: this.fields(spec.fields || [], isCreate, prefix),
			};
		}
		if (spec.control === "integer") {
			return Ext.apply(common, {
				xtype: "proxmoxintegerfield",
				minValue: spec.minimum,
				maxValue: spec.maximum,
			});
		}
		if (spec.control === "boolean") {
			return Ext.apply(common, {
				xtype: "proxmoxcheckbox",
				inputValue: "true",
				uncheckedValue: "false",
			});
		}
		if (spec.control === "select") {
			return Ext.apply(common, {
				xtype: "proxmoxKVComboBox",
				comboItems: (spec.options || []).map((option) => [option.value, option.label]),
			});
		}
		if (spec.control === "status") {
			return Ext.apply(common, { xtype: "displayfield", submitValue: false });
		}
		if (spec.control === "secret") {
			return Ext.apply(common, {
				xtype: "proxmoxtextfield",
				inputType: "password",
				allowBlank: !isCreate || !spec.required,
				emptyText: isCreate ? "" : "Leave blank to keep the current value",
			});
		}
		return Ext.apply(common, { xtype: "proxmoxtextfield" });
	},

	fields: function(specs, isCreate, prefix) {
		let ordered = (specs || []).slice().sort((left, right) => (left.order || 0) - (right.order || 0));
		return ordered.map((spec) => this.field(spec, isCreate, prefix));
	},

	visibility: function(win, schema) {
		let byKey = {};
		win.query("[pluginFieldKey]").forEach((field) => {
			byKey[field.pluginFieldKey] = field;
		});
		let update = function() {
			((schema || win.definition.schema).fields || []).forEach(function visit(spec) {
				let field = byKey[spec.key];
				if (field && spec.visible_when) {
					let source = byKey[spec.visible_when.field];
					let visible = source && String(source.getValue()) === String(spec.visible_when.equals);
					field.setHidden(!visible);
					field.setDisabled(!visible);
				}
				(spec.fields || []).forEach(visit);
			});
		};
		win.query("field").forEach((field) => field.on("change", update));
		update();
	},

	loadDefinitions: function(callback) {
		let me = this;
		if (me.definitions) {
			callback(me.definitions);
			return;
		}
		PBS.PlusUtils.API2Request({
			url: "/api2/extjs/config/d2d-plugin-target-types",
			method: "GET",
			success: function(response) {
				me.definitions = (response.result && response.result.data) || [];
				callback(me.definitions);
			},
			failure: function() {
				me.definitions = [];
				callback(me.definitions);
			},
		});
	},

	jobFields: function(container, record, schemaName) {
		let me = this;
		me.loadDefinitions(function(definitions) {
			let definition = record && definitions.find((item) =>
				item.plugin_id === record.get("plugin_id") && item.target_type === record.get("target_type"));
			let schema = definition && definition[schemaName];
			container.removeAll();
			container.setHidden(!schema || !(schema.fields || []).length);
			container.setDisabled(!schema || !(schema.fields || []).length);
			if (schema && (schema.fields || []).length) {
				container.add(me.fields(schema.fields, false, "plugin-options."));
				me.visibility(container, schema);
			}
		});
	},
};

PBS.D2DManagement.PluginForms.loadDefinitions(Ext.emptyFn);

Ext.define("PBS.D2DManagement.PluginTargetEditWindow", {
	extend: "PBS.plusWindow.Edit",
	mixins: ["Proxmox.Mixin.CBind"],
	alias: "widget.pbsPluginTargetEditWindow",
	subject: "Plugin Target",
	width: 650,
	isAdd: true,

	initComponent: function() {
		let me = this;
		me.isCreate = !me.contentid;
		me.autoLoad = !me.isCreate;
		me.method = me.isCreate ? "POST" : "PUT";
		me.url = me.isCreate
			? "/api2/extjs/config/d2d-plugin-target"
			: "/api2/extjs/config/d2d-plugin-target/" + encodeURIComponent(encodePathValue(me.contentid));
		let identity = [{
			xtype: me.isCreate ? "proxmoxtextfield" : "displayfield",
			fieldLabel: "Name",
			name: "name",
			allowBlank: false,
		}];
		if (me.isCreate) {
			identity.push({ xtype: "hiddenfield", name: "plugin_id", value: me.definition.plugin_id });
			identity.push({ xtype: "hiddenfield", name: "target_type", value: me.definition.target_type });
		}
		me.items = [{
			xtype: "inputpanel",
			bodyPadding: 12,
			items: identity.concat(PBS.D2DManagement.PluginForms.fields(me.definition.schema.fields, me.isCreate)),
		}];
		me.callParent();
		me.on("afterrender", function() {
			PBS.D2DManagement.PluginForms.visibility(me);
			Ext.defer(() => PBS.D2DManagement.PluginForms.visibility(me), 200);
		});
	},
});

Ext.define("PBS.D2DManagement.PluginRepositoryEditWindow", {
	extend: "PBS.plusWindow.Edit",
	alias: "widget.pbsPluginRepositoryEditWindow",
	subject: "Plugin Repository",
	width: 650,
	isCreate: true,
	isAdd: true,
	method: "POST",
	url: "/api2/extjs/config/d2d-plugin-repository",
	items: [{
		xtype: "inputpanel",
		bodyPadding: 12,
		items: [
			{ xtype: "proxmoxtextfield", fieldLabel: "Repository ID", name: "id", allowBlank: false },
			{ xtype: "proxmoxtextfield", fieldLabel: "Name", name: "name", allowBlank: false },
			{ xtype: "proxmoxtextfield", fieldLabel: "Index URL", name: "url", allowBlank: false, emptyText: "https://example.com/index.toml" },
			{ xtype: "textareafield", fieldLabel: "Public Key", name: "public_key_pem", allowBlank: false, grow: true },
			{ xtype: "proxmoxtextfield", fieldLabel: "Confirmed SHA-256", name: "fingerprint", allowBlank: false },
			{ xtype: "displayfield", userCls: "pmx-hint", value: "Confirm the publisher fingerprint through a separate trusted channel before adding the repository." },
		],
	}],
});

Ext.define("PBS.D2DManagement.PluginInstallWindow", {
	extend: "PBS.plusWindow.Edit",
	alias: "widget.pbsPluginInstallWindow",
	subject: "Target Plugin",
	width: 520,
	isCreate: true,
	isAdd: true,
	method: "POST",
	url: "/api2/extjs/config/d2d-plugin-install",
	items: [{
		xtype: "inputpanel",
		bodyPadding: 12,
		items: [
			{ xtype: "proxmoxtextfield", fieldLabel: "Repository ID", name: "repository_id", allowBlank: false },
			{ xtype: "proxmoxtextfield", fieldLabel: "Plugin ID", name: "plugin_id", allowBlank: false },
			{ xtype: "proxmoxtextfield", fieldLabel: "Version", name: "version", allowBlank: false },
			{ xtype: "proxmoxcheckbox", fieldLabel: "Activation", name: "activate", boxLabel: "Activate after installation", checked: true, inputValue: "true", uncheckedValue: "false" },
		],
	}],
});

Ext.define("PBS.D2DManagement.PluginAdminWindow", {
	extend: "Ext.window.Window",
	alias: "widget.pbsPluginAdminWindow",
	title: "Target Plugin Administration",
	width: 900,
	height: 560,
	modal: true,
	layout: "fit",

	initComponent: function() {
		let me = this;
		let repositoryStore = Ext.create("Ext.data.Store", {
			fields: ["id", "name", "url", "fingerprint", "enabled", "last_refreshed_at", "last_error"],
			proxy: { type: "proxmox", url: "/api2/extjs/config/d2d-plugin-repository" },
			autoLoad: true,
		});
		let pluginStore = Ext.create("Ext.data.Store", {
			fields: ["plugin_id", "repository_id", "active_version", "enabled", "versions"],
			proxy: { type: "proxmox", url: "/api2/extjs/config/d2d-installed-plugin" },
			autoLoad: true,
		});
		me.items = [{
			xtype: "tabpanel",
			items: [{
				title: "Repositories",
				xtype: "grid",
				store: repositoryStore,
				columns: [
					{ text: "Name", dataIndex: "name", flex: 1 },
					{ text: "Repository ID", dataIndex: "id", flex: 1 },
					{ text: "Index URL", dataIndex: "url", flex: 2 },
					{ text: "Enabled", dataIndex: "enabled", renderer: Proxmox.Utils.format_boolean },
					{ text: "Last Refresh", dataIndex: "last_refreshed_at", flex: 1 },
					{ text: "Error", dataIndex: "last_error", flex: 1 },
				],
				tbar: [{
					text: "Add Repository",
					handler: function() {
						Ext.create("PBS.D2DManagement.PluginRepositoryEditWindow", { listeners: { destroy: () => repositoryStore.reload() } }).show();
					},
				}, {
					text: "Refresh",
					handler: function(button) {
						let record = button.up("grid").getSelection()[0];
						if (!record) return;
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-plugin-repository/" + encodeURIComponent(encodePathValue(record.get("id"))) + "/refresh",
							method: "POST",
							waitMsgTarget: me,
							success: () => repositoryStore.reload(),
							failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
						});
					},
				}, {
					text: "Enable / Disable",
					handler: function(button) {
						let record = button.up("grid").getSelection()[0];
						if (!record) return;
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-plugin-repository/" + encodeURIComponent(encodePathValue(record.get("id"))),
							method: "PUT",
							params: { enabled: !record.get("enabled") },
							success: () => repositoryStore.reload(),
							failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
						});
					},
				}, {
					text: "Remove",
					handler: function(button) {
						let record = button.up("grid").getSelection()[0];
						if (!record) return;
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-plugin-repository/" + encodeURIComponent(encodePathValue(record.get("id"))),
							method: "DELETE",
							success: () => repositoryStore.reload(),
							failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
						});
					},
				}],
			}, {
				title: "Installed Plugins",
				xtype: "grid",
				store: pluginStore,
				columns: [
					{ text: "Plugin ID", dataIndex: "plugin_id", flex: 1 },
					{ text: "Repository", dataIndex: "repository_id", flex: 1 },
					{ text: "Active Version", dataIndex: "active_version", flex: 1 },
					{ text: "Enabled", dataIndex: "enabled", renderer: Proxmox.Utils.format_boolean },
					{ text: "Installed Versions", dataIndex: "versions", flex: 2, renderer: (versions) => (versions || []).map((version) => version.version).join(", ") },
				],
				tbar: [{
					text: "Install Plugin",
					handler: function() {
						Ext.create("PBS.D2DManagement.PluginInstallWindow", { listeners: { destroy: () => pluginStore.reload() } }).show();
					},
				}, { text: "Refresh", handler: () => pluginStore.reload() }],
			}],
		}];
		me.callParent();
	},
});

Ext.define("PBS.D2DManagement.PluginTargetPanel", {
	extend: "Ext.grid.Panel",
	alias: "widget.pbsPluginTargetPanel",
	title: "Plugins",
	iconCls: "fa fa-puzzle-piece",
	border: false,

	initComponent: function() {
		let me = this;
		me.definitions = [];
		me.store = Ext.create("Ext.data.Store", {
			fields: ["name", "plugin_id", "plugin_version", "target_type", "schema_version", "secret_fields"],
		});
		me.columns = [
			{ text: "Name", dataIndex: "name", flex: 1 },
			{ text: "Target Type", dataIndex: "target_type", flex: 1 },
			{ text: "Plugin", dataIndex: "plugin_id", flex: 1 },
			{ text: "Version", dataIndex: "plugin_version", width: 110 },
		];
		me.tbar = [{ text: "Add Target", handler: () => me.showAddMenu() }, {
			text: "Edit",
			handler: () => me.editTarget(),
		}, {
			text: "Probe",
			handler: () => me.probeTarget(),
		}, {
			text: "Remove",
			handler: () => me.removeTarget(),
		}, "-", {
			text: "Manage Plugins",
			handler: () => Ext.create("PBS.D2DManagement.PluginAdminWindow").show(),
		}, "->", { text: "Refresh", handler: () => me.reloadTargets() }];
		me.listeners = {
			activate: () => me.reloadTargets(),
			itemdblclick: () => me.editTarget(),
		};
		me.callParent();
		me.loadDefinitions();
		me.reloadTargets();
	},

	loadDefinitions: function(callback) {
		let me = this;
		PBS.PlusUtils.API2Request({
			url: "/api2/extjs/config/d2d-plugin-target-types",
			method: "GET",
			success: function(response) {
				me.definitions = (response.result && response.result.data) || [];
				if (callback) callback();
			},
			failure: function(response) {
				me.definitions = [];
				if (callback) callback(response);
			},
		});
	},

	reloadTargets: function() {
		let me = this;
		PBS.PlusUtils.API2Request({
			url: "/api2/extjs/config/d2d-installed-plugin",
			method: "GET",
			success: function(response) {
				let installed = (response.result && response.result.data) || [];
				if (!installed.length) {
					me.store.loadData([]);
					return;
				}
				let pending = installed.length;
				let targets = [];
				installed.forEach(function(plugin) {
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-plugin-target?plugin_id=" + encodeURIComponent(plugin.plugin_id),
						method: "GET",
						autoErrorAlert: false,
						success: (result) => targets.push(...((result.result && result.result.data) || [])),
						callback: function() {
							pending--;
							if (pending === 0) me.store.loadData(targets);
						},
					});
				});
			},
			failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
		});
		me.loadDefinitions();
	},

	definitionFor: function(record) {
		return this.definitions.find((definition) =>
			definition.plugin_id === record.get("plugin_id") && definition.target_type === record.get("target_type"));
	},

	showAddMenu: function() {
		let me = this;
		let show = function() {
			if (!me.definitions.length) {
				Ext.Msg.alert(gettext("No Plugins"), gettext("Install and enable a target plugin first."));
				return;
			}
			Ext.create("Ext.menu.Menu", {
				items: me.definitions.map((definition) => ({
					text: definition.target_type + " (" + definition.plugin_id + ")",
					handler: function() {
						Ext.create("PBS.D2DManagement.PluginTargetEditWindow", {
							definition: definition,
							listeners: { destroy: () => me.reloadTargets() },
						}).show();
					},
				})),
			}).showBy(me.down("button[text='Add Target']"));
		};
		me.loadDefinitions(show);
	},

	editTarget: function() {
		let me = this;
		let record = me.getSelection()[0];
		if (!record) return;
		let definition = me.definitionFor(record);
		if (!definition) {
			Ext.Msg.alert(gettext("Unavailable"), gettext("Enable the target plugin before editing this target."));
			return;
		}
		Ext.create("PBS.D2DManagement.PluginTargetEditWindow", {
			contentid: record.get("name"),
			definition: definition,
			listeners: { destroy: () => me.reloadTargets() },
		}).show();
	},

	probeTarget: function() {
		let me = this;
		let record = me.getSelection()[0];
		if (!record) return;
		PBS.PlusUtils.API2Request({
			url: "/api2/extjs/config/d2d-plugin-target/" + encodeURIComponent(encodePathValue(record.get("name"))) + "/probe",
			method: "POST",
			waitMsgTarget: me,
			success: function(response) {
				let result = (response.result && response.result.data) || {};
				let title = result.available ? gettext("Available") : gettext("Unavailable");
				Ext.Msg.alert(title, Ext.String.htmlEncode(result.message || title));
			},
			failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
		});
	},

	removeTarget: function() {
		let me = this;
		let record = me.getSelection()[0];
		if (!record) return;
		Ext.Msg.confirm(gettext("Confirm"), "Remove target '" + Ext.String.htmlEncode(record.get("name")) + "'?", function(button) {
			if (button !== "yes") return;
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/d2d-plugin-target/" + encodeURIComponent(encodePathValue(record.get("name"))),
				method: "DELETE",
				success: () => me.reloadTargets(),
				failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
			});
		});
	},
});
`)
