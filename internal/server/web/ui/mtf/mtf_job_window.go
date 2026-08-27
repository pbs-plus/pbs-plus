package mtf

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var mtfJobEdit = js.EditWindow{
	Name: "PBS.MtfManagement.JobEdit", XType: "pbsMtfJobEdit",
	Subject: "MTF Migration Job", IsCreate: true,
	FieldDefaults: js.Obj{"labelWidth": 120},
	BodyPadding:   new(0),
	URL:           "/api2/extjs/config/mtf-job",
	Methods: map[string]js.Raw{
		"submitUrl": js.Func("", `
			let base = "/api2/extjs/config/mtf-job";
			if (this.method === "PUT" && this.jobId) {
				return base + "/" + encodeURIComponent(encodePathValue(this.jobId));
			}
			return base;
		`),
	},
	Listeners: js.Listeners{AfterRender: "onAfterRender"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAfterRender": js.Func("win", `
			if (win.jobId) {
				win.getController().loadForm(win.jobId);
				return;
			}
			win.method = "POST";
			win.isCreate = true;
			if (win.sourceKind) {
				let kindCombo = win.down("combobox[name=source_kind]");
				if (kindCombo) kindCombo.setValue(win.sourceKind);
				win.getController().loadSourceStore(win.sourceKind, win.sourceRef);
			}
			Ext.defer(function () {
				let form = win.down("form").getForm();
				if (win.defaultJobId && form.findField("id")) {
					form.findField("id").setValue(win.defaultJobId);
				}
				win.getController().defaultDatastore();
			}, 200);
		`),
		"loadForm": js.Func("id", `
			let view = this.getView();
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/mtf-job/" + encodeURIComponent(encodePathValue(id)),
				method: "GET",
				waitMsgTarget: view,
				success: function (response) {
					let data = response.result.data;
					let form = view.down("form").getForm();
					view.method = "PUT";
					view.jobId = data.id;
					view.isCreate = false;
					let btn = view.down("[reference=submitbutton]") || view.down("button[reference=submitbutton]");
					if (btn) {
						btn.setText(gettext("Update"));
					}
					form.setValues(data);
					// Load the source store for the saved kind. Once it finishes,
					// re-apply the source_ref so the combo resolves display text.
					view.getController().loadSourceStore(data.source_kind, data.source_ref);
					view.setTitle(gettext("Edit") + ": " + data.id);
				},
				failure: function (resp) {
					Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
					view.close();
				},
			});
		`),
		"onSourceKindChange": js.Func("combo, value", `
			this.loadSourceStore(value);
		`),
		"loadSourceStore": js.Func("kind, sourceRef", `
			let view = this.getView();
			let combo = view.down("combobox[name=source_ref]");
			if (!combo) return;
			let type = kind === "cartridge" ? "cartridges" : kind === "dataset" ? "datasets" : "families";
			let store = combo.store;
			store.getProxy().setUrl(pbsPlusBaseUrl + "/api2/extjs/config/mtf-inventory?type=" + type);
			store.removeAll();
			store.load({
				callback: function () {
					if (sourceRef) {
						combo.setValue(sourceRef);
					}
				},
			});
		`),
		"defaultDatastore": js.Func("", `
			let sel = this.getView().down("pbsDataStoreSelector");
			if (!sel || sel.getValue()) return;
			try {
				let dsNode = Ext.getStore("NavigationStore").getRoot().findChild("id", "datastores", false);
				if (dsNode && dsNode.childNodes && dsNode.childNodes.length) {
					let first = dsNode.childNodes[0].get("text");
					if (first && first !== "Add Datastore") {
						sel.setValue(first);
					}
				}
			} catch (e) {}
		`),
	}},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Panel{
				Extend: js.ExtInputPanel, Title: "Options",
				Methods: map[string]js.Raw{
					"onGetValues": js.Func("values", `
						if (this.up("pbsMtfJobEdit").isCreate) {
							delete values.delete;
						}
						return values;
					`),
				},
				Column1: js.Items(
					js.Field{XType: js.XDisplayEditField, Name: "id", Label: "Job ID", Renderer: "Ext.htmlEncode",
						AllowBlank: new(true), Editable: new(true), EmptyText: "auto-generated from source"},
					js.Field{XType: js.XKVComboBox, Name: "source_kind", Label: "Source Type", AllowBlank: new(false),
						Value: "family", ComboItems: js.Arr{
							js.Arr{"family", js.T("Media Set (Family)")},
							js.Arr{"cartridge", js.T("Single Cartridge")},
							js.Arr{"dataset", js.T("Single Data Set")},
						}, Change: "onSourceKindChange"},
					js.Field{XType: js.XComboBox, Name: "source_ref", Label: "Source", AllowBlank: new(false),
						Editable: new(true), ForceSelection: false, AnyMatch: true, QueryMode: "local", TriggerAction: "all",
						DisplayField: "text", ValueField: "value",
						Store: js.Store{
							AutoLoad: new(false),
							Fields:   js.Fields("value", "text", "volumes"),
							APIPath:  "/api2/extjs/config/mtf-inventory?type=families",
							Listeners: js.Obj{"load": js.Func("store", `
								store.each(function (rec) {
									let kind = store.getProxy().getUrl().split("type=")[1];
									let val, text;
									if (kind === "cartridges") {
										val = rec.get("barcode");
										text = rec.get("label") || rec.get("barcode");
									} else if (kind === "datasets") {
										val = String(rec.get("id"));
										let name = rec.get("name") || "";
										let machine = rec.get("machine_name") || "";
										let wt = rec.get("write_time");
										let timeStr = wt ? Ext.Date.format(new Date(wt * 1000), "Y-m-d H:i") : "";
										let vols = rec.get("volumes") || [];
										let drive = vols.length && vols[0].device ? vols[0].device : "";
										text = drive || name || "Data Set #" + val;
										if (machine) text += " on " + machine;
										if (timeStr) text += " (" + timeStr + ")";
									} else {
										val = String(rec.get("id"));
										text = rec.get("name") || "Media-Family-" + val;
									}
									rec.set("value", val);
									rec.set("text", text, { dirty: false });
								});
							`)},
						}},
					js.Field{XType: js.XDataStoreSelector, Name: "datastore", Label: "Datastore", AllowBlank: new(false)},
				),
				Column2: js.Items(
					js.Field{XType: js.XTextField, Name: "namespace", Label: "Namespace", AllowBlank: new(true), EmptyText: "auto (use mappings)"},
					js.Field{XType: js.XComboBox, Name: "changer", Label: "Changer", AllowBlank: new(true),
						Editable: new(true), ForceSelection: false, QueryMode: "local", TriggerAction: "all",
						EmptyText: "(auto)", DisplayField: "name", ValueField: "name",
						Store: js.Store{
							Fields:  js.Fields("name", "path"),
							APIPath: "/api2/json/tape/changer", Proxy: js.ProxyProxmox, QueryParamNull: true,
						}},
					js.Field{XType: js.XComboBox, Name: "drive", Label: "Drive", AllowBlank: new(true),
						Editable: new(true), ForceSelection: false, QueryMode: "local", TriggerAction: "all",
						EmptyText: "(first available)", DisplayField: "name", ValueField: "name",
						Store: js.Store{
							Fields:  js.Fields("name", "path"),
							APIPath: "/api2/json/tape/drive", Proxy: js.ProxyProxmox, QueryParamNull: true,
						}},
					js.Field{XType: js.XCheckbox, Name: "overwrite_mappings", Label: "Skip Mappings",
						BoxLabel: "Bypass namespace mapping rules, use value as-is", Value: false},
					js.Field{XType: js.XCheckbox, Name: "keep_loaded", Label: "Keep Loaded",
						BoxLabel: "Leave the tape in the drive after the job finishes", Value: true},
				),
				ColumnB: js.Items(
					js.Field{XType: js.XTextField, Name: "comment", Label: "Comment", Width: "100%"},
				),
			},
			js.Raw("PBS.D2DManagement.makeNotificationTab()"),
		),
	}),
}
