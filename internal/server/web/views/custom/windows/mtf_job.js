Ext.define("PBS.MtfManagement.JobEdit", {
  extend: "Proxmox.window.Edit",

  title: gettext("MTF Migration Job"),
  isCreate: true,
  onlineHelp: undefined,

  method: "POST",
  // `url` is required by Proxmox.window.Edit's initComponent validation
  // (it throws if neither url nor both submitUrl+loadUrl are set). The
  // actual submit URL is computed by submitUrl below.
  url: "/api2/extjs/config/mtf-job",
  submitUrl: function () {
    let id = this.jobId;
    let base = "/api2/extjs/config/mtf-job";
    if (this.method === "PUT" && id) {
      return base + "/" + encodeURIComponent(encodePathValue(id));
    }
    return base;
  },

  controller: {
    xclass: "Ext.app.ViewController",

    loadForm: function (id) {
      let view = this.getView();
      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/mtf-job/" +
          encodeURIComponent(encodePathValue(id)),
        method: "GET",
        waitMsgTarget: view,
        success: function (response) {
          let data = response.result.data;
          let form = view.down("form").getForm();
          view.method = "PUT";
          view.jobId = data.id;
          view.isCreate = false;
          form.setValues(data);
          // (re)load the source dropdown for this kind so the selected
          // source_ref is represented in the store.
          view.getController().loadSourceStore(data.source_kind);
          view.setTitle(gettext("Edit") + ": " + data.id);
        },
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
          view.close();
        },
      });
    },

    onSourceKindChange: function (combo) {
      let kind = combo.getValue();
      this.loadSourceStore(kind);
      this.getView().sourceKind = kind;
    },

    // Builds/reloads the source_ref dropdown store for the given kind.
    // families  -> value=id,          text=name
    // cartridge -> value=barcode,     text=label|barcode
    // dataset   -> value=id,          text=machine_name|name
    loadSourceStore: function (kind) {
      let combo = this.getView().down("field[name=source_ref]");
      if (!combo) return;
      let type = kind === "cartridge" ? "cartridges" : kind === "dataset" ? "datasets" : "families";
      let store = combo.store;
      store.getProxy().setUrl(
        pbsPlusBaseUrl + "/api2/extjs/config/mtf-inventory?type=" + type,
      );
      store.valueField = "value";
      store.displayField = "text";
      store.removeAll();
      store.load();
    },
  },

  listeners: {
    afterrender: function (win) {
      let ctrl = win.getController();
      if (win.jobId) {
        ctrl.loadForm(win.jobId);
      } else {
        let kind = win.sourceKind;
        if (!kind) {
          let combo = win.down("field[name=source_kind]");
          kind = (combo && combo.getValue()) || "family";
        }
        ctrl.loadSourceStore(kind);
      }
    },
  },

  items: [
    {
      xtype: "inputpanel",
      column1: [
        {
          xtype: "textfield",
          name: "id",
          fieldLabel: gettext("Job ID"),
          allowBlank: false,
          emptyText: gettext("e.g. mtf-dp-d010"),
        },
        {
          xtype: "proxmoxKVComboBox",
          name: "source_kind",
          fieldLabel: gettext("Source Type"),
          allowBlank: false,
          value: "family",
          comboItems: [
            ["family", gettext("Media Set (Family)")],
            ["cartridge", gettext("Single Cartridge")],
            ["dataset", gettext("Single Data Set")],
          ],
          listeners: {
            change: "onSourceKindChange",
          },
        },
        {
          xtype: "combobox",
          name: "source_ref",
          fieldLabel: gettext("Source"),
          allowBlank: false,
          editable: true,
          forceSelection: false,
          anyMatch: true,
          queryMode: "local",
          triggerAction: "all",
          displayField: "text",
          valueField: "value",
          store: {
            fields: ["value", "text"],
            autoLoad: false,
            proxy: {
              type: "ajax",
              url:
                pbsPlusBaseUrl +
                "/api2/extjs/config/mtf-inventory?type=families",
              reader: {
                type: "json",
                rootProperty: "data",
              },
            },
            listeners: {
              load: function (store) {
                store.each(function (rec) {
                  let kind = store.getProxy().getUrl().split("type=")[1];
                  let val, text;
                  if (kind === "cartridges") {
                    val = rec.get("barcode");
                    text = rec.get("label") || rec.get("barcode");
                  } else if (kind === "datasets") {
                    val = String(rec.get("id"));
                    text = rec.get("machine_name") || rec.get("name") || val;
                  } else {
                    val = String(rec.get("id"));
                    text = rec.get("name") || "Media-Family-" + val;
                  }
                  rec.set("value", val);
                  rec.set("text", text);
                });
              },
            },
          },
        },
        {
          xtype: "pbsDataStoreSelector",
          name: "datastore",
          fieldLabel: gettext("Datastore"),
          allowBlank: false,
        },
      ],
      column2: [
        {
          xtype: "proxmoxKVComboBox",
          name: "namespace",
          fieldLabel: gettext("Namespace"),
          allowBlank: true,
          editable: true,
          emptyText: gettext("default (or auto via mapping)"),
          comboItems: [["", gettext("auto (use mappings)")]],
        },
        {
          xtype: "combobox",
          name: "changer",
          fieldLabel: gettext("Changer"),
          allowBlank: true,
          editable: true,
          forceSelection: false,
          queryMode: "local",
          triggerAction: "all",
          emptyText: gettext("(auto)"),
          displayField: "name",
          valueField: "name",
          store: {
            fields: ["name", "device"],
            autoLoad: true,
            proxy: {
              type: "ajax",
              url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-changer",
              reader: { type: "json", rootProperty: "data" },
            },
          },
        },
        {
          xtype: "textfield",
          name: "drive",
          fieldLabel: gettext("Drive"),
          allowBlank: true,
          emptyText: gettext("(first drive)"),
        },
        {
          xtype: "proxmoxcheckbox",
          name: "spanning",
          fieldLabel: gettext("Spanning"),
          boxLabel: gettext("Merge all cartridges of the media set"),
          value: true,
        },
        {
          xtype: "proxmoxcheckbox",
          name: "overwrite_mappings",
          fieldLabel: gettext("Overwrite NS"),
          boxLabel: gettext("Ignore mappings, use the Namespace field above"),
          value: false,
        },
      ],
      columnB: [
        {
          xtype: "textfield",
          name: "comment",
          fieldLabel: gettext("Comment"),
          width: "100%",
        },
        {
          xtype: "textfield",
          name: "schedule",
          fieldLabel: gettext("Schedule"),
          width: "100%",
          emptyText: gettext("calendar event, e.g. mon..fri 02:00"),
        },
      ],
    },
  ],
});
