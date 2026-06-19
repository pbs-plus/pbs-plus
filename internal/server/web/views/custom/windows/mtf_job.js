Ext.define("PBS.MtfManagement.JobEdit", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsMtfJobEdit",

  isCreate: true,

  subject: gettext("MTF Migration Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  // `url` must be set for Proxmox.window.Edit validation. The actual
  // GET/PUT URLs are computed by submitUrl / loadForm.
  url: "/api2/extjs/config/mtf-job",

  submitUrl: function () {
    let base = "/api2/extjs/config/mtf-job";
    if (this.method === "PUT" && this.jobId) {
      return base + "/" + encodeURIComponent(encodePathValue(this.jobId));
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
          // Load the source store for the saved kind. Once it finishes,
          // re-apply the source_ref so the combo resolves display text.
          view.getController().loadSourceStore(
            data.source_kind,
            data.source_ref,
          );
          view.setTitle(gettext("Edit") + ": " + data.id);
        },
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
          view.close();
        },
      });
    },

    onSourceKindChange: function (combo, value) {
      this.loadSourceStore(value);
    },

    // Loads the source_ref dropdown store for the given kind. If sourceRef
    // is provided (= edit flow), the combo value is re-applied after load
    // so it resolves the display text from the transformed store records.
    loadSourceStore: function (kind, sourceRef) {
      let view = this.getView();
      let combo = view.down("combobox[name=source_ref]");
      if (!combo) return;
      let type =
        kind === "cartridge"
          ? "cartridges"
          : kind === "dataset"
            ? "datasets"
            : "families";
      let store = combo.store;

      store.getProxy().setUrl(
        pbsPlusBaseUrl + "/api2/extjs/config/mtf-inventory?type=" + type,
      );
      store.removeAll();
      store.load({
        callback: function () {
          if (sourceRef) {
            combo.setValue(sourceRef);
          }
        },
      });
    },
  },

  listeners: {
    afterrender: function (win) {
      if (win.jobId) {
        win.getController().loadForm(win.jobId);
        return;
      }
      // Create flow: pre-load the source store if sourceKind was provided
      // (e.g. when opening from inventory).
      if (win.sourceKind) {
        let kindCombo = win.down("combobox[name=source_kind]");
        if (kindCombo) kindCombo.setValue(win.sourceKind);
        win.getController().loadSourceStore(win.sourceKind);
      }
    },
  },

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "inputpanel",
        onGetValues: function (values) {
          if (this.up("pbsMtfJobEdit").isCreate) {
            delete values.delete;
          }
          return values;
        },
        column1: [
          {
            xtype: "pmxDisplayEditField",
            name: "id",
            fieldLabel: gettext("Job ID"),
            renderer: Ext.htmlEncode,
            allowBlank: true,
            editable: true,
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
              fields: ["value", "text", "volumes"],
              autoLoad: false,
              proxy: {
                type: "pbsplus",
                url:
                  pbsPlusBaseUrl +
                  "/api2/extjs/config/mtf-inventory?type=families",
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
                      let name = rec.get("name") || "";
                      let machine = rec.get("machine_name") || "";
                      let wt = rec.get("write_time");
                      let timeStr = wt
                        ? Ext.Date.format(new Date(wt * 1000), "Y-m-d H:i")
                        : "";
                      let vols = rec.get("volumes") || [];
                      let drive =
                        vols.length && vols[0].device
                          ? vols[0].device
                          : "";
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
            xtype: "textfield",
            name: "namespace",
            fieldLabel: gettext("Namespace"),
            allowBlank: true,
            emptyText: gettext("auto (use mappings)"),
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
              fields: ["name", "path"],
              autoLoad: true,
              proxy: {
                type: "proxmox",
                url: "/api2/json/tape/changer",
                queryParam: null,
              },
            },
          },
          {
            xtype: "combobox",
            name: "drive",
            fieldLabel: gettext("Drive"),
            allowBlank: true,
            editable: true,
            forceSelection: false,
            queryMode: "local",
            triggerAction: "all",
            emptyText: gettext("(first available)"),
            displayField: "name",
            valueField: "name",
            store: {
              fields: ["name", "path"],
              autoLoad: true,
              proxy: {
                type: "proxmox",
                url: "/api2/json/tape/drive",
                queryParam: null,
              },
            },
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
            fieldLabel: gettext("Skip Mappings"),
            boxLabel: gettext("Bypass namespace mapping rules, use value as-is"),
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
          {
            xtype: "textfield",
            name: "retry",
            fieldLabel: gettext("Number of retries"),
            emptyText: gettext("0"),
          },
          {
            xtype: "textfield",
            name: "retry-interval",
            fieldLabel: gettext("Retry interval (minutes)"),
            emptyText: gettext("1"),
          },
        ],
      },
      PBS.D2DManagement.makeNotificationTab(),
    ],
  },
});
