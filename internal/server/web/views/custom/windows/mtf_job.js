Ext.define("PBS.MtfManagement.JobEdit", {
  extend: "Proxmox.window.Edit",

  title: gettext("MTF Migration Job"),
  isCreate: true,
  onlineHelp: undefined,

  method: "POST",
  submitUrl: function () {
    let id = this.id;
    let base = "/api2/extjs/config/mtf-job";
    if (this.method === "POST" && id) {
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
          view.isCreate = false;
          form.setValues(data);
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
      let form = combo.up("form");
      let familyCombo = form.down("field[name=source_ref]");
      if (familyCombo) {
        familyCombo.store.load();
      }
      this.getView().sourceKind = kind;
    },
  },

  listeners: {
    afterrender: function (win) {
      if (win.id) {
        win.getController().loadForm(win.id);
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
          xtype: "proxmoxKVComboBox",
          name: "source_ref",
          fieldLabel: gettext("Source"),
          allowBlank: false,
          editable: true,
          displayField: "text",
          valueField: "value",
          store: {
            fields: ["value", "text"],
            autoLoad: true,
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
                  rec.set("text", rec.get("name"));
                  rec.set(
                    "value",
                    String(rec.get("id")),
                  );
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
          xtype: "proxmoxKVComboBox",
          name: "changer",
          fieldLabel: gettext("Changer"),
          allowBlank: true,
          editable: true,
          comboItems: [["", gettext("(auto)")]],
          displayField: "text",
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
