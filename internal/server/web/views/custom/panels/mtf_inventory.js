Ext.define("PBS.MtfManagement.InventoryPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfInventoryPanel",

  title: "MTF Inventory",

  controller: {
    xclass: "Ext.app.ViewController",

    reload: function () {
      this.getView().getStore().rstore.load();
    },

    startScan: function () {
      let view = this.getView();
      let win = Ext.create("PBS.plusWindow.Edit", {
        title: gettext("MTF Inventory Scan"),
        method: "POST",
        url: "/api2/extjs/config/mtf-scan",
        isCreate: true,
        submitText: gettext("Start Scan"),
        autoShow: true,
        width: 450,
        submitUrl: function () {
          return "/api2/extjs/config/mtf-scan";
        },
        submit: function () {
          let me = this;
          let form = me.formPanel.getForm();
          let values = me.getValues();
          delete values._source_type;
          delete values.delete;
          let url = Ext.isFunction(me.submitUrl)
            ? me.submitUrl(me.url, values)
            : me.submitUrl || me.url;

          PBS.PlusUtils.API2Request({
            url: url,
            method: me.method || "POST",
            params: values,
            waitMsgTarget: me,
            success: function (response) {
              me.hide();
              Ext.create("PBS.plusWindow.TaskViewer", {
                upid: response.result.data,
                taskDone: function () {
                  view.getStore().rstore.load();
                  me.close();
                },
              }).show();
            },
            failure: function (response) {
              Ext.Msg.alert(gettext("Error"), response.htmlStatus);
            },
          });
        },
        items: [
          {
            xtype: "combobox",
            name: "_source_type",
            submitValue: false,
            fieldLabel: gettext("Source"),
            labelWidth: 100,
            editable: false,
            value: "changer",
            store: [
              ["changer", gettext("Changer")],
              ["drive", gettext("Standalone Drive")],
              ["bkf", gettext(".bkf File or Directory")],
            ],
            listeners: {
              change: function (cb, val) {
                let win = cb.up("window");
                let changerCt = win.down("#scanChangerCt");
                let driveCt = win.down("#scanDriveCt");
                let bkfCt = win.down("#scanBkfCt");
                changerCt.setVisible(val === "changer");
                driveCt.setVisible(val === "changer" || val === "drive");
                bkfCt.setVisible(val === "bkf");
              },
            },
          },
          {
            xtype: "container",
            itemId: "scanChangerCt",
            layout: "anchor",
            defaults: { anchor: "100%", labelWidth: 100 },
            items: [
              {
                xtype: "combobox",
                name: "changer",
                fieldLabel: gettext("Changer"),
                allowBlank: false,
                editable: false,
                displayField: "name",
                valueField: "path",
                store: {
                  fields: ["name", "path"],
                  autoLoad: true,
                  proxy: { type: "proxmox", url: "/api2/json/tape/changer", queryParam: null },
                },
                listeners: {
                  change: function (cb) {
                    let driveCombo = cb.up("window").down("#scanDriveField");
                    if (driveCombo) driveCombo.setValue("");
                  },
                },
              },
            ],
          },
          {
            xtype: "container",
            itemId: "scanDriveCt",
            layout: "anchor",
            defaults: { anchor: "100%", labelWidth: 100 },
            items: [
              {
                xtype: "combobox",
                itemId: "scanDriveField",
                name: "drive",
                fieldLabel: gettext("Drive"),
                allowBlank: false,
                editable: false,
                displayField: "name",
                valueField: "path",
                store: {
                  fields: ["name", "path"],
                  autoLoad: true,
                  proxy: { type: "proxmox", url: "/api2/json/tape/drive", queryParam: null },
                },
              },
            ],
          },
          {
            xtype: "container",
            itemId: "scanBkfCt",
            hidden: true,
            layout: "anchor",
            defaults: { anchor: "100%", labelWidth: 100 },
            items: [
              {
                xtype: "textfield",
                name: "bkf_path",
                fieldLabel: gettext(".bkf Path"),
                emptyText: "/mnt/bkf/backup.bkf",
              },
            ],
          },
        ],
        listeners: {
          destroy: () => view.getStore().rstore.load(),
        },
      });
      win.show();
    },

    migrateCartridge: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) return;
      let bc = selection[0].get("barcode");
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Create a migration job for cartridge '{0}'?"), bc),
        (btn) => {
          if (btn !== "yes") return;
          this.openJobWindow({ source_kind: "cartridge", source_ref: bc, id: "mtf-" + bc.toLowerCase() });
        },
      );
    },

    migrateFamily: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) return;
      let famID = String(selection[0].get("media_family_id"));
      let famName = selection[0].get("media_family_name") || famID;
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Create a migration job for media set '{0}'?"), famName),
        (btn) => {
          if (btn !== "yes") return;
          this.openJobWindow({ source_kind: "family", source_ref: famID, id: "mtf-" + famName.toLowerCase().replace(/[^a-z0-9]+/g, "-") });
        },
      );
    },

    migrateDataset: function (dsId) {
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Create a migration job for data set #{0}?"), dsId),
        (btn) => {
          if (btn !== "yes") return;
          this.openJobWindow({ source_kind: "dataset", source_ref: String(dsId), id: "mtf-ds-" + dsId });
        },
      );
    },

    openJobWindow: function (defaults) {
      let ctrl = this;
      Ext.create("PBS.MtfManagement.JobEdit", {
        autoShow: true,
        sourceKind: defaults.source_kind,
        sourceRef: defaults.source_ref,
        defaultJobId: defaults.id,
        listeners: {
          destroy: function () {
            ctrl.reload();
          },
        },
      }).show();
    },

    showDataSets: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) return;
      let famID = selection[0].get("media_family_id");
      let famName = selection[0].get("media_family_name") || "Media Set";
      if (!famID) return;

      let ctrl = this;
      PBS.PlusUtils.API2Request({
        url: "/api2/extjs/config/mtf-inventory?type=datasets&family=" + famID,
        method: "GET",
        waitMsgTarget: view,
        success: function (resp) {
          let datasets = resp.result.data || [];
          let store = Ext.create("Ext.data.Store", {
            fields: ["name", "machine_name", "owner", "write_time", "num_files", "num_directories", "id", "volumes"],
            data: datasets,
          });
          Ext.create("Ext.window.Window", {
            title: Ext.String.format(gettext("Data Sets - {0}"), famName),
            width: 800,
            height: 400,
            layout: "fit",
            modal: true,
            items: [{
              xtype: "grid",
              store: store,
              columns: [
                { text: "#", xtype: "rownumberer", width: 40 },
                { text: gettext("Drive"), dataIndex: "volumes", width: 120, renderer: function (vols) { if (!vols || !vols.length) return "-"; let d = vols[0].device; let l = vols[0].volume_label; return Ext.String.htmlEncode(d || "") + (l ? " (" + Ext.String.htmlEncode(l) + ")" : ""); } },
                { text: gettext("Machine"), dataIndex: "machine_name", flex: 1 },
                { text: gettext("Owner"), dataIndex: "owner", flex: 1 },
                { text: gettext("Write Time"), dataIndex: "write_time", width: 140, renderer: function (v) { return v ? Ext.Date.format(new Date(v * 1000), "Y-m-d H:i") : "-"; } },
                { text: gettext("Files"), dataIndex: "num_files", width: 70, align: "right" },
                { text: gettext("Dirs"), dataIndex: "num_directories", width: 70, align: "right" },
                { xtype: "actioncolumn", width: 30, items: [{ iconCls: "fa fa-floppy-o", tooltip: gettext("Migrate this data set"), handler: function (grid, rowIdx) { let rec = store.getAt(rowIdx); ctrl.migrateDataset(rec.get("id")); } }] },
              ],
            }],
          }).show();
        },
        failure: function () {
          Ext.Msg.alert(gettext("Error"), gettext("Failed to load data sets."));
        },
      });
    },

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
      this.checkScanStatus();
      this.scanPoll = setInterval(() => this.checkScanStatus(), 5000);
      view.on("destroy", () => { if (this.scanPoll) clearInterval(this.scanPoll); });
    },

    checkScanStatus: function () {
      let view = this.getView();
      if (!view || view.isDestroyed) return;
      let btn = view.down("#mtfScanBtn");
      let status = view.down("#mtfScanStatus");
      PBS.PlusUtils.API2Request({
        url: "/api2/extjs/config/mtf-scan",
        method: "GET",
        success: function (resp) {
          let d = resp.result.data || {};
          if (d.active) {
            if (btn) { btn.setDisabled(true); btn.setText(gettext("Scan in progress…")); }
            if (status) { status.show(); status.setHtml('<i class="fa fa-refresh fa-spin"></i> ' + gettext("An inventory scan is running.")); }
          } else {
            if (btn) { btn.setDisabled(false); btn.setText(gettext("Run Scan")); }
            if (status) { status.hide(); }
          }
        },
      });
    },
  },

  listeners: {
    beforedestroy: 'stopStore',
    deactivate: 'stopStore',
    activate: function () { this.getController().startStore(); this.getController().checkScanStatus(); },
    itemdblclick: 'showDataSets',
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "pbs-mtf-cartridge",
      model: "pbs-mtf-cartridge",
      proxy: {
        type: "pbsplus",
        url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-inventory?type=cartridges",
      },
      interval: 5000,
    },
    sorters: "media_family_name",
    groupField: "media_family_name",
  },

  features: [{
    ftype: "grouping",
    groupHeaderTpl: [
      '{name:this.formatName} ({rows.length} Cartridge{[values.rows.length > 1 ? "s" : ""]})',
      { formatName: function (name) { return name || gettext("(unknown)"); } },
    ],
  }],

  viewConfig: {
    stripeRows: false,
    getRowClass: function (record) {
      if (record.get("status") === "damaged" || record.get("status") === "retired") return "proxmox-invalid-row";
      if (!record.get("catalog_type")) return "proxmox-warning-row";
      return "";
    },
  },

  tbar: [
    { text: gettext("Reload"), handler: "reload" },
    "-",
    { text: gettext("Run Scan"), handler: "startScan", iconCls: "fa fa-search", itemId: "mtfScanBtn" },
    { xtype: "tbtext", itemId: "mtfScanStatus", hidden: true, cls: "proxmox-warning-row", html: "" },
    "->",
    { xtype: "proxmoxButton", text: gettext("View Data Sets"), disabled: true, handler: "showDataSets", enableFn: (rec) => !!rec.get("media_family_id") },
    { xtype: "proxmoxButton", text: gettext("Migrate Cartridge"), disabled: true, handler: "migrateCartridge", enableFn: () => true },
    { xtype: "proxmoxButton", text: gettext("Migrate Set"), disabled: true, handler: "migrateFamily", enableFn: () => true },
  ],

  columns: [
    { text: gettext("Barcode"), dataIndex: "barcode", width: 140, sortable: true },
    { text: gettext("Label"), dataIndex: "label", flex: 1.2, renderer: function (v, meta, rec) { return (rec.get("is_bkf_file") ? '<i class="fa fa-file"></i> ' : "") + Ext.String.htmlEncode(v || ""); } },
    { text: gettext("Media Set"), dataIndex: "media_family_name", flex: 1.5, sortable: true, renderer: function (v) { return Ext.String.htmlEncode(v || gettext("(unknown)")); } },
    { text: gettext("Seq"), dataIndex: "sequence", width: 60, align: "right" },
    { text: gettext("Role"), dataIndex: "role", width: 90 },
    { text: gettext("Volumes"), dataIndex: "volumes", width: 80, align: "right" },
    { text: gettext("Files"), dataIndex: "files", width: 90, align: "right", renderer: function (v) { return v ? Ext.util.Format.number(v, "0,000") : "-"; } },
    { text: gettext("Last Scan"), dataIndex: "last_scanned", width: 150, renderer: function (v) { return v ? Ext.Date.format(new Date(v * 1000), "Y-m-d H:i:s") : "-"; } },
  ],
});
