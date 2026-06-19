Ext.define("PBS.MtfManagement.CartridgePanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfCartridgePanel",

  title: "MTF Inventory",

  selType: "checkboxmodel",
  multiSelect: true,

  controller: {
    xclass: "Ext.app.ViewController",

    reload: function () {
      this.getView().getStore().load();
    },

    startScan: function () {
      let view = this.getView();
      Ext.create("Proxmox.window.Edit", {
        title: gettext("Run Inventory Scan"),
        method: "POST",
        url: "/api2/extjs/config/mtf-scan",
        isCreate: true,
        autoShow: true,
        submitUrl: function () {
          return "/api2/extjs/config/mtf-scan";
        },
        items: {
          xtype: "inputpanel",
          column1: [
            {
              xtype: "textfield",
              name: "changer",
              fieldLabel: gettext("Changer Device"),
              emptyText: "/dev/sg1",
              allowBlank: true,
            },
            {
              xtype: "textfield",
              name: "drive",
              fieldLabel: gettext("Tape Drive Device"),
              emptyText: "/dev/nst0",
              allowBlank: true,
            },
          ],
          column2: [
            {
              xtype: "numberfield",
              name: "drive_index",
              fieldLabel: gettext("Drive Index"),
              value: 0,
              minValue: 0,
            },
            {
              xtype: "textfield",
              name: "bkf_path",
              fieldLabel: gettext("Or .bkf Path"),
              emptyText: "/path/to/backup.bkf (or dir)",
              allowBlank: true,
            },
          ],
        },
        listeners: {
          destroy: () => view.getStore().load(),
        },
      }).show();
    },

    migrateCartridge: function () {
      let recs = this.getView().getSelection();
      if (!recs.length) return;
      let bc = recs[0].get("barcode");
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(
          gettext("Create a migration job for cartridge '{0}'?"),
          bc,
        ),
        (btn) => {
          if (btn !== "yes") return;
          this.openJobWindow({
            source_kind: "cartridge",
            source_ref: bc,
            id: "mtf-" + bc.toLowerCase(),
          });
        },
      );
    },

    migrateFamily: function () {
      let recs = this.getView().getSelection();
      if (!recs.length) return;
      let famID = String(recs[0].get("media_family_id"));
      let famName = recs[0].get("media_family_name") || famID;
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(
          gettext("Create a migration job for media set '{0}'?"),
          famName,
        ),
        (btn) => {
          if (btn !== "yes") return;
          this.openJobWindow({
            source_kind: "family",
            source_ref: famID,
            id: "mtf-" + famName.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
          });
        },
      );
    },

    openJobWindow: function (defaults) {
      let view = this.getView();
      Ext.create("PBS.MtfManagement.JobEdit", {
        autoShow: true,
        sourceKind: defaults.source_kind,
        listeners: {
          afterrender: function (win) {
            let form = win.down("form").getForm();
            form.setValues(defaults);
            win.method = "POST";
            win.isCreate = true;
            win.getController().loadSourceStore(defaults.source_kind);
          },
          destroy: () => view.getStore().load(),
        },
      }).show();
    },

    renderBytes: function (v) {
      if (!v) return "-";
      return Proxmox.Utils.format_size(v);
    },

    renderTime: function (v) {
      if (!v) return "-";
      return Ext.Date.format(new Date(v * 1000), "Y-m-d H:i:s");
    },
  },

  listeners: {
    activate: function () {
      this.getStore().load();
    },
  },

  store: {
    model: "pbs-mtf-cartridge",
    autoLoad: true,
    proxy: {
      type: "ajax",
      url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-inventory?type=cartridges",
      reader: { type: "json", rootProperty: "data" },
    },
    sorters: "media_family_name",
  },

  features: [
    {
      ftype: "grouping",
      groupers: [
        {
          property: "media_family_name",
          groupFn: function (rec) {
            return rec.get("media_family_name") || gettext("(unknown)");
          },
        },
      ],
      groupHeaderTpl: "{name} ({rows.length} Cartridge{[values.rows.length > 1 ? \"s\" : \"\"]})",
    },
  ],

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Run Scan"),
      selModel: false,
      handler: "startScan",
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Migrate Set"),
      handler: "migrateFamily",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Migrate Cartridge"),
      handler: "migrateCartridge",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Reload"),
      selModel: false,
      handler: "reload",
    },
  ],

  columns: [
    {
      header: gettext("Barcode"),
      dataIndex: "barcode",
      width: 140,
      sortable: true,
    },
    {
      header: gettext("Label"),
      dataIndex: "label",
      flex: 1.2,
      renderer: function (v, meta, rec) {
        if (rec.get("is_bkf_file")) {
          return `<i class="fa fa-file"></i> ${v}`;
        }
        return v || "-";
      },
    },
    {
      header: gettext("Media Set"),
      dataIndex: "media_family_name",
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Seq"),
      dataIndex: "sequence",
      width: 60,
      align: "right",
    },
    {
      header: gettext("Role"),
      dataIndex: "role",
      width: 90,
    },
    {
      header: gettext("Volumes"),
      dataIndex: "volumes",
      width: 80,
      align: "right",
    },
    {
      header: gettext("Files"),
      dataIndex: "files",
      width: 90,
      align: "right",
      renderer: function (v) {
        return v ? Ext.util.Format.number(v, "0,000") : "-";
      },
    },
    {
      header: gettext("Last Scan"),
      dataIndex: "last_scanned",
      width: 150,
      renderer: "renderTime",
    },
  ],
});
