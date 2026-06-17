Ext.define("PBS.MtfManagement.ChangerPanel", {
  extend: "Ext.panel.Panel",
  alias: "widget.pbsMtfChangerPanel",

  title: "MTF Changers & Drives",
  layout: "border",

  controller: {
    xclass: "Ext.app.ViewController",

    addChanger: function () {
      this.openSimpleWindow("changer");
    },

    addDrive: function () {
      this.openSimpleWindow("drive");
    },

    openSimpleWindow: function (which) {
      let view = this.getView();
      let isChanger = which === "changer";
      let fields;
      if (isChanger) {
        fields = [
          {
            xtype: "textfield",
            name: "name",
            fieldLabel: gettext("Name"),
            allowBlank: false,
            emptyText: "library-1",
          },
          {
            xtype: "textfield",
            name: "device",
            fieldLabel: gettext("Device"),
            allowBlank: false,
            emptyText: "/dev/sg1",
          },
          {
            xtype: "textfield",
            name: "comment",
            fieldLabel: gettext("Comment"),
          },
        ];
      } else {
        fields = [
          {
            xtype: "textfield",
            name: "name",
            fieldLabel: gettext("Name"),
            allowBlank: false,
            emptyText: "drive-0",
          },
          {
            xtype: "textfield",
            name: "device",
            fieldLabel: gettext("Device"),
            allowBlank: false,
            emptyText: "/dev/nst0",
          },
          {
            xtype: "textfield",
            name: "changer",
            fieldLabel: gettext("Changer Name"),
          },
          {
            xtype: "numberfield",
            name: "drive_index",
            fieldLabel: gettext("Drive Index"),
            value: 0,
            minValue: 0,
          },
          {
            xtype: "textfield",
            name: "comment",
            fieldLabel: gettext("Comment"),
          },
        ];
      }

      Ext.create("Proxmox.window.Edit", {
        title: isChanger ? gettext("Add Changer") : gettext("Add Drive"),
        method: "POST",
        isCreate: true,
        autoShow: true,
        submitUrl: function () {
          return (
            "/api2/extjs/config/mtf-" + (isChanger ? "changer" : "drive")
          );
        },
        items: {
          xtype: "inputpanel",
          column1: fields,
        },
        listeners: {
          destroy: () => {
            view.down("#changerGrid").getStore().load();
            view.down("#driveGrid").getStore().load();
          },
        },
      }).show();
    },

    removeRow: function (btn) {
      let grid = btn.up("grid");
      let isChanger = grid.itemId === "changerGrid";
      let recs = grid.getSelection();
      if (!recs.length) return;
      let url =
        "/api2/extjs/config/mtf-" + (isChanger ? "changer" : "drive");
      recs.forEach((r) => {
        PBS.PlusUtils.API2Request({
          url: url + "/" + encodeURIComponent(encodePathValue(r.getId())),
          method: "DELETE",
          success: () => grid.getStore().load(),
          failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
        });
      });
    },
  },

  items: [
    {
      xtype: "grid",
      itemId: "changerGrid",
      region: "center",
      title: gettext("Changers"),
      store: {
        model: "pbs-mtf-changer",
        autoLoad: true,
        proxy: {
          type: "ajax",
          url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-changer",
          reader: { type: "json", rootProperty: "data" },
        },
      },
      tbar: [
        {
          xtype: "proxmoxButton",
          text: gettext("Add Changer"),
          selModel: false,
          handler: "addChanger",
        },
        {
          xtype: "proxmoxButton",
          text: gettext("Remove"),
          handler: "removeRow",
          enableFn: function () {
            return this.up("grid").getSelection().length > 0;
          },
          disabled: true,
        },
      ],
      columns: [
        { header: gettext("Name"), dataIndex: "name", flex: 1 },
        { header: gettext("Device"), dataIndex: "device", flex: 1 },
        { header: gettext("Comment"), dataIndex: "comment", flex: 1 },
      ],
    },
    {
      xtype: "grid",
      itemId: "driveGrid",
      region: "south",
      height: "40%",
      split: true,
      title: gettext("Drives"),
      store: {
        model: "pbs-mtf-drive",
        autoLoad: true,
        proxy: {
          type: "ajax",
          url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-drive",
          reader: { type: "json", rootProperty: "data" },
        },
      },
      tbar: [
        {
          xtype: "proxmoxButton",
          text: gettext("Add Drive"),
          selModel: false,
          handler: "addDrive",
        },
        {
          xtype: "proxmoxButton",
          text: gettext("Remove"),
          handler: "removeRow",
          enableFn: function () {
            return this.up("grid").getSelection().length > 0;
          },
          disabled: true,
        },
      ],
      columns: [
        { header: gettext("Name"), dataIndex: "name", flex: 1 },
        { header: gettext("Device"), dataIndex: "device", flex: 1 },
        { header: gettext("Changer"), dataIndex: "changer", flex: 1 },
        {
          header: gettext("Index"),
          dataIndex: "drive_index",
          width: 60,
          align: "right",
        },
        { header: gettext("Comment"), dataIndex: "comment", flex: 1 },
      ],
    },
  ],
});
