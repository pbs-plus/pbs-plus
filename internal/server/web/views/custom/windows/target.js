Ext.define("PBS.D2DManagement.TargetEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsTargetEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  isCreate: true,
  isAdd: true,
  subject: "Disk Backup Target",
  cbindData: function (initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = "/api2/extjs/config/d2d-target";

    me.isCreate = !contentid;
    me.url = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}`
      : baseurl;
    me.method = contentid ? "PUT" : "POST";

    return {};
  },

  items: [
    {
      fieldLabel: gettext("Name"),
      name: "name",
      xtype: "pmxDisplayEditField",
      renderer: Ext.htmlEncode,
      allowBlank: false,
      cbind: {
        editable: "{isCreate}",
      },
    },
    {
      fieldLabel: gettext("Path"),
      name: "path",
      xtype: "pmxDisplayEditField",
      renderer: Ext.htmlEncode,
      allowBlank: false,
      cbind: {
        editable: "{isCreate}",
      },
    },
    {
      xtype: "pbsD2DScriptSelector",
      fieldLabel: "Mount Script",
      name: "mount_script",
    },
  ],
});

Ext.define("PBS.D2DManagement.TargetS3Secret", {
  extend: "PBS.plusWindow.Create",
  alias: "widget.pbsTargetS3Secret",
  mixins: ["Proxmox.Mixin.CBind"],

  subject: gettext("Set Target S3 Secret Key"),
  width: 400,
  resizable: false,
  isCreate: true,
  method: "POST",

  cbindData: function (initialConfig) {
    let me = this;
    let contentid = initialConfig.contentid;
    me.url = `/api2/extjs/config/d2d-target/${encodeURIComponent(encodePathValue(contentid))}/s3-secret`;
    return {};
  },

  items: [
    {
      xtype: "inputpanel",
      padding: 10,
      items: [
        {
          xtype: "proxmoxtextfield",
          name: "secret",
          fieldLabel: gettext("Secret Key"),
          inputType: "password",
          allowBlank: false,
          emptyText: gettext("Enter S3 Secret Key"),
        },
      ],
    },
  ],
});
