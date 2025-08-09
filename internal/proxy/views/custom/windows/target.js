Ext.define("PBS.D2DManagement.TargetEditWindow", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsTargetEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  isCreate: true,
  isAdd: true,
  subject: "Disk Backup Target",
  cbindData: function(initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/d2d-target";

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
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsTargetEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  subject: "Target S3 Secret Key",
  cbindData: function(initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/d2d-target";

    me.isCreate = !contentid;
    me.url = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}/s3-secret`
      : baseurl;
    me.method = "POST";

    return {};
  },

  items: [
    {
      fieldLabel: gettext("Secret Key"),
      name: "secret",
      xtype: "password",
      allowBlank: false,
    },
  ],
});
