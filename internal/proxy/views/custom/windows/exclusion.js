Ext.define("PBS.D2DManagement.ExclusionEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsExclusionEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  isCreate: true,
  isAdd: true,
  subject: "Disk Backup Global Path Exclusion",
  cbindData: function(initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = "/api2/extjs/config/d2d-exclusion";

    me.isCreate = !contentid;
    me.url = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}`
      : baseurl;
    me.method = contentid ? "PUT" : "POST";

    return {};
  },

  items: [
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
      fieldLabel: gettext("Comment"),
      xtype: "proxmoxtextfield",
      name: "comment",
      cbind: {
        deleteEmpty: "{!isCreate}",
      },
    },
  ],
});
