Ext.define("PBS.D2DManagement.TokenEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsTokenEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  isCreate: true,
  isAdd: true,
  subject: "Agent Bootstrap Token",
  cbindData: function (initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = "/api2/extjs/config/d2d-token";

    me.isCreate = !contentid;
    me.url = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}`
      : baseurl;
    me.method = contentid ? "PUT" : "POST";

    return {};
  },

  items: [
    {
      fieldLabel: gettext("Duration"),
      name: "duration",
      xtype: "pmxDisplayEditField",
      renderer: Ext.htmlEncode,
      allowBlank: true,
      emptyText: "24h",
      cbind: {
        editable: "{isCreate}",
      },
    },
    {
      xtype: "displayfield",
      userCls: "pmx-hint",
      value: gettext(
        "Format: Use a Go duration string (e.g., '2h', '30m', '1h30m'). Use '0' for a token that never expires.",
      ),
    },
    {
      fieldLabel: gettext("Comment"),
      name: "comment",
      xtype: "pmxDisplayEditField",
      renderer: Ext.htmlEncode,
      allowBlank: false,
      cbind: {
        editable: "{isCreate}",
      },
    },
  ],
});
