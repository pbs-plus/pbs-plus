Ext.define("PBS.D2DManagement.ScriptEditWindow", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsScriptEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  width: "80%",

  isCreate: true,
  isAdd: true,
  subject: "Script",
  cbindData: function (initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/d2d-script";

    me.isCreate = !contentid;
    me.url = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}`
      : baseurl;
    me.method = contentid ? "PUT" : "POST";

    return {};
  },

  items: [
    {
      fieldLabel: gettext("Description"),
      name: "description",
      xtype: "pmxDisplayEditField",
      renderer: Ext.htmlEncode,
      allowBlank: true,
      cbind: {
        editable: true,
      },
    },
    {
      fieldLabel: gettext("Script Content"),
      name: "script",
      xtype: "textarea",
      allowBlank: false,
      height: 300,
      fieldStyle: "font-family: monospace; white-space: pre;"
    },
  ],
});
