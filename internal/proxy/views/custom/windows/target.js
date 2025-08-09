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
  extend: "Proxmox.window.EditBase", // or Ext.window.Window
  alias: "widget.pbsTargetEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  title: gettext("Set Target S3 Secret Key"),
  width: 400,
  modal: true,
  layout: "fit",

  cbindData: function(initialConfig) {
    let me = this;
    let contentid = initialConfig.contentid;
    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/d2d-target";

    me.submitUrl = contentid
      ? `${baseurl}/${encodeURIComponent(encodePathValue(contentid))}/s3-secret`
      : baseurl;

    return {};
  },

  items: [
    {
      xtype: "form",
      bodyPadding: 10,
      border: false,
      items: [
        {
          fieldLabel: gettext("Secret Key"),
          name: "secret",
          xtype: "proxmoxtextfield",
          inputType: "password",
          allowBlank: false,
        },
      ],
      buttons: [
        {
          text: gettext("OK"),
          handler: function(btn) {
            let win = btn.up("window");
            let form = win.down("form").getForm();
            if (form.isValid()) {
              Proxmox.Utils.API2Request({
                url: win.submitUrl,
                method: "POST",
                params: form.getValues(),
                success: function() {
                  win.close();
                },
                failure: function(response) {
                  Ext.Msg.alert(gettext("Error"), response.htmlStatus);
                },
              });
            }
          },
        },
        {
          text: gettext("Cancel"),
          handler: function(btn) {
            btn.up("window").close();
          },
        },
      ],
    },
  ],
});
