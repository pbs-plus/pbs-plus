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
  extend: "Ext.window.Window",
  alias: "widget.pbsTargetEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  title: gettext("Set Target S3 Secret Key"),
  width: 400,
  height: 200,
  modal: true,
  resizable: false,
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
      fieldDefaults: {
        labelWidth: 120,
        anchor: "100%",
      },
      items: [
        {
          fieldLabel: gettext("Secret Key"),
          name: "secret",
          xtype: "textfield", // Use standard textfield, not proxmoxtextfield
          inputType: "password",
          allowBlank: false,
        },
      ],
      buttons: [
        {
          text: gettext("OK"),
          formBind: true,
          handler: function(btn) {
            let win = btn.up("window");
            let form = btn.up("form").getForm();

            if (form.isValid()) {
              form.submit({
                url: win.submitUrl,
                method: "POST",
                waitMsg: gettext("Please wait..."),
                success: function(form, action) {
                  win.close();
                  // Add any success callback here
                },
                failure: function(form, action) {
                  Ext.Msg.alert(
                    gettext("Error"),
                    action.result?.message || gettext("Unknown error")
                  );
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
