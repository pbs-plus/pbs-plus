Ext.define("PBS.D2DManagement.ScriptEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsScriptEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  width: "80%",
  resizable: true,

  isCreate: true,
  isAdd: true,
  subject: "Script",
  cbindData: function (initialConfig) {
    let me = this;

    let contentid = initialConfig.contentid;
    let baseurl = "/api2/extjs/config/d2d-script";

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
      editable: true,
    },
    {
      xtype: "component",
      itemId: "scriptEditor",
      name: "script",
      html: '<div style="height: 100%; border: 1px solid #ccc;"></div>',
      listeners: {
        afterrender: function (component) {
          PBS.PlusUtils.LoadCodeMirror(function () {
            let editor = CodeMirror(component.el.dom.firstChild, {
              mode: "shell",
              lineNumbers: true,
              indentUnit: 2,
              tabSize: 2,
              indentWithTabs: false,
              lineWrapping: false,
              theme: "default",
            });

            component.codeMirror = editor;
            component.setValue = (val) => editor.setValue(val || "");
            component.getValue = () => editor.getValue();
            component.isValid = () => true;
            component.validate = () => true;
          });
        },
      },
    },
  ],
});
