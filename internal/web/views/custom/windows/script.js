Ext.define("PBS.D2DManagement.ScriptEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsScriptEditWindow",
  mixins: ["Proxmox.Mixin.CBind"],

  width: "80%",
  height: "80%",
  resizable: true,

  isCreate: true,
  isAdd: true,
  subject: "Script",

  bodyPadding: 10,

  layout: {
    type: "vbox",
    align: "stretch",
  },

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
      xtype: "fieldcontainer",
      fieldLabel: gettext("Script Content"),
      layout: "fit",
      flex: 1,
      items: [
        {
          xtype: "component",
          itemId: "scriptEditor",
          name: "script",
          anchor: "100%",
          height: 400,
          html: '<div style="height: 100%;"></div>',
          listeners: {
            afterrender: function (component) {
              PBS.PlusUtils.LoadCodeMirror(function () {
                let isDark =
                  window.matchMedia &&
                  window.matchMedia("(prefers-color-scheme: dark)").matches;

                if (Proxmox.Utils && Proxmox.Utils.theme) {
                  isDark =
                    Proxmox.Utils.theme === "auto"
                      ? isDark
                      : Proxmox.Utils.theme === "dark";
                }

                let editor = CodeMirror(component.el.dom.firstChild, {
                  mode: "shell",
                  lineNumbers: true,
                  indentUnit: 2,
                  tabSize: 2,
                  indentWithTabs: false,
                  lineWrapping: false,
                  theme: isDark ? "material-darker" : "default",
                });

                component.codeMirror = editor;
                component.setValue = (val) => editor.setValue(val || "");
                component.getValue = () => editor.getValue();
                component.isValid = () => true;
                component.validate = () => true;

                // Refresh editor to ensure proper rendering
                setTimeout(() => editor.refresh(), 1);
              });
            },
            resize: function (component) {
              if (component.codeMirror) {
                component.codeMirror.refresh();
              }
            },
          },
        },
      ],
    },
  ],

  getValues: function () {
    let values = this.callParent();
    let editor = this.down("#scriptEditor");
    if (editor && editor.codeMirror) {
      values.script = editor.codeMirror.getValue();
    }
    return values;
  },

  setValues: function (values) {
    this.callParent([values]);
    let editor = this.down("#scriptEditor");
    if (editor && editor.codeMirror && values.script) {
      editor.codeMirror.setValue(values.script);
    }
  },
});
