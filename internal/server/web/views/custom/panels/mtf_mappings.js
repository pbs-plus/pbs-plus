Ext.define("PBS.MtfManagement.MappingPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfMappingPanel",

  title: "Namespace Mappings",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      this.openMappingWindow(null);
    },

    onEdit: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      this.openMappingWindow(selection[0].get("id"));
    },

    openMappingWindow: function (id) {
      let view = this.getView();
      let win = Ext.create("PBS.plusWindow.Edit", {
        title: id ? gettext("Edit Mapping") : gettext("Add Mapping"),
        method: id ? "PUT" : "POST",
        isCreate: !id,
        autoShow: true,
        width: 600,
        url: "/api2/extjs/config/mtf-mapping",
        submitUrl: function () {
          let base = "/api2/extjs/config/mtf-mapping";
          return id ? base + "/" + id : base;
        },
        items: {
          xtype: "inputpanel",
          onGetValues: function (values) {
            if (this.up("pbsPlusWindowEdit").isCreate) {
              delete values.delete;
            }
            delete values._pattern_type;
            return values;
          },
          column1: [
            {
              xtype: "textfield",
              name: "name",
              fieldLabel: gettext("Name"),
              allowBlank: false,
              emptyText: gettext("e.g. Windows D-drive backups"),
            },
            {
              xtype: "fieldcontainer",
              fieldLabel: gettext("Source Pattern"),
              layout: { type: "vbox", align: "stretch" },
              defaults: { margin: "0 0 4 0" },
              items: [
                {
                  xtype: "combobox",
                  name: "_pattern_type",
                  submitValue: false,
                  hideLabel: true,
                  editable: false,
                  value: "custom",
                  store: [
                    ["custom", gettext("Custom regex")],
                    ["win-unc", gettext("Windows UNC path (\\\\HOST\\DRIVE\\...)")],
                    ["win-local", gettext("Windows local path (DRIVE:\\...)")],
                    ["unix", gettext("Unix path")],
                    ["any", gettext("Match all sources")],
                  ],
                  listeners: {
                    change: function (cb, val) {
                      let win = cb.up("window");
                      let patternCt = win.down("#patternBuilderCt");
                      let regexField = win.down("field[name=match_regex]");
                      let previewEl = win.down("#regexPreview");

                      // Show/hide builder
                      if (val === "custom") {
                        patternCt.hide();
                        regexField.show();
                      } else if (val === "any") {
                        patternCt.hide();
                        regexField.hide();
                        regexField.setValue("");
                        if (previewEl) previewEl.setValue('<span style="color:#888">' + gettext("Matches all sources") + '</span>');
                      } else {
                        patternCt.show();
                        regexField.hide();
                        if (val === "win-unc") {
                          patternCt.down("#hostField").show();
                          patternCt.down("#driveField").show();
                          patternCt.down("#pathField").show();
                        } else if (val === "win-local") {
                          patternCt.down("#hostField").hide();
                          patternCt.down("#driveField").show();
                          patternCt.down("#pathField").show();
                        } else if (val === "unix") {
                          patternCt.down("#hostField").show();
                          patternCt.down("#driveField").hide();
                          patternCt.down("#pathField").hide();
                        }
                      }

                      // Build regex
                      if (val !== "custom" && val !== "any") {
                        let host = cb.up("window").down("#hostField").getValue() || "[^\\\\]+";
                        let drive = cb.up("window").down("#driveField").getValue() || "[A-Z]";
                        let path = cb.up("window").down("#pathField").getValue() || ".*";
                        let regex = "";
                        if (val === "win-unc") {
                          regex = "\\\\\\\\(?P<host>" + host + ")\\\\(?P<drive>" + drive + "):\\\\(" + path + ")";
                        } else if (val === "win-local") {
                          regex = "(?P<drive>" + drive + "):\\\\(" + path + ")";
                        } else if (val === "unix") {
                          regex = "/?(?P<host>" + host + ")/" + path;
                        }
                        regexField.setValue(regex);
                        if (previewEl) previewEl.setValue("<code>" + Ext.String.htmlEncode(regex) + "</code>");
                      } else if (val === "custom") {
                        let val = regexField.getValue();
                        if (previewEl) previewEl.setValue(val ? "<code>" + Ext.String.htmlEncode(val) + "</code>" : '<span style="color:#888">' + gettext("Matches all sources") + '</span>');
                      }
                    },
                  },
                },
                {
                  xtype: "container",
                  itemId: "patternBuilderCt",
                  hidden: true,
                  layout: "anchor",
                  defaults: { anchor: "100%", hideLabel: true },
                  items: [
                    {
                      xtype: "textfield",
                      itemId: "hostField",
                      emptyText: gettext("Host pattern, e.g. SERVER[0-9]+"),
                    },
                    {
                      xtype: "textfield",
                      itemId: "driveField",
                      emptyText: gettext("Drive letter pattern, e.g. [D-F]"),
                    },
                    {
                      xtype: "textfield",
                      itemId: "pathField",
                      emptyText: gettext("Path pattern, e.g. Backups.*"),
                    },
                  ],
                },
                {
                  xtype: "displayfield",
                  itemId: "regexPreview",
                  hideLabel: true,
                  value: '<span style="color:#888">' + gettext("Matches all sources") + '</span>',
                  fieldCls: "x-form-display-field",
                },
              ],
            },
            {
              xtype: "textfield",
              name: "match_regex",
              fieldLabel: gettext("Regex"),
              allowBlank: true,
              emptyText: gettext("Empty = match all sources"),
              listeners: {
                change: function (f, val) {
                  let win = f.up("window");
                  let previewEl = win.down("#regexPreview");
                  if (previewEl) {
                    previewEl.setValue(val ? "<code>" + Ext.String.htmlEncode(val) + "</code>" : '<span style="color:#888">' + gettext("Matches all sources") + '</span>');
                  }
                },
              },
            },
          ],
          column2: [
            {
              xtype: "textfield",
              name: "template",
              fieldLabel: gettext("Target Template"),
              allowBlank: false,
              emptyText: "{machine.short}/{drive}",
            },
            {
              xtype: "fieldcontainer",
              fieldLabel: gettext("Available Tokens"),
              layout: "anchor",
              defaults: { anchor: "100%" },
              items: [
                {
                  xtype: "displayfield",
                  hideLabel: true,
                  value: '<span style="color:#888;font-size:11px">' +
                    gettext("{machine}, {machine.short}, {machine.label}, {drive}, {label}, $1..$N (regex captures)") +
                    '</span>',
                },
              ],
            },
            {
              xtype: "numberfield",
              name: "priority",
              fieldLabel: gettext("Priority"),
              value: 10,
              minValue: 0,
            },
          ],
          columnB: [
            {
              xtype: "proxmoxcheckbox",
              name: "is_default",
              fieldLabel: gettext("Default"),
              boxLabel: gettext("Use as fallback when no other mapping matches"),
              value: false,
            },
            {
              xtype: "proxmoxcheckbox",
              name: "enabled",
              fieldLabel: gettext("Enabled"),
              boxLabel: gettext("Enable this mapping rule"),
              value: true,
            },
            {
              xtype: "textfield",
              name: "comment",
              fieldLabel: gettext("Comment"),
              width: "100%",
            },
          ],
        },
        listeners: {
          afterrender: function (win) {
            if (!id) return;
            PBS.PlusUtils.API2Request({
              url: "/api2/extjs/config/mtf-mapping/" + id,
              method: "GET",
              success: function (resp) {
                let data = resp.result.data;
                // Set pattern combo first so its change handler
                // establishes correct visibility before setValues
                // writes the regex value. This avoids the change
                // handler on match_regex firing with stale state.
                let regex = data.match_regex || "";
                let patternCombo = win.down("field[name=_pattern_type]");
                if (!regex) {
                  patternCombo.setValue("any");
                } else {
                  patternCombo.setValue("custom");
                }
                // Now fill the rest of the form.
                win.down("form").getForm().setValues(data);
                // Update preview to match.
                let previewEl = win.down("#regexPreview");
                if (previewEl) {
                  previewEl.setValue(
                    regex
                      ? "<code>" + Ext.String.htmlEncode(regex) + "</code>"
                      : '<span style="color:#888">' + gettext("Matches all sources") + '</span>',
                  );
                }
              },
              failure: function (resp) {
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
                win.close();
              },
            });
          },
          destroy: () => view.getStore().load(),
        },
      });
      win.show();
    },

    removeMapping: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let id = selection[0].get("id");
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Remove mapping '{0}'?"), selection[0].get("name")),
        function (btn) {
          if (btn !== "yes") {
            return;
          }
          PBS.PlusUtils.API2Request({
            url: "/api2/extjs/config/mtf-mapping/" + id,
            method: "DELETE",
            success: function () {
              me.reload();
            },
            failure: function (resp) {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        },
      );
    },

    reload: function () {
      this.getView().getStore().load();
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore());
    },
  },

  listeners: {
    activate: function () {
      this.getStore().load();
    },
    itemdblclick: "onEdit",
  },

  store: {
    model: "pbs-mtf-mapping",
    autoLoad: true,
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-mapping",
    },
    sorters: [
      { property: "is_default", direction: "DESC" },
      { property: "priority", direction: "ASC" },
    ],
  },

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add"),
      selModel: false,
      handler: "onAdd",
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "onEdit",
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeMapping",
      disabled: true,
    },
  ],

  columns: [
    {
      header: gettext("Default"),
      dataIndex: "is_default",
      width: 60,
      align: "center",
      renderer: function (v) {
        return v
          ? '<i class="fa fa-check-circle" style="color:#f0ad4e"></i>'
          : "";
      },
    },
    {
      header: gettext("Enabled"),
      dataIndex: "enabled",
      width: 60,
      align: "center",
      renderer: function (v) {
        return v
          ? '<i class="fa fa-check-circle" style="color:green"></i>'
          : '<i class="fa fa-times-circle" style="color:#bbb"></i>';
      },
    },
    {
      header: gettext("Name"),
      dataIndex: "name",
      flex: 1.5,
      sortable: true,
    },
    {
      header: gettext("Priority"),
      dataIndex: "priority",
      width: 70,
      align: "right",
    },
    {
      header: gettext("Source Match"),
      dataIndex: "match_regex",
      flex: 1.5,
      renderer: function (v) {
        if (!v) {
          return '<span style="color:#888">' +
            gettext("(any source)") +
            "</span>";
        }
        return "<code>" + Ext.String.htmlEncode(v) + "</code>";
      },
    },
    {
      header: gettext("Target Template"),
      dataIndex: "template",
      flex: 1.5,
      renderer: function (v) {
        return "<code>" + Ext.String.htmlEncode(v) + "</code>";
      },
    },
    {
      header: gettext("Comment"),
      dataIndex: "comment",
      flex: 1,
      renderer: Ext.String.htmlEncode,
    },
  ],
});
