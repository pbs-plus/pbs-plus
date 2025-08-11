Ext.define("PBS.D2DManagement.TargetPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskTargetPanel",

  stateful: true,
  stateId: "grid-disk-backup-targets-v1",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function() {
      let me = this;
      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        listeners: {
          destroy: function() {
            me.reload();
          },
        },
      }).show();
    },

    addJob: function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();

      if (!selection || selection.length < 1) {
        return;
      }

      targetName = selection[0].data.name;

      Ext.create("PBS.D2DManagement.BackupJobEdit", {
        autoShow: true,
        jobData: { target: targetName },
        listeners: {
          destroy: function() {
            me.reload();
          },
        },
      }).show();
    },

    removeTargets: function() {
      const me = this;
      const view = me.getView();
      const recs = view.getSelection();
      if (!recs.length) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected entries?"),
        (btn) => {
          if (btn !== "yes") return;
          recs.forEach((rec) => {
            PBS.PlusUtils.API2Request({
              url:
                "/api2/extjs/config/d2d-target/" +
                encodeURIComponent(encodePathValue(rec.getId())),
              method: "DELETE",
              waitMsgTarget: view,
              failure: (resp) =>
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
              success: () => me.reload(),
            });
          });
        }
      );
    },

    onEdit: function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        contentid: selection[0].data.name,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    setS3Secret: function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      Ext.create("PBS.D2DManagement.TargetS3Secret", {
        contentid: selection[0].data.name,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    reload: function() {
      this.getView().getStore().rstore.load();
    },

    stopStore: function() {
      this.getView().getStore().rstore.stopUpdate();
    },

    startStore: function() {
      this.getView().getStore().rstore.startUpdate();
    },

    render_status: function(value) {
      if (value.toString() == "true") {
        icon = "check good";
        text = "Reachable";
      } else {
        icon = "times critical";
        text = "Unreachable";
      }

      return `<i class="fa fa-${icon}"></i> ${text}`;
    },

    init: function(view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);

      // Apply custom grouper for "ns" on initialization
      const store = view.getStore();
      store.setGrouper({
        property: "path",
        groupFn: function(record) {
          let ns = record.get("path");
          let name = record.get("name");
          if (ns.startsWith("agent://")) {
            host = name.split(" - ")[0];
            return "Agent - " + host;
          }
          return "Non-Agent";
        },
      });
    },
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: "onEdit",
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "proxmox-disk-targets",
      model: "pbs-model-targets",
      proxy: {
        type: "pbsplus",
        url: pbsPlusBaseUrl + "/api2/json/d2d/target",
      },
    },
    sorters: "name",
    groupField: "path",
  },

  features: [
    {
      ftype: "grouping",
      groupers: [
        {
          property: "path",
          groupFn: function(record) {
            let ns = record.get("path");
            let name = record.get("name");
            if (ns.startsWith("agent://")) {
              host = name.split(" - ")[0];
              return "Agent - " + host;
            }
            return "Non-Agent";
          },
        },
      ],
      groupHeaderTpl: [
        '{name:this.formatNS} ({rows.length} Item{[values.rows.length > 1 ? "s" : ""]})',
        {
          formatNS: function(group) {
            return group || "Unassigned";
          },
        },
      ],
    },
  ],

  tbar: [
    {
      text: gettext("Add"),
      xtype: "proxmoxButton",
      handler: "onAdd",
      selModel: false,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Create Job"),
      handler: "addJob",
      disabled: true,
    },
    "-",
    {
      text: gettext("Edit"),
      xtype: "proxmoxButton",
      handler: "onEdit",
      disabled: true,
    },
    {
      text: gettext("Set S3 Secret Key"),
      xtype: "proxmoxButton",
      handler: "setS3Secret",
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeTargets",
      enableFn: function() {
        let recs = this.up("grid").getSelection();
        return recs.length > 0;
      },
      disabled: true,
    },
  ],
  columns: [
    {
      text: gettext("Name"),
      dataIndex: "name",
      flex: 1,
    },
    {
      text: gettext("Path"),
      dataIndex: "path",
      flex: 2,
    },
    {
      text: gettext("Job Count"),
      dataIndex: "job_count",
      flex: 1,
    },
    {
      text: gettext("Drive Type"),
      dataIndex: "drive_type",
      flex: 1,
    },
    {
      text: gettext("Drive Name"),
      dataIndex: "drive_name",
      flex: 1,
    },
    {
      text: gettext("Drive FS"),
      dataIndex: "drive_fs",
      flex: 1,
    },
    {
      text: gettext("Used Size"),
      dataIndex: "drive_used_bytes",
      renderer: function(value) {
        if (!value && value !== 0) {
          return "-";
        }
        return humanReadableBytes(value);
      },
      flex: 1,
    },
    {
      text: gettext("Total Size"),
      dataIndex: "drive_total_bytes",
      renderer: function(value) {
        if (!value && value !== 0) {
          return "-";
        }
        return humanReadableBytes(value);
      },
      flex: 1,
    },
    {
      header: gettext("Status"),
      dataIndex: "connection_status",
      renderer: "render_status",
      flex: 1,
    },
    {
      text: gettext("Agent OS"),
      dataIndex: "os",
      flex: 1,
    },
    {
      text: gettext("Agent Version"),
      dataIndex: "agent_version",
      flex: 1,
    },
  ],
});
