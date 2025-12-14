Ext.define("PBS.D2DManagement.TargetPanel", {
  extend: "Ext.tree.Panel",
  alias: "widget.pbsDiskTargetPanel",

  stateful: true,
  stateId: "tree-disk-backup-targets-v2",

  rootVisible: false,
  useArrows: true,
  singleExpand: false,

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      let me = this;
      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    addJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();

      if (!selection || selection.length < 1) {
        return;
      }

      let rec = selection[0];
      if (rec.get("nodeType") === "volume") {
        let targetName = rec.get("target_name");
        Ext.create("PBS.D2DManagement.BackupJobEdit", {
          autoShow: true,
          jobData: { target: targetName, volume: rec.get("volume_name") },
          listeners: {
            destroy: function () {
              me.reload();
            },
          },
        }).show();
      } else if (rec.get("nodeType") === "target") {
        let targetName = rec.get("name");
        Ext.create("PBS.D2DManagement.BackupJobEdit", {
          autoShow: true,
          jobData: { target: targetName },
          listeners: {
            destroy: function () {
              me.reload();
            },
          },
        }).show();
      }
    },

    removeTargets: function () {
      const me = this;
      const view = me.getView();
      const recs = view.getSelection();
      if (!recs.length) return;

      // Only allow deleting target nodes (API is for targets, not volumes)
      const targetRecs = recs.filter((r) => r.get("nodeType") === "target");
      if (!targetRecs.length) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected target(s)?"),
        (btn) => {
          if (btn !== "yes") return;
          targetRecs.forEach((rec) => {
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
        },
      );
    },

    onEdit: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      const rec = selection[0];
      if (rec.get("nodeType") !== "target") return;

      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        contentid: rec.get("name"),
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    setS3Secret: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      const rec = selection[0];
      if (rec.get("nodeType") !== "target") return;

      Ext.create("PBS.D2DManagement.TargetS3Secret", {
        contentid: rec.get("name"),
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    reload: function () {
      this.getView().getStore().rstore.load();
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
    },

    render_status: function (value) {
      let icon, text;
      if (value === true || value === "true") {
        icon = "check good";
        text = "Reachable";
      } else {
        icon = "times critical";
        text = "Unreachable";
      }
      return `<i class="fa fa-${icon}"></i> ${text}`;
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
    },
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: "onEdit",
  },

  store: {
    type: "tree",
    root: {
      expanded: true,
    },
    fields: [
      "nodeType",
      "id",
      "name",
      "type",
      "job_count",
      "connection_status",
      "os",
      "agent_version",
      "local_path",
      "s3_host",
      "s3_region",
      "s3_bucket",
      "s3_ssl",
      "s3_path_style",
      "token_used",
      "host",
      // volume fields
      "volume_name",
      "target_name",
      "meta_type",
      "meta_name",
      "meta_fs",
      "meta_total_bytes",
      "meta_used_bytes",
      "meta_free_bytes",
      "meta_total",
      "meta_used",
      "meta_free",
      "accessible",
    ],
    rstore: {
      type: "update",
      storeid: "proxmox-disk-targets-tree",
      proxy: {
        type: "pbsplus",
        url: pbsPlusBaseUrl + "/api2/json/d2d/target?volumes=true&status=true",
      },
      listeners: {
        load: function (store, records, success) {
          // Transform flat target array with nested volumes to tree nodes
          const tree = [];
          const data = store.getData().items.map((r) => r.data);
          data.forEach((t) => {
            const targetId = t.name;
            const targetNode = {
              id: targetId,
              nodeType: "target",
              leaf: false,
              expanded: true,
              name: t.name,
              type: t.type,
              job_count: t.job_count,
              connection_status: t.connection_status,
              os: t.os,
              agent_version: t.agent_version,
              local_path: t.local_path,
              s3_host: t.s3_host,
              s3_region: t.s3_region,
              s3_bucket: t.s3_bucket,
              s3_ssl: t.s3_ssl,
              s3_path_style: t.s3_path_style,
              token_used: t.token_used,
              host: t.host,
              children: [],
            };

            const vols = Array.isArray(t.volumes) ? t.volumes : [];
            if (vols.length) {
              vols.forEach((v) => {
                targetNode.children.push({
                  id: `${t.name}::${v.volume_name}`,
                  nodeType: "volume",
                  leaf: true,
                  name: v.meta_name || v.volume_name,
                  volume_name: v.volume_name,
                  target_name: v.target_name || t.name,
                  meta_type: v.meta_type,
                  meta_name: v.meta_name,
                  meta_fs: v.meta_fs,
                  meta_total_bytes: v.meta_total_bytes,
                  meta_used_bytes: v.meta_used_bytes,
                  meta_free_bytes: v.meta_free_bytes,
                  meta_total: v.meta_total,
                  meta_used: v.meta_used,
                  meta_free: v.meta_free,
                  accessible: v.accessible,
                });
              });
            }
            tree.push(targetNode);
          });

          const view = Ext.ComponentQuery.query("pbsDiskTargetPanel")[0];
          if (view) {
            const ts = view.getStore();
            ts.setRoot({
              expanded: true,
              children: tree,
            });
          }
        },
      },
    },
  },

  tbar: [
    {
      text: gettext("Add Target"),
      xtype: "proxmoxButton",
      handler: "onAdd",
      selModel: false,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Create Job"),
      handler: "addJob",
      disabled: true,
      enableFn: function () {
        const recs = this.up("treepanel").getSelection();
        return recs && recs.length === 1;
      },
    },
    "-",
    {
      text: gettext("Edit Target"),
      xtype: "proxmoxButton",
      handler: "onEdit",
      disabled: true,
      enableFn: function () {
        let recs = this.up("treepanel").getSelection();
        return recs.length === 1 && recs[0].get("nodeType") === "target";
      },
    },
    {
      text: gettext("Set S3 Secret Key"),
      xtype: "proxmoxButton",
      handler: "setS3Secret",
      disabled: true,
      enableFn: function () {
        let recs = this.up("treepanel").getSelection();
        if (recs.length !== 1) return false;
        const rec = recs[0];
        return rec.get("nodeType") === "target" && rec.get("type") === "s3";
      },
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove Target"),
      handler: "removeTargets",
      enableFn: function () {
        let recs = this.up("treepanel").getSelection();
        return (
          recs.length > 0 && recs.every((r) => r.get("nodeType") === "target")
        );
      },
      disabled: true,
    },
  ],

  columns: [
    {
      xtype: "treecolumn",
      text: gettext("Name"),
      dataIndex: "name",
      flex: 2,
    },
    {
      text: gettext("Type / FS"),
      flex: 1,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          const t = rec.get("type");
          if (t === "s3") {
            return "S3";
          } else if (t === "local") {
            return "Local";
          } else if (t === "agent") {
            return "Agent";
          }
          return t || "-";
        } else {
          return rec.get("meta_fs") || "-";
        }
      },
    },
    {
      text: gettext("Path / Bucket"),
      flex: 2,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          const t = rec.get("type");
          if (t === "s3") {
            const host = rec.get("s3_host") || "";
            const bucket = rec.get("s3_bucket") || "";
            return bucket ? `${host}/${bucket}` : host || "-";
          }
          if (t === "local") {
            return rec.get("local_path") || "-";
          }
          if (t === "agent") {
            return rec.get("host") || rec.get("name") || "-";
          }
          return "-";
        } else {
          return rec.get("meta_name") || rec.get("volume_name") || "-";
        }
      },
      dataIndex: "dummy",
    },
    {
      text: gettext("Job Count"),
      width: 110,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          return rec.get("job_count");
        }
        return "-";
      },
    },
    {
      header: gettext("Status"),
      width: 140,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          return Ext.ComponentQuery.query("pbsDiskTargetPanel")[0]
            .getController()
            .render_status(rec.get("connection_status"));
        } else if (rec.get("nodeType") === "volume") {
          return Ext.ComponentQuery.query("pbsDiskTargetPanel")[0]
            .getController()
            .render_status(rec.get("accessible"));
        }
        return "-";
      },
    },
    {
      text: gettext("Agent OS"),
      width: 120,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          return rec.get("type") === "agent" ? rec.get("os") || "-" : "-";
        }
        return "-";
      },
    },
    {
      text: gettext("Agent Version"),
      width: 140,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "target") {
          return rec.get("type") === "agent"
            ? rec.get("agent_version") || "-"
            : "-";
        }
        return "-";
      },
    },
    {
      text: gettext("Used Size"),
      width: 130,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "volume") {
          const val = rec.get("meta_used_bytes");
          if (!val && val !== 0) return "-";
          return humanReadableBytes(val);
        }
        return "-";
      },
    },
    {
      text: gettext("Total Size"),
      width: 130,
      renderer: function (v, meta, rec) {
        if (rec.get("nodeType") === "volume") {
          const val = rec.get("meta_total_bytes");
          if (!val && val !== 0) return "-";
          return humanReadableBytes(val);
        }
        return "-";
      },
    },
  ],
});
