Ext.define("PBS.D2DManagement.TargetPanelController", {
  extend: "Ext.app.ViewController",
  alias: "controller.pbsDiskTargetPanel",

  viewConfig: {
    getRowClass: function (record) {
      if (record.data.isGroup) {
        return "";
      }

      let jobCount = record.get("job_count");

      if (
        jobCount === undefined ||
        jobCount === null ||
        Number(jobCount) === 0
      ) {
        return "pbs-row-warning-no-jobs";
      }

      return "";
    },
  },
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

    if (!selection || selection.length < 1 || selection[0].data.isGroup) {
      return;
    }

    let targetName = selection[0].data.name;

    Ext.create("PBS.D2DManagement.BackupJobEdit", {
      autoShow: true,
      jobData: { target: targetName },
      listeners: {
        destroy: function () {
          me.reload();
        },
      },
    }).show();
  },

  removeItems: function () {
    const me = this;
    const view = me.getView();
    const selections = view.getSelection();

    if (!selections.length) return;

    const agentHosts = selections.filter(
      (rec) => rec.data.isGroup && rec.data.groupType === "agent",
    );
    const targets = selections.filter((rec) => !rec.data.isGroup);

    if (!agentHosts.length && !targets.length) return;

    let confirmMsg = "";
    if (agentHosts.length > 0 && targets.length > 0) {
      confirmMsg = `Remove ${agentHosts.length} agent host(s) and ${targets.length} target(s)? This will delete all targets associated with the agent hosts.`;
    } else if (agentHosts.length > 0) {
      confirmMsg = `Remove ${agentHosts.length} agent host(s)? This will delete all targets associated with these hosts.`;
    } else {
      confirmMsg = `Remove ${targets.length} target(s)?`;
    }

    Ext.Msg.confirm(gettext("Confirm"), confirmMsg, (btn) => {
      if (btn !== "yes") return;

      let requestsCompleted = 0;
      let totalRequests = agentHosts.length + targets.length;
      let hasError = false;

      const checkCompletion = () => {
        requestsCompleted++;
        if (requestsCompleted === totalRequests) {
          if (!hasError) {
            me.reload();
          }
        }
      };

      agentHosts.forEach((rec) => {
        PBS.PlusUtils.API2Request({
          url:
            "/api2/extjs/config/d2d-agent/" +
            encodeURIComponent(encodePathValue(rec.data.text)),
          method: "DELETE",
          waitMsgTarget: view,
          failure: (resp) => {
            hasError = true;
            Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            checkCompletion();
          },
          success: () => checkCompletion(),
        });
      });

      targets.forEach((rec) => {
        PBS.PlusUtils.API2Request({
          url:
            "/api2/extjs/config/d2d-target/" +
            encodeURIComponent(encodePathValue(rec.get("name"))),
          method: "DELETE",
          waitMsgTarget: view,
          failure: (resp) => {
            hasError = true;
            Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            checkCompletion();
          },
          success: () => checkCompletion(),
        });
      });
    });
  },

  onEdit: function () {
    let me = this;
    let view = me.getView();
    let selection = view.getSelection();
    if (!selection || selection.length < 1 || selection[0].data.isGroup) {
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

  setS3Secret: function () {
    let me = this;
    let view = me.getView();
    let selection = view.getSelection();
    if (!selection || selection.length < 1 || selection[0].data.isGroup) {
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

  onSearch: function (field, value) {
    let me = this;
    me.searchValue = value.toLowerCase();
    me.filterTree();
  },

  onClearSearch: function () {
    let me = this;
    let view = me.getView();
    let searchField = view.down("#targetSearchField");

    if (searchField) {
      searchField.setValue("");
    }

    me.searchValue = "";
    me.filterTree();
  },

  filterTree: function () {
    let me = this;
    let view = me.getView();
    let searchValue = me.searchValue || "";

    if (!me.allTargetsData) {
      return;
    }

    if (!searchValue) {
      // No search, reload tree from server
      me.loadData();
      return;
    }

    // Filter: reload tree and then hide non-matching nodes
    // For simplicity, we filter client-side after loading
    let filtered = me.allTargetsData.filter(function (node) {
      let d = node.data;
      return (
        (d.name && d.name.toLowerCase().includes(searchValue)) ||
        (d.path && d.path.toLowerCase().includes(searchValue)) ||
        (d.agent_hostname && d.agent_hostname.toLowerCase().includes(searchValue)) ||
        (d.ip && d.ip.toLowerCase().includes(searchValue)) ||
        (d.volume_name && d.volume_name.toLowerCase().includes(searchValue)) ||
        (d.volume_type && d.volume_type.toLowerCase().includes(searchValue)) ||
        (d.target_type && d.target_type.toLowerCase().includes(searchValue))
      );
    });

    // Build filtered tree
    let localTargets = [];
    let agentGroups = {};
    let s3Targets = [];

    filtered.forEach(function (node) {
      let targetData = node.data;
      let treeNode = Ext.apply({}, targetData);
      treeNode.leaf = true;
      treeNode.isGroup = false;

      if (targetData.target_type === "agent") {
        let hostname = targetData.agent_hostname;
        treeNode.iconCls = "fa fa-hdd-o";
        if (hostname) {
          if (!agentGroups[hostname]) {
            agentGroups[hostname] = {
              text: hostname,
              os: targetData.os,
              ip: targetData.ip,
              children: [],
              isGroup: true,
              groupType: "agent",
              iconCls: "fa fa-server",
              expanded: true,
            };
          }
          agentGroups[hostname].children.push(treeNode);
        } else {
          localTargets.push(treeNode);
        }
      } else if (targetData.target_type === "s3") {
        treeNode.iconCls = "fa fa-cloud";
        s3Targets.push(treeNode);
      } else {
        treeNode.iconCls = "fa fa-folder";
        localTargets.push(treeNode);
      }
    });

    let rootChildren = [];
    if (localTargets.length > 0) {
      rootChildren.push({ text: "Local Targets", children: localTargets, isGroup: true, groupType: "local", iconCls: "fa fa-desktop", expanded: true });
    }
    if (Object.keys(agentGroups).length > 0) {
      rootChildren.push({ text: "Agent Targets", children: Object.values(agentGroups), isGroup: true, groupType: "agent-root", iconCls: "fa fa-sitemap", expanded: true });
    }
    if (s3Targets.length > 0) {
      rootChildren.push({ text: "S3 Targets", children: s3Targets, isGroup: true, groupType: "s3", iconCls: "fa fa-cloud", expanded: true });
    }

    view.setRootNode({ text: "Root", expanded: true, children: rootChildren });
  },

  reload: function () {
    this.loadData();
  },

  loadData: function () {
    let me = this;
    let view = me.getView();

    Ext.Ajax.request({
      url: pbsPlusBaseUrl + "/api2/json/d2d/target/tree",
      method: "GET",
      withCredentials: true,
      headers: {
        Accept: "application/json",
      },
      success: function (response) {
        let data = Ext.decode(response.responseText);
        let treeNodes = data.data || [];

        let rootChildren = treeNodes.map(function (node) {
          return me.convertTreeNode(node);
        });

        view.setRootNode({
          text: "Root",
          expanded: true,
          children: rootChildren,
        });

        // Store all targets for filtering
        me.allTargetsData = [];
        view.getRootNode().cascadeBy(function (node) {
          if (!node.data.isGroup) {
            me.allTargetsData.push(node);
          }
        });

        // Async: fetch detailed statuses
        me.loadStatuses();
      },
      failure: function (response) {
        Ext.Msg.alert(gettext("Error"), gettext("Failed to load targets"));
      },
    });
  },

  convertTreeNode: function (node) {
    let result = {
      text: node.text,
      iconCls: node.iconCls,
      expanded: node.expanded,
      isGroup: node.isGroup,
      groupType: node.groupType,
      leaf: node.leaf,
    };

    if (!node.isGroup) {
      Ext.apply(result, {
        name: node.name,
        path: node.path,
        target_type: node.target_type,
        mount_script: node.mount_script,
        volume_id: node.volume_id,
        job_count: node.job_count,
        agent_version: node.agent_version,
        connection_status: node.connection_status,
        volume_type: node.volume_type,
        volume_name: node.volume_name,
        volume_fs: node.volume_fs,
        volume_total_bytes: node.volume_total_bytes,
        volume_used_bytes: node.volume_used_bytes,
        volume_free_bytes: node.volume_free_bytes,
        volume_total: node.volume_total || node.volume_total_human,
        volume_used: node.volume_used || node.volume_used_human,
        volume_free: node.volume_free || node.volume_free_human,
        agent_hostname: node.agent_hostname,
        os: node.os,
        ip: node.ip,
      });
    }

    if (node.children && node.children.length > 0) {
      result.children = node.children.map(function (child) {
        return me.convertTreeNode(child);
      }.bind(this));
      result.leaf = false;
    }

    return result;
  },

  loadStatuses: function () {
    let me = this;
    let view = me.getView();

    Ext.Ajax.request({
      url: pbsPlusBaseUrl + "/api2/extjs/config/d2d-target-status?refresh=true",
      method: "GET",
      withCredentials: true,
      headers: {
        Accept: "application/json",
      },
      success: function (response) {
        let statuses = Ext.decode(response.responseText);
        if (!statuses || Object.keys(statuses).length === 0) {
          return;
        }

        let store = view.getStore();
        store.each(function (node) {
          node.eachChild(function (child) {
            let name = child.get("name");
            if (name && statuses[name]) {
              let st = statuses[name];
              child.set("agent_version", st.AgentVersion || "");
              child.set("connection_status", st.ConnectionStatus);
            }
          });
        });
      },
    });
  },

  stopStore: function () {},

  startStore: function () {
    this.loadData();
  },

  render_status: function (value, metaData, record) {
    if (record.data.isGroup) {
      return "";
    }

    if (value === true) {
      return '<i class="fa fa-check good"></i> Reachable';
    } else {
      return '<i class="fa fa-times critical"></i> Unreachable';
    }
  },

  render_path: function (value, metaData, record) {
    if (record.data.isGroup) {
      return "";
    }
    return value || "-";
  },

  render_bytes: function (value, metaData, record) {
    if (record.data.isGroup) {
      return "";
    }
    return value || "-";
  },

  render_field: function (value, metaData, record) {
    if (record.data.isGroup) {
      return "";
    }
    return value || "-";
  },

  init: function () {
    this.searchValue = "";

    if (!document.getElementById("pbs-target-panel-styles")) {
      const style = document.createElement("style");
      style.id = "pbs-target-panel-styles";
      style.innerHTML = `
          .pbs-row-warning-no-jobs .x-grid-cell {
            background-color: #ffc107 !important;
          }
          
          @media (prefers-color-scheme: dark) {
            .pbs-row-warning-no-jobs .x-grid-cell {
              background-color: rgba(255, 193, 7, 0.35) !important;
            }
          }
        `;
      document.head.appendChild(style);
    }

    this.loadData();
  },
});

Ext.define("PBS.D2DManagement.TargetPanel", {
  extend: "Ext.tree.Panel",
  alias: "widget.pbsDiskTargetPanel",

  controller: "pbsDiskTargetPanel",

  stateful: true,
  stateId: "grid-disk-backup-targets-v1",
  rootVisible: false,
  useArrows: true,
  rowLines: true,

  selModel: {
    selType: "rowmodel",
    mode: "SINGLE",
    allowDeselect: true,
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: function (view, record) {
      if (!record.data.isGroup) {
        this.getController().onEdit();
      }
    },
    selectionchange: function (selModel, selected) {
      let me = this;
      let rec = selected[0] || null;

      me.query("proxmoxButton").forEach((btn) => {
        if (btn.enableFn) {
          btn.setDisabled(!btn.enableFn(rec));
        } else if (btn.selModel !== false) {
          btn.setDisabled(!rec);
        } else {
          btn.setDisabled(false);
        }
      });
    },
  },

  tbar: [
    {
      text: gettext("Add"),
      xtype: "proxmoxButton",
      handler: "onAdd",
      enableFn: function (rec) {
        return true;
      },
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Create Job"),
      handler: "addJob",
      disabled: true,
      enableFn: function (rec) {
        return rec && !rec.data.isGroup;
      },
    },
    "-",
    {
      text: gettext("Edit"),
      xtype: "proxmoxButton",
      handler: "onEdit",
      disabled: true,
      enableFn: function (rec) {
        return rec && !rec.data.isGroup;
      },
    },
    {
      text: gettext("Set S3 Secret Key"),
      xtype: "proxmoxButton",
      handler: "setS3Secret",
      disabled: true,
      enableFn: function (rec) {
        return rec && !rec.data.isGroup && rec.data.target_type === "s3";
      },
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeItems",
      enableFn: function () {
        let view = this.up("treepanel");
        let selections = view.getSelection();
        return selections.some(
          (rec) =>
            !rec.data.isGroup ||
            (rec.data.isGroup && rec.data.groupType === "agent"),
        );
      },
      disabled: true,
    },
    "->",
    {
      xtype: "textfield",
      itemId: "targetSearchField",
      emptyText: gettext("Search targets..."),
      width: 250,
      enableKeyEvents: true,
      triggers: {
        clear: {
          cls: "x-form-clear-trigger",
          handler: "onClearSearch",
        },
      },
      listeners: {
        change: {
          fn: "onSearch",
          buffer: 300,
        },
      },
    },
  ],

  columns: [
    {
      xtype: "treecolumn",
      text: gettext("Name"),
      dataIndex: "name",
      flex: 1,
      renderer: function (value, metaData, record) {
        if (record.data.isGroup) {
          return record.data.text;
        }
        return value;
      },
    },
    {
      text: gettext("Path / Volume"),
      dataIndex: "path",
      flex: 2,
      renderer: function (value, metaData, record) {
        if (record.data.isGroup) {
          if (record.data.groupType === "agent") {
            return `<i class="fa fa-info-circle"></i> Host IP: ${record.data.ip || ""}`;
          }
          return "";
        }
        if (record.data.target_type === "agent") {
          return record.data.volume_name || record.data.volume_id || "-";
        }
        return value || "-";
      },
    },
    {
      text: gettext("Backup Job Count"),
      dataIndex: "job_count",
      flex: 1,
      renderer: "render_field",
    },
    {
      text: gettext("Type"),
      dataIndex: "volume_type",
      flex: 1,
      renderer: function (value, metaData, record) {
        if (record.data.isGroup) {
          if (record.data.groupType === "agent") {
            return `<i class="fa fa-info-circle"></i> Host OS: ${record.data.os || ""}`;
          }
          return "";
        }
        return value || record.data.target_type || "-";
      },
    },
    {
      text: gettext("Drive Name"),
      dataIndex: "volume_name",
      flex: 1,
      renderer: "render_field",
    },
    {
      text: gettext("File System"),
      dataIndex: "volume_fs",
      flex: 1,
      renderer: "render_field",
    },
    {
      text: gettext("Used Size"),
      dataIndex: "volume_used_bytes",
      renderer: "render_bytes",
      flex: 1,
    },
    {
      text: gettext("Total Size"),
      dataIndex: "volume_total_bytes",
      renderer: "render_bytes",
      flex: 1,
    },
    {
      header: gettext("Status"),
      dataIndex: "connection_status",
      renderer: "render_status",
      flex: 1,
    },
    {
      text: gettext("Agent Version"),
      dataIndex: "agent_version",
      flex: 1,
      renderer: "render_field",
    },
  ],
});
