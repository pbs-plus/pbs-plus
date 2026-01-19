Ext.define("PBS.D2DManagement.TargetPanelController", {
  extend: "Ext.app.ViewController",
  alias: "controller.pbsDiskTargetPanel",

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
            encodeURIComponent(encodePathValue(rec.getId())),
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
      // No search, show all
      me.buildTree(me.allTargetsData);
      return;
    }

    // Filter targets based on search
    let filteredTargets = me.allTargetsData.filter((target) => {
      let targetData = target.getData();
      return (
        (targetData.name &&
          targetData.name.toLowerCase().includes(searchValue)) ||
        (targetData.path &&
          targetData.path.toLowerCase().includes(searchValue)) ||
        (targetData.agent_hostname &&
          targetData.agent_hostname.toLowerCase().includes(searchValue)) ||
        (targetData.volume_name &&
          targetData.volume_name.toLowerCase().includes(searchValue)) ||
        (targetData.volume_type &&
          targetData.volume_type.toLowerCase().includes(searchValue)) ||
        (targetData.target_type &&
          targetData.target_type.toLowerCase().includes(searchValue))
      );
    });

    me.buildTree(filteredTargets);
  },

  buildTree: function (targets) {
    let me = this;
    let view = me.getView();

    let localTargets = [];
    let agentGroups = {};
    let s3Targets = [];

    targets.forEach((target) => {
      let targetData = target.getData();

      if (targetData.target_type === "agent") {
        let hostname =
          targetData.agent_hostname ||
          (targetData.agent_host && targetData.agent_host.name);
        let os =
          targetData.os || (targetData.agent_host && targetData.agent_host.os);

        if (hostname) {
          if (!agentGroups[hostname]) {
            agentGroups[hostname] = {
              text: hostname,
              os: os,
              children: [],
              isGroup: true,
              groupType: "agent",
              iconCls: "fa fa-server",
              expanded: true,
            };
          }
          agentGroups[hostname].children.push({
            ...targetData,
            agent_hostname: hostname, // Ensure it's set
            os: os, // Ensure it's set
            leaf: true,
            isGroup: false,
            iconCls: "fa fa-hdd-o",
          });
        } else {
          localTargets.push({
            ...targetData,
            leaf: true,
            isGroup: false,
            iconCls: "fa fa-hdd-o",
          });
        }
      } else if (targetData.target_type === "s3") {
        s3Targets.push({
          ...targetData,
          leaf: true,
          isGroup: false,
          iconCls: "fa fa-cloud",
        });
      } else {
        localTargets.push({
          ...targetData,
          leaf: true,
          isGroup: false,
          iconCls: "fa fa-folder",
        });
      }
    });

    let rootChildren = [];

    if (localTargets.length > 0) {
      rootChildren.push({
        text: "Local Targets",
        children: localTargets,
        isGroup: true,
        groupType: "local",
        iconCls: "fa fa-desktop",
        expanded: true,
      });
    }

    if (Object.keys(agentGroups).length > 0) {
      let agentRootNode = {
        text: "Agent Targets",
        children: Object.values(agentGroups),
        isGroup: true,
        groupType: "agent-root",
        iconCls: "fa fa-sitemap",
        expanded: true,
      };
      rootChildren.push(agentRootNode);
    }

    if (s3Targets.length > 0) {
      rootChildren.push({
        text: "S3 Targets",
        children: s3Targets,
        isGroup: true,
        groupType: "s3",
        iconCls: "fa fa-cloud",
        expanded: true,
      });
    }

    view.setRootNode({
      text: "Root",
      expanded: true,
      children: rootChildren,
    });
  },

  reload: function () {
    this.loadData();
  },

  loadData: function () {
    let me = this;
    let view = me.getView();

    Ext.Ajax.request({
      url: pbsPlusBaseUrl + "/api2/json/d2d/target?status=true",
      method: "GET",
      withCredentials: true,
      headers: {
        Accept: "application/json",
      },
      success: function (response) {
        let data = Ext.decode(response.responseText);
        let rawTargets = data.data || [];

        let targets = rawTargets.map((target) => {
          return Ext.create("pbs-model-targets", target);
        });

        // Store all targets for filtering
        me.allTargetsData = targets;

        // Apply current filter if exists
        me.filterTree();
      },
      failure: function (response) {
        Ext.Msg.alert(gettext("Error"), gettext("Failed to load targets"));
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
    if (!value && value !== 0) {
      return "-";
    }
    return humanReadableBytes(value);
  },

  render_field: function (value, metaData, record) {
    if (record.data.isGroup) {
      return "";
    }
    return value || "-";
  },

  init: function () {
    this.searchValue = "";
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
        }
      });
    },
  },

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
          return "";
        }
        if (record.data.target_type === "agent") {
          return record.data.volume_name || record.data.volume_id || "-";
        }
        return value || "-";
      },
    },
    {
      text: gettext("Job Count"),
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
            return `<i class="fa fa-info-circle"></i> ${record.data.os || ""}`;
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
