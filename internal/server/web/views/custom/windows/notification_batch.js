// Notification Batch edit window — create / edit a notification batch
// and assign jobs (backups, restores, verifications) to it.

Ext.define("PBS.D2DManagement.NotificationBatchEdit", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsNotificationBatchEdit",

  subject: gettext("Notification Batch"),

  isAdd: true,

  fieldDefaults: {
    labelWidth: 140,
  },

  bodyPadding: 0,

  initComponent: function () {
    var me = this;
    var name = me.initialConfig.batchName;

    me.isCreate = !name;
    me.url = name
      ? "/api2/json/d2d/notification-batch?batch=" +
        encodeURIComponent(name)
      : "/api2/json/d2d/notification-batch";
    me.method = name ? "PUT" : "POST";
    me.autoLoad = !!name;
    me.batchName = name || "";

    me.callParent(arguments);
  },

  controller: {
    xclass: "Ext.app.ViewController",

    init: function (view) {
      var me = this;
      // Load available jobs when the window opens
      me.loadAvailableJobs();

      if (view.batchName) {
        me.loadAssignedJobs(view.batchName);
      }
    },

    loadAvailableJobs: function () {
      var me = this;
      // Load backup jobs
      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/backup",
        method: "GET",
        success: function (resp) {
          var jobs = (resp.result.data || []).map(function (j) {
            return { "job-type": "backup", "job-id": j.id, display: "Backup: " + j.id };
          });
          me.populateJobStore(jobs, "backup");
        },
      });

      // Load restore jobs
      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/restore",
        method: "GET",
        success: function (resp) {
          var jobs = (resp.result.data || []).map(function (j) {
            return { "job-type": "restore", "job-id": j.id, display: "Restore: " + j.id };
          });
          me.populateJobStore(jobs, "restore");
        },
      });

      // Load verification jobs
      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/verification",
        method: "GET",
        success: function (resp) {
          var jobs = (resp.result.data || []).map(function (j) {
            return { "job-type": "verification", "job-id": j.id, display: "Verify: " + j.id };
          });
          me.populateJobStore(jobs, "verification");
        },
      });
    },

    populateJobStore: function (jobs, type) {
      var me = this;
      var grid = me.lookup("jobGrid");
      if (!grid) return;

      var store = grid.getStore();
      if (!store) return;

      jobs.forEach(function (j) {
        // Don't add duplicates
        if (store.findExact("job-id", j["job-id"]) < 0) {
          store.add({
            "job-type": j["job-type"],
            "job-id": j["job-id"],
            display: j.display,
            assigned: false,
          });
        }
      });
    },

    loadAssignedJobs: function (batchName) {
      var me = this;
      PBS.PlusUtils.API2Request({
        url:
          "/api2/json/d2d/notification-batch/jobs?batch=" +
          encodeURIComponent(batchName),
        method: "GET",
        success: function (resp) {
          var assigned = resp.result.data || [];
          var grid = me.lookup("jobGrid");
          if (!grid) return;

          var store = grid.getStore();
          if (!store) return;

          // Deferred to ensure populateJobStore has finished first
          Ext.defer(function () {
            assigned.forEach(function (a) {
              store.each(function (rec) {
                if (
                  rec.get("job-type") === a["job-type"] &&
                  rec.get("job-id") === a["job-id"]
                ) {
                  rec.set("assigned", true);
                }
              });
            });
            store.commitChanges();
          }, 500);
        },
      });
    },
  },

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "inputpanel",
        cbind: {
          isCreate: "{isCreate}",
        },

        column1: [
          {
            xtype: "proxmoxtextfield",
            name: "name",
            fieldLabel: gettext("Batch Name"),
            allowBlank: false,
            cbind: {
              editable: "{isCreate}",
              value: "{batchName}",
            },
          },
          {
            xtype: "hidden",
            name: "_jobsDirty",
            value: 0,
          },
        ],

        column2: [
          {
            xtype: "numberfield",
            name: "wait-timeout-secs",
            fieldLabel: gettext("Wait Timeout (seconds)"),
            minValue: 30,
            maxValue: 86400,
            value: 300,
            allowBlank: false,
          },
          {
            xtype: "proxmoxcheckbox",
            name: "send-on-timeout",
            fieldLabel: gettext("Send on Timeout"),
            boxLabel: gettext("Send partial results when timeout is reached"),
            value: true,
            uncheckedValue: 0,
            inputValue: 1,
          },
        ],

        columnB: [
          {
            xtype: "proxmoxtextfield",
            name: "comment",
            fieldLabel: gettext("Comment"),
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
          },
        ],
      },
      {
        title: gettext("Jobs"),
        xtype: "panel",
        layout: "fit",
        maxHeight: 300,
        items: [
          {
            xtype: "grid",
            reference: "jobGrid",
            selType: "checkboxmodel",
            multiSelect: true,
            scroll: true,
            store: {
              fields: ["job-type", "job-id", "display", "assigned"],
            },
            columns: [
              {
                header: gettext("Job"),
                dataIndex: "display",
                renderer: Ext.String.htmlEncode,
                flex: 1,
              },
              {
                header: gettext("Type"),
                dataIndex: "job-type",
                width: 120,
              },
            ],
            listeners: {
              selectionchange: function (sm, selected) {
                var grid = sm.view && sm.view.grid;
                if (!grid) return;
                var store = grid.getStore();
                if (!store) return;
                store.each(function (rec) {
                  rec.set("assigned", false);
                });
                selected.forEach(function (rec) {
                  rec.set("assigned", true);
                });

                // Mark the form dirty so the OK button enables
                var win = grid.up("pbsPlusWindowEdit");
                if (win && win.formPanel) {
                  var dirtyField = win.formPanel.getForm().findField("_jobsDirty");
                  if (dirtyField) {
                    dirtyField.setValue(Date.now());
                  }
                }
              },
            },
          },
        ],
      },
      PBS.D2DManagement.makeSimpleNotificationTab(),
    ],
  },

  onGetValues: function (values) {
    var me = this;

    // Collect selected jobs from the grid
    var grid = me.down("grid[reference=jobGrid]");
    if (grid) {
      var selected = grid.getSelectionModel().getSelection();
      var jobs = selected.map(function (rec) {
        return {
          "job-type": rec.get("job-type"),
          "job-id": rec.get("job-id"),
        };
      });
      values.jobs = JSON.stringify(jobs);
    }

    // Convert checkbox value
    if (values["send-on-timeout"]) {
      values["send-on-timeout"] = "1";
    } else {
      values["send-on-timeout"] = "0";
    }

    // Remove internal dirty tracker
    delete values["_jobsDirty"];

    return values;
  },
});
