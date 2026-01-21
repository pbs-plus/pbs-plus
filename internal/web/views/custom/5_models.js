Ext.define("pbs-disk-backup-status", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "store",
    "mode",
    "sourcemode",
    "readmode",
    "subpath",
    "ns",
    "schedule",
    "comment",
    "notification-mode",
    "pre_script",
    "post_script",
    "next-run",
    "retry",
    "retry-interval",
    "max-dir-entries",
    "rawexclusions",
    "include-xattr",
    "legacy-xattr",
    "last-run-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    "current_file_count",
    "current_folder_count",
    "current_files_speed",
    "current_bytes_speed",
    "current_bytes_total",
    "target",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/backup",
    reader: {
      type: "json",
      rootProperty: "data",
      transform: function (raw) {
        let rows = raw.data || [];
        return rows.map((item) => {
          if (item.history) {
            item["last-run-upid"] = item.history["last-run-upid"];
            item["last-run-state"] = item.history["last-run-state"];
            item["last-run-endtime"] = item.history["last-run-endtime"];
            item["last-successful-endtime"] =
              item.history["last-successful-endtime"];
            item["last-successful-upid"] = item.history["last-successful-upid"];
            item["duration"] = item.history["duration"] || null;
          }
          if (item["current-stats"]) {
            let s = item["current-stats"];
            item.current_file_count = s.current_file_count || null;
            item.current_folder_count = s.current_folder_count || null;
            item.current_files_speed = s.current_files_speed || null;
            item.current_bytes_speed = s.current_bytes_speed || null;
            item.current_bytes_total = s.current_bytes_total || null;
          }
          if (item.target && Ext.isObject(item.target)) {
            item.target = item.target.name;
          }
          return item;
        });
      },
    },
  },
});

Ext.define("pbs-disk-restore-job-status", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "store",
    "ns",
    "snapshot",
    "src-path",
    "dest-subpath",
    "pre_script",
    "post_script",
    "comment",
    "retry",
    "retry-interval",
    "expected_size",
    "last-run-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    "current_file_count",
    "current_folder_count",
    "current_files_speed",
    "current_bytes_speed",
    "current_bytes_total",
    "dest-target",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/restore",
    reader: {
      type: "json",
      rootProperty: "data",
      transform: function (raw) {
        let rows = raw.data || [];
        return rows.map((item) => {
          if (item.history) {
            item["last-run-upid"] = item.history["last-run-upid"];
            item["last-run-state"] = item.history["last-run-state"];
            item["last-run-endtime"] = item.history["last-run-endtime"];
            item["last-successful-endtime"] =
              item.history["last-successful-endtime"];
            item["last-successful-upid"] = item.history["last-successful-upid"];
            item["duration"] = item.history["duration"] || null;
          }
          if (item["current-stats"]) {
            let s = item["current-stats"];
            item.current_file_count = s.current_file_count || null;
            item.current_folder_count = s.current_folder_count || null;
            item.current_files_speed = s.current_files_speed || null;
            item.current_bytes_speed = s.current_bytes_speed || null;
            item.current_bytes_total = s.current_bytes_total || null;
          }
          if (item["dest-target"] && Ext.isObject(item["dest-target"])) {
            item["dest-target"] = item["dest-target"].name;
          }
          return item;
        });
      },
    },
  },
});

Ext.define("pbs-model-targets", {
  extend: "Ext.data.TreeModel",
  fields: [
    "name",
    "path",
    "target_type",
    "mount_script",
    "volume_id",
    "job_count",
    "agent_version",
    "connection_status",
    "volume_type",
    "volume_name",
    "volume_fs",
    "volume_total_bytes",
    "volume_used_bytes",
    "volume_free_bytes",
    "volume_total",
    "volume_used",
    "volume_free",
    "agent_hostname",
    "os",
    "agent_ip",
    "text",
    "isGroup",
    "groupType",
    "iconCls",
  ],
  idProperty: "name",
});

Ext.define("pbs-model-d2d-snapshots", {
  extend: "Ext.data.Model",
  fields: [
    "backup-id",
    "backup-time",
    "backup-type",
    "files",
    "value",
    "display",
  ],
  proxy: {
    type: "memory",
    reader: {
      type: "json",
      transform: function (raw) {
        let rows = Array.isArray(raw) ? raw : raw.data || [];
        return rows.map((item) => {
          let type = item["backup-type"] || "host";
          item.value = `${type}/${item["backup-id"]}/${item["backup-time"]}`;
          if (item["backup-time"]) {
            let time = new Date(item["backup-time"] * 1000);
            item.display = `${Ext.Date.format(time, "Y-m-d H:i:s")} | ${item["backup-id"]}`;
          }
          return item;
        });
      },
    },
  },
});

Ext.define("pbs-model-d2d-snapshots", {
  extend: "Ext.data.Model",
  fields: [
    "backup-id",
    "backup-time",
    "backup-type",
    "files",
    "value",
    "display",
  ],
  proxy: {
    type: "memory",
    reader: {
      type: "json",
      transform: function (raw) {
        let rows = Array.isArray(raw) ? raw : raw.data || [];
        return rows.map((item) => {
          let type = item["backup-type"] || "host";
          item.value = `${type}/${item["backup-id"]}/${item["backup-time"]}`;
          if (item["backup-time"]) {
            let time = new Date(item["backup-time"] * 1000);
            item.display = `${Ext.Date.format(time, "Y-m-d H:i:s")} | ${item["backup-id"]}`;
          }
          return item;
        });
      },
    },
  },
});

Ext.define("pbs-model-tokens", {
  extend: "Ext.data.Model",
  fields: [
    "token",
    "comment",
    "created_at",
    "revoked",
    "win_install",
    "duration",
  ],
  idProperty: "token",
});

Ext.define("pbs-model-exclusions", {
  extend: "Ext.data.Model",
  fields: ["path", "comment"],
  idProperty: "path",
});

Ext.define("pbs-model-scripts", {
  extend: "Ext.data.Model",
  fields: ["path", "description", "job_count", "target_count"],
  idProperty: "path",
});
