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
    "expected_size",
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
            item.expected_size = item.target.volume_used_bytes;
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
            item.expected_size = item["dest-target"].volume_used_bytes;
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
    {
      name: "value",
      convert: function (v, record) {
        if (v) return v;
        if (!record.data["backup-id"]) return "";
        let type = record.data["backup-type"] || "host";
        return `${type}/${record.data["backup-id"]}/${record.data["backup-time"]}`;
      },
    },
    {
      name: "display",
      convert: function (v, record) {
        if (record.data["backup-time"]) {
          let time = new Date(record.data["backup-time"] * 1000);
          return `${Ext.Date.format(time, "Y-m-d H:i:s")} | ${record.data["backup-id"]}`;
        }
        return v || record.data.value || "";
      },
    },
  ],
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
