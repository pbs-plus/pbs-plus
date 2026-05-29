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
    // Flattened from target
    "target",
    "expected_size",
    // Flattened from history (now returned flat by API)
    "last-run-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    // Flattened from current-stats
    "current_file_count",
    "current_folder_count",
    "current_files_speed",
    "current_bytes_speed",
    "current_bytes_total",
    // Pre-formatted display fields
    "target_size_human",
    "read_speed_human",
    "read_total_human",
    "processing_speed_human",
    "status_parsed",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/backup",
    reader: {
      type: "json",
      rootProperty: "data",
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
    "notification-mode",
    "retry",
    "retry-interval",
    "expected_size",
    // Flattened from dest-target
    "dest-target",
    // Flattened from history
    "last-run-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    // Flattened from current-stats
    "current_file_count",
    "current_folder_count",
    "current_files_speed",
    "current_bytes_speed",
    "current_bytes_total",
    // Pre-formatted display fields
    "target_size_human",
    "read_speed_human",
    "read_total_human",
    "processing_speed_human",
    "status_parsed",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/restore",
    reader: {
      type: "json",
      rootProperty: "data",
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

Ext.define("pbs-verification-job-status", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "backup_job_id",
    "store",
    "ns",
    "mode",
    "schedule",
    "comment",
    "notification-mode",
    "spot_config",
    "next-run",
    "retry",
    "retry-interval",
    "created_at",
    // Flattened from history (now returned flat by API)
    "last-run-upid",
    "last-run-state",
    "last-run-endtime",
    "last-run-starttime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    // Pre-formatted
    "status_parsed",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/verification",
    reader: {
      type: "json",
      rootProperty: "data",
    },
  },
});
