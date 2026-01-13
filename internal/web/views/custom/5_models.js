Ext.define("pbs-disk-backup-status", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "store",
    "target",
    "mode",
    "sourcemode",
    "subpath",
    "ns",
    "schedule",
    "comment",
    "duration",
    "current_bytes_total",
    "current_bytes_speed",
    "current_file_count",
    "current_files_speed",
    "current_folder_count",
    "pre_script",
    "post_script",
    "expected_size",
    "next-run",
    "last-run-upid",
    "last-successful-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "max-dir-entries",
    "rawexclusions",
    "retry",
    "retry-interval",
    "include-xattr",
    "legacy-xattr",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/backup",
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
    "dest-target",
    "dest-path",
    "comment",
    "duration",
    "current_bytes_total",
    "current_bytes_speed",
    "current_file_count",
    "current_files_speed",
    "current_folder_count",
    "expected_size",
    "last-run-upid",
    "last-successful-upid",
    "last-run-state",
    "last-run-endtime",
    "last-successful-endtime",
    "retry",
    "retry-interval",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/restore",
  },
});

Ext.define("pbs-model-targets", {
  extend: "Ext.data.Model",
  fields: [
    "name",
    "path",
    "job_count",
    "drive_type",
    "agent_version",
    "connection_status",
    "drive_name",
    "drive_fs",
    "drive_total_bytes",
    "drive_used_bytes",
    "drive_free_bytes",
    "drive_total",
    "drive_used",
    "drive_free",
    "os",
    "mount_script",
  ],
  idProperty: "name",
});

Ext.define("pbs-model-tokens", {
  extend: "Ext.data.Model",
  fields: ["token", "comment", "created_at", "revoked", "win_install"],
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
