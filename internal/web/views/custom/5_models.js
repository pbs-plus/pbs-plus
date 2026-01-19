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
    { name: "last-run-upid", mapping: "history.last-run-upid" },
    { name: "last-run-state", mapping: "history.last-run-state" },
    { name: "last-run-endtime", mapping: "history.last-run-endtime" },
    {
      name: "last-successful-endtime",
      mapping: "history.last-successful-endtime",
    },
    { name: "last-successful-upid", mapping: "history.last-successful-upid" },
    { name: "duration", mapping: "history.duration" },
    { name: "current_file_count", mapping: "current-stats.current_file_count" },
    {
      name: "current_folder_count",
      mapping: "current-stats.current_folder_count",
    },
    {
      name: "current_files_speed",
      mapping: "current-stats.current_files_speed",
    },
    {
      name: "current_bytes_speed",
      mapping: "current-stats.current_bytes_speed",
    },
    {
      name: "current_bytes_total",
      mapping: "current-stats.current_bytes_total",
    },
    { name: "target", mapping: "target.name" },
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
    "dest-subpath",
    "pre_script",
    "post_script",
    "comment",
    "retry",
    "retry-interval",
    "expected_size",
    { name: "last-run-upid", mapping: "history.last-run-upid" },
    { name: "last-run-state", mapping: "history.last-run-state" },
    { name: "last-run-endtime", mapping: "history.last-run-endtime" },
    {
      name: "last-successful-endtime",
      mapping: "history.last-successful-endtime",
    },
    { name: "last-successful-upid", mapping: "history.last-successful-upid" },
    { name: "duration", mapping: "history.duration" },
    { name: "current_file_count", mapping: "current-stats.current_file_count" },
    {
      name: "current_folder_count",
      mapping: "current-stats.current_folder_count",
    },
    {
      name: "current_files_speed",
      mapping: "current-stats.current_files_speed",
    },
    {
      name: "current_bytes_speed",
      mapping: "current-stats.current_bytes_speed",
    },
    {
      name: "current_bytes_total",
      mapping: "current-stats.current_bytes_total",
    },
    { name: "dest-target", mapping: "dest-target.name" },
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/restore",
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

    { name: "agent_hostname", mapping: "agent_host.name" },
    { name: "os", mapping: "agent_host.os" },
    { name: "agent_ip", mapping: "agent_host.ip" },

    "text",
    "isGroup",
    "groupType",
    "iconCls",

    // Legacy mappings for backward compatibility
    { name: "drive_type", mapping: "target_type" },
    { name: "drive_name", mapping: "volume_name" },
    { name: "drive_fs", mapping: "volume_fs" },
    { name: "drive_total_bytes", mapping: "volume_total_bytes" },
    { name: "drive_used_bytes", mapping: "volume_used_bytes" },
    { name: "drive_free_bytes", mapping: "volume_free_bytes" },
    { name: "drive_total", mapping: "volume_total" },
    { name: "drive_used", mapping: "volume_used" },
    { name: "drive_free", mapping: "volume_free" },
  ],
  idProperty: "name",
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
