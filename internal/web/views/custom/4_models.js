Ext.define("pbs-disk-backup-job-status", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "store",
    "target",
    "volume",
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
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/json/d2d/backup",
  },
});

Ext.define("pbs-model-targets", {
  extend: "Ext.data.Model",
  fields: [
    "name",
    "local_path",
    "type",
    "job_count",
    "agent_version",
    "connection_status",
    "volumes",
    "os",
    "mount_script",
    "host",
    "s3_access_id",
    "s3_host",
    "s3_region",
    "s3_ssl",
    "s3_path_style",
    "s3_bucket",
  ],
  idProperty: "name",
});

Ext.define("pbs-model-volumes", {
  extend: "Ext.data.Model",
  fields: [
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
  idProperty: "volume_name",
});

Ext.define("pbs-model-tokens", {
  extend: "Ext.data.Model",
  fields: ["token", "comment", "created_at", "revoked"],
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
