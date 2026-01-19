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
      transform: function (data) {
        return data.map((item) => {
          if (item.history) {
            item["last-run-upid"] = item.history["last-run-upid"];
            item["last-run-state"] = item.history["last-run-state"];
            item["last-run-endtime"] = item.history["last-run-endtime"];
            item["last-successful-endtime"] =
              item.history["last-successful-endtime"];
            item["last-successful-upid"] = item.history["last-successful-upid"];
            item["duration"] = item.history["duration"] || 0;
          }
          if (item["current-stats"]) {
            let s = item["current-stats"];
            item.current_file_count = s.current_file_count || 0;
            item.current_folder_count = s.current_folder_count || 0;
            item.current_files_speed = s.current_files_speed || 0;
            item.current_bytes_speed = s.current_bytes_speed || 0;
            item.current_bytes_total = s.current_bytes_total || 0;
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
      transform: function (data) {
        return data.map((item) => {
          if (item.history) {
            item["last-run-upid"] = item.history["last-run-upid"];
            item["last-run-state"] = item.history["last-run-state"];
            item["last-run-endtime"] = item.history["last-run-endtime"];
            item["last-successful-endtime"] =
              item.history["last-successful-endtime"];
            item["last-successful-upid"] = item.history["last-successful-upid"];
            item["duration"] = item.history["duration"] || 0;
          }
          if (item["current-stats"]) {
            let s = item["current-stats"];
            item.current_file_count = s.current_file_count || 0;
            item.current_folder_count = s.current_folder_count || 0;
            item.current_files_speed = s.current_files_speed || 0;
            item.current_bytes_speed = s.current_bytes_speed || 0;
            item.current_bytes_total = s.current_bytes_total || 0;
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
  proxy: {
    type: "memory",
    reader: {
      type: "json",
      transform: function (data) {
        let transformNode = (node) => {
          if (node.agent_host) {
            node.agent_hostname = node.agent_host.name;
            node.os = node.agent_host.os;
            node.agent_ip = node.agent_host.ip;
          }
          if (node.children) {
            node.children.forEach(transformNode);
          }
          return node;
        };
        return Array.isArray(data)
          ? data.map(transformNode)
          : transformNode(data);
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
      transform: function (data) {
        return data.map((item) => {
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
