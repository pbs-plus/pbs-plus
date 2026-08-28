package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var models = []js.Value{
	js.Model{
		Name: "pbs-disk-backup-status",
		Fields: js.Typed(js.Fields(
			"id", "store", "mode", "sourcemode", "readmode", "subpath", "ns", "schedule", "comment",
			"notification-mode", "pre_script", "post_script", "next-run", "retry", "retry-interval",
			"max-dir-entries", "rawexclusions", "include-xattr", "legacy-xattr", "target", "expected_size",
			"last-run-upid", "last-run-state", "last-run-endtime", "last-successful-endtime",
			"last-successful-upid", "duration", "current_file_count", "current_folder_count",
			"current_files_speed", "current_bytes_speed", "current_bytes_total", "target_size_human",
			"read_speed_human", "read_total_human", "processing_speed_human", "status_parsed",
		), "stale", "bool"),
		IDProperty: "id",
		APIPath:    "/api2/json/d2d/backup",
	},
	js.Model{
		Name: "pbs-disk-restore-job-status",
		Fields: js.Fields(
			"id", "store", "ns", "snapshot", "src-path", "dest-subpath", "pre_script", "post_script",
			"comment", "notification-mode", "retry", "retry-interval", "expected_size", "dest-target",
			"last-run-upid", "last-run-state", "last-run-endtime", "last-successful-endtime",
			"last-successful-upid", "duration", "current_file_count", "current_folder_count",
			"current_files_speed", "current_bytes_speed", "current_bytes_total", "target_size_human",
			"read_speed_human", "read_total_human", "processing_speed_human", "status_parsed",
		),
		IDProperty: "id",
		APIPath:    "/api2/json/d2d/restore",
	},
	js.Model{
		Name:   "pbs-model-targets",
		Extend: "Ext.data.TreeModel",
		Fields: js.Fields(
			"name", "path", "target_type", "mount_script", "volume_id", "job_count", "agent_version",
			"connection_status", "volume_type", "volume_name", "volume_fs", "volume_total_bytes",
			"volume_used_bytes", "volume_free_bytes", "volume_total", "volume_used", "volume_free",
			"agent_hostname", "os", "agent_ip", "text", "isGroup", "groupType", "iconCls",
		),
		IDProperty: "name",
	},
	js.Model{
		Name: "pbs-model-d2d-snapshots",
		Fields: []js.ModelField{
			{Name: "backup-id"},
			{Name: "backup-time"},
			{Name: "backup-type"},
			{Name: "files"},
			{Name: "value", Convert: js.Func("v, record", `
				if (v) return v;
				if (!record.data["backup-id"]) return "";
				let type = record.data["backup-type"] || "host";
				return type + "/" + record.data["backup-id"] + "/" + record.data["backup-time"];
			`)},
			{Name: "display", Convert: js.Func("v, record", `
				if (record.data["backup-time"]) {
					let time = new Date(record.data["backup-time"] * 1000);
					return Ext.Date.format(time, "Y-m-d H:i:s") + " | " + record.data["backup-id"];
				}
				return v || record.data.value || "";
			`)},
		},
	},
	js.Model{
		Name:       "pbs-model-tokens",
		Fields:     js.Fields("token", "comment", "created_at", "revoked", "win_install", "duration"),
		IDProperty: "token",
	},
	js.Model{
		Name:       "pbs-model-exclusions",
		Fields:     js.Fields("path", "comment"),
		IDProperty: "path",
	},
	scriptModel,
	js.Model{
		Name: "pbs-verification-job-status",
		Fields: js.Fields(
			"id", "backup_job_id", "store", "ns", "mode", "schedule", "comment", "notification-mode",
			"spot_config", "next-run", "retry", "retry-interval", "created_at", "last-run-upid",
			"last-run-state", "last-run-endtime", "last-run-starttime", "last-successful-endtime",
			"last-successful-upid", "duration", "status_parsed",
		),
		IDProperty: "id",
		APIPath:    "/api2/json/d2d/verification",
	},
}
