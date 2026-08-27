package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var mtfModels = []js.Value{
	js.Model{
		Name: "pbs-mtf-job",
		Fields: js.Fields(
			"id", "source_kind", "source_ref", "source_label", "datastore", "namespace", "comment",
			"notification-mode", "overwrite_mappings", "changer", "drive", "current_pid", "created_at",
			"last-run-upid", "last-run-starttime", "last-run-state", "last-run-status", "last-run-endtime",
			"last-successful-endtime", "last-successful-upid", "duration", "status_parsed",
			"current_file_count", "current_folder_count", "current_files_speed", "current_bytes_speed",
			"current_bytes_total", "read_speed_human", "read_total_human", "processing_speed_human",
		),
		IDProperty: "id",
		APIPath:    "/api2/extjs/config/mtf-job",
	},
	js.Model{
		Name:       "pbs-mtf-family",
		Fields:     js.Typed(js.Typed(js.Typed(js.Fields("name", "has_catalog", "last_scanned", "created_at"), "id", "int"), "total_tapes", "int"), "cartridge_count", "int"),
		IDProperty: "id",
		Extra: js.Obj{"fields": js.Arr{
			js.Obj{"name": "id", "type": "int"},
			"name",
			js.Obj{"name": "total_tapes", "type": "int"},
			js.Obj{"name": "cartridge_count", "type": "int"},
			"has_catalog",
			js.Obj{"name": "data_set_count", "type": "int"},
			"last_scanned",
			"created_at",
		}},
	},
	js.Model{
		Name: "pbs-mtf-cartridge",
		Fields: js.Fields(
			"barcode", "label", "media_family_name", "role", "is_bkf_file", "source_path", "status",
			"last_scanned", "created_at",
		),
		IDProperty: "barcode",
		Extra: js.Obj{"fields": js.Arr{
			"barcode", "label",
			js.Obj{"name": "media_family_id", "type": "int"},
			"media_family_name",
			js.Obj{"name": "sequence", "type": "int"},
			"role",
			js.Obj{"name": "catalog_type", "type": "int"},
			"is_bkf_file", "source_path",
			js.Obj{"name": "volumes", "type": "int"},
			js.Obj{"name": "directories", "type": "int"},
			js.Obj{"name": "files", "type": "int"},
			"status", "last_scanned", "created_at",
		}},
	},
	js.Model{
		Name:       "pbs-mtf-dataset",
		IDProperty: "id",
		Extra: js.Obj{"fields": js.Arr{
			js.Obj{"name": "id", "type": "int"},
			js.Obj{"name": "media_family_id", "type": "int"},
			js.Obj{"name": "set_number", "type": "int"},
			"name", "description", "owner", "machine_name", "write_time",
			js.Obj{"name": "num_directories", "type": "int"},
			js.Obj{"name": "num_files", "type": "int"},
			"size", "volumes",
		}},
	},
	js.Model{
		Name:       "pbs-mtf-mapping",
		IDProperty: "id",
		Extra: js.Obj{"fields": js.Arr{
			js.Obj{"name": "id", "type": "int"},
			"name",
			js.Obj{"name": "priority", "type": "int"},
			"match_regex", "template", "is_default", "enabled", "comment", "created_at",
		}},
	},
}
