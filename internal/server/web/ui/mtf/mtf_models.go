package mtf

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

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
		Name: "pbs-mtf-family",
		Fields: []js.ModelField{
			{Name: "id", Type: "int"}, {Name: "name"}, {Name: "total_tapes", Type: "int"},
			{Name: "cartridge_count", Type: "int"}, {Name: "has_catalog"},
			{Name: "data_set_count", Type: "int"}, {Name: "last_scanned"}, {Name: "created_at"},
		},
		IDProperty: "id",
	},
	js.Model{
		Name: "pbs-mtf-cartridge",
		Fields: []js.ModelField{
			{Name: "barcode"}, {Name: "label"}, {Name: "media_family_id", Type: "int"},
			{Name: "media_family_name"}, {Name: "sequence", Type: "int"}, {Name: "role"},
			{Name: "catalog_type", Type: "int"}, {Name: "is_bkf_file"}, {Name: "source_path"},
			{Name: "volumes", Type: "int"}, {Name: "directories", Type: "int"},
			{Name: "files", Type: "int"}, {Name: "status"}, {Name: "last_scanned"},
			{Name: "created_at"},
		},
		IDProperty: "barcode",
	},
	js.Model{
		Name: "pbs-mtf-dataset",
		Fields: []js.ModelField{
			{Name: "id", Type: "int"}, {Name: "media_family_id", Type: "int"},
			{Name: "set_number", Type: "int"}, {Name: "name"}, {Name: "description"},
			{Name: "owner"}, {Name: "machine_name"}, {Name: "write_time"},
			{Name: "num_directories", Type: "int"}, {Name: "num_files", Type: "int"},
			{Name: "size"}, {Name: "volumes"},
		},
		IDProperty: "id",
	},
	js.Model{
		Name: "pbs-mtf-mapping",
		Fields: []js.ModelField{
			{Name: "id", Type: "int"}, {Name: "name"}, {Name: "priority", Type: "int"},
			{Name: "match_regex"}, {Name: "template"}, {Name: "is_default"}, {Name: "enabled"},
			{Name: "comment"}, {Name: "created_at"},
		},
		IDProperty: "id",
	},
}
