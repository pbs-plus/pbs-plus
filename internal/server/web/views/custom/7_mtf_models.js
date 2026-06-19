Ext.define("pbs-mtf-job", {
  extend: "Ext.data.Model",
  fields: [
    "id",
    "source_kind",
    "source_ref",
    "source_label",
    "datastore",
    "namespace",
    "schedule",
    "comment",
    "notification-mode",
    "spanning",
    "overwrite_mappings",
    "changer",
    "drive",
    { name: "retry", type: "int" },
    { name: "retry-interval", type: "int" },
    "current_pid",
    "created_at",
    // Flattened from history
    "last-run-upid",
    "last-run-starttime",
    "last-run-state",
    "last-run-status",
    "last-run-endtime",
    "last-successful-endtime",
    "last-successful-upid",
    "duration",
    "status_parsed",
  ],
  idProperty: "id",
  proxy: {
    type: "pbsplus",
    url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-job",
    reader: {
      type: "json",
      rootProperty: "data",
    },
  },
});

Ext.define("pbs-mtf-family", {
  extend: "Ext.data.Model",
  fields: [
    { name: "id", type: "int" },
    "name",
    { name: "total_tapes", type: "int" },
    { name: "cartridge_count", type: "int" },
    "has_catalog",
    { name: "data_set_count", type: "int" },
    "last_scanned",
    "created_at",
  ],
  idProperty: "id",
});

Ext.define("pbs-mtf-cartridge", {
  extend: "Ext.data.Model",
  fields: [
    "barcode",
    "label",
    { name: "media_family_id", type: "int" },
    "media_family_name",
    { name: "sequence", type: "int" },
    "role",
    { name: "catalog_type", type: "int" },
    "is_bkf_file",
    "source_path",
    { name: "volumes", type: "int" },
    { name: "directories", type: "int" },
    { name: "files", type: "int" },
    "status",
    "last_scanned",
    "created_at",
  ],
  idProperty: "barcode",
});

Ext.define("pbs-mtf-dataset", {
  extend: "Ext.data.Model",
  fields: [
    { name: "id", type: "int" },
    { name: "media_family_id", type: "int" },
    { name: "set_number", type: "int" },
    "name",
    "description",
    "owner",
    "machine_name",
    "write_time",
    { name: "num_directories", type: "int" },
    { name: "num_files", type: "int" },
    "size",
    "volumes",
  ],
  idProperty: "id",
});

Ext.define("pbs-mtf-mapping", {
  extend: "Ext.data.Model",
  fields: [
    { name: "id", type: "int" },
    "name",
    { name: "priority", type: "int" },
    "match_regex",
    "template",
    "is_default",
    "enabled",
    "comment",
    "created_at",
  ],
  idProperty: "id",
});

Ext.define("pbs-mtf-changer", {
  extend: "Ext.data.Model",
  fields: ["name", "device", "comment", "created_at"],
  idProperty: "name",
});

Ext.define("pbs-mtf-drive", {
  extend: "Ext.data.Model",
  fields: [
    "name",
    "device",
    "changer",
    { name: "drive_index", type: "int" },
    "comment",
    "created_at",
  ],
  idProperty: "name",
});
