var verificationModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Random Spot Check", value: "random_spot" },
    // Future: { display: "Metadata Verification", value: "metadata" },
    // Future: { display: "Full Verification", value: "full" },
  ],
});

// --- Sampling strategy descriptions ---

var strategyDescriptions = {
  random:
    '<i class="fa fa-shuffle" style="margin-right:4px;color:#888;"></i>' +
    '<b>Random</b> — Shuffle all eligible files and pick the first N. ' +
    "Provides statistically uniform coverage. Best for general-purpose verification.",
  systematic:
    '<i class="fa fa-arrows-left-right" style="margin-right:4px;color:#888;"></i>' +
    '<b>Systematic</b> — Sort files by path, then pick evenly-spaced entries. ' +
    "Ensures broad spatial coverage across the entire archive.",
  stratified:
    '<i class="fa fa-layer-group" style="margin-right:4px;color:#888;"></i>' +
    '<b>Stratified</b> — Group files by top-level directory, then sample proportionally from each group. ' +
    "Ensures every directory tree is represented.",
};

// --- Size unit helpers ---

var sizeUnitStore = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "B", value: 1 },
    { display: "KiB", value: 1024 },
    { display: "MiB", value: 1048576 },
    { display: "GiB", value: 1073741824 },
    { display: "TiB", value: 1099511627776 },
  ],
});

function renderSizeValue(bytes) {
  if (!bytes) return "-";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KiB";
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MiB";
  return (bytes / 1073741824).toFixed(2) + " GiB";
}

function bestUnitForBytes(bytes) {
  if (!bytes || bytes < 1024) return sizeUnitStore.getAt(0);
  for (var i = sizeUnitStore.getCount() - 1; i >= 1; i--) {
    if (bytes >= sizeUnitStore.getAt(i).get("value")) return sizeUnitStore.getAt(i);
  }
  return sizeUnitStore.getAt(0);
}

// --- Filter edit popup ---

Ext.define("PBS.D2DVerification.FilterEditWindow", {
  extend: "Ext.window.Window",
  alias: "widget.pbsD2DVerificationFilterEditWindow",

  title: gettext("Add Filter"),
  width: 480,
  modal: true,
  layout: "fit",
  resizable: false,

  // config passed in:
  // filterGrid: reference to the filter grid
  // editRecord: existing record to edit (null = add mode)

  initComponent: function () {
    var me = this;
    var isEdit = !!me.editRecord;

    if (isEdit) {
      me.title = gettext("Edit Filter");
    }

    var minBytes = isEdit ? me.editRecord.get("min_size") || 0 : 0;
    var maxBytes = isEdit ? me.editRecord.get("max_size") || 0 : 0;
    var minUnit = bestUnitForBytes(minBytes);
    var maxUnit = bestUnitForBytes(maxBytes);

    me.items = [
      {
        xtype: "form",
        reference: "filterForm",
        bodyPadding: 10,
        border: false,
        fieldDefaults: { labelWidth: 110, anchor: "100%" },
        items: [
          {
            xtype: "textfield",
            name: "path_pattern",
            fieldLabel: gettext("Path Pattern"),
            emptyText: gettext('e.g. /data or *.log'),
            allowBlank: false,
            value: isEdit ? me.editRecord.get("path_pattern") : "",
          },
          {
            xtype: "fieldcontainer",
            fieldLabel: gettext("Min Size"),
            layout: "hbox",
            items: [
              {
                xtype: "numberfield",
                name: "min_size_val",
                minValue: 0,
                decimalPrecision: 2,
                flex: 1,
                value: minBytes > 0 ? (minBytes / minUnit.get("value")) : 0,
              },
              {
                xtype: "combo",
                name: "min_size_unit",
                store: sizeUnitStore,
                displayField: "display",
                valueField: "value",
                queryMode: "local",
                editable: false,
                anyMatch: true,
                forceSelection: true,
                value: minUnit.get("value"),
                width: 80,
                margin: "0 0 0 5",
              },
            ],
          },
          {
            xtype: "fieldcontainer",
            fieldLabel: gettext("Max Size"),
            layout: "hbox",
            items: [
              {
                xtype: "numberfield",
                name: "max_size_val",
                minValue: 0,
                decimalPrecision: 2,
                flex: 1,
                value: maxBytes > 0 ? (maxBytes / maxUnit.get("value")) : 0,
              },
              {
                xtype: "combo",
                name: "max_size_unit",
                store: sizeUnitStore,
                displayField: "display",
                valueField: "value",
                queryMode: "local",
                editable: false,
                anyMatch: true,
                forceSelection: true,
                value: maxUnit.get("value"),
                width: 80,
                margin: "0 0 0 5",
              },
            ],
          },
        ],
        buttons: [
          {
            text: gettext("Cancel"),
            handler: function () {
              me.close();
            },
          },
          {
            text: isEdit ? gettext("Save") : gettext("Add"),
            handler: function () {
              var form = me.down("form");
              if (!form.isValid()) return;

              var vals = form.getValues();
              var minVal = parseFloat(vals.min_size_val) || 0;
              var maxVal = parseFloat(vals.max_size_val) || 0;
              var minFactor = parseInt(vals.min_size_unit, 10) || 1;
              var maxFactor = parseInt(vals.max_size_unit, 10) || 1;

              var values = {
                path_pattern: vals.path_pattern,
                min_size: Math.round(minVal * minFactor),
                max_size: Math.round(maxVal * maxFactor),
              };

              if (isEdit) {
                me.editRecord.set(values);
              } else {
                me.filterGrid.getStore().add(values);
              }
              me.filterGrid.syncHiddenField();
              me.close();
            },
          },
        ],
      },
    ];

    me.callParent();
  },
});

// --- Options tab ---

Ext.define("PBS.D2DVerification.OptionsInputPanel", {
  extend: "Proxmox.panel.InputPanel",
  xtype: "pbsD2DVerificationOptionsPanel",

  column1: [
    {
      xtype: "pmxDisplayEditField",
      name: "id",
      fieldLabel: gettext("Job ID"),
      renderer: Ext.htmlEncode,
      allowBlank: true,
      cbind: {
        editable: "{isCreate}",
      },
    },
    {
      xtype: "combo",
      name: "target_mode",
      fieldLabel: gettext("Target Mode"),
      queryMode: "local",
      store: [
        ["backup_job", gettext("Backup Job")],
        ["namespace", gettext("Namespace")],
      ],
      value: "backup_job",
      editable: false,
      forceSelection: true,
      listeners: {
        change: function (combo, val) {
          var panel = combo.up("pbsD2DVerificationOptionsPanel");
          if (!panel) return;
          var backupCombo = panel.down("[reference=backupJobField]");
          var nsFields = panel.down("[reference=namespaceFields]");
          if (val === "namespace") {
            if (backupCombo) { backupCombo.hide(); backupCombo.disable(); }
            if (nsFields) { nsFields.show(); nsFields.enable(); }
          } else {
            if (backupCombo) { backupCombo.show(); backupCombo.enable(); }
            if (nsFields) { nsFields.hide(); nsFields.disable(); }
          }
        },
      },
    },
    {
      xtype: "combo",
      reference: "backupJobField",
      fieldLabel: gettext("Backup Job"),
      name: "backup_job_id",
      store: {
        fields: ["id"],
        proxy: {
          type: "pbsplus",
          url: pbsPlusBaseUrl + "/api2/json/d2d/backup",
        },
        autoLoad: true,
      },
      displayField: "id",
      valueField: "id",
      allowBlank: false,
    },
    {
      xtype: "fieldcontainer",
      reference: "namespaceFields",
      layout: "vbox",
      hidden: true,
      disabled: true,
      width: "100%",
      items: [
        {
          xtype: "pbsDataStoreSelector",
          fieldLabel: gettext("Datastore"),
          name: "store",
          reference: "nsDatastore",
          allowBlank: false,
          width: "100%",
        },
        {
          xtype: "pbsD2DNamespaceSelector",
          fieldLabel: gettext("Namespace"),
          name: "ns",
          reference: "nsNamespace",
          emptyText: gettext("Root"),
          width: "100%",
          margin: "5 0 0 0",
        },
        {
          xtype: "checkbox",
          name: "recursive",
          fieldLabel: gettext("Recursive"),
          boxLabel: gettext("Include sub-namespaces"),
          inputValue: "true",
          uncheckedValue: "false",
          width: "100%",
          margin: "5 0 0 0",
        },
      ],
    },
  ],

  column2: [
    {
      fieldLabel: gettext("Schedule"),
      xtype: "pbsD2DCalendarEvent",
      name: "schedule",
      emptyText: gettext("none (disabled)"),
      cbind: {
        deleteEmpty: "{!isCreate}",
        value: "{scheduleValue}",
      },
    },
    {
      xtype: "checkbox",
      fieldLabel: gettext("Run after backup"),
      name: "run_on_backup_complete",
      inputValue: "true",
      uncheckedValue: "false",
      boxLabel: gettext("Wait for backup completion"),
      autoEl: {
        tag: "div",
        "data-qtip": gettext(
          "Instead of running at the scheduled time, wait for the backup job to complete " +
          "successfully after the scheduled time has passed."
        ),
      },
    },
    {
      xtype: "combo",
      fieldLabel: gettext("Verification Mode"),
      name: "mode",
      queryMode: "local",
      store: verificationModes,
      displayField: "display",
      valueField: "value",
      editable: false,
      forceSelection: true,
      allowBlank: false,
      value: "random_spot",
    },
    {
      xtype: "proxmoxtextfield",
      fieldLabel: gettext("Number of retries"),
      emptyText: gettext("0"),
      name: "retry",
    },
    {
      xtype: "proxmoxtextfield",
      fieldLabel: gettext("Retry interval (minutes)"),
      emptyText: gettext("1"),
      name: "retry-interval",
    },
  ],

  columnB: [
    {
      fieldLabel: gettext("Comment"),
      xtype: "proxmoxtextfield",
      name: "comment",
      cbind: {
        deleteEmpty: "{!isCreate}",
      },
    },
  ],

  setValues: function (values) {
    var me = this;
    me.callParent([values]);

    // Toggle UI based on target_mode
    if (values.target_mode) {
      var combo = me.down("[name=target_mode]");
      if (combo) combo.setValue(values.target_mode);
    }

    // When loading in namespace mode, set datastore on namespace selector
    if (values.target_mode === "namespace" && values.store) {
      var nsSel = me.down("[reference=nsNamespace]");
      if (nsSel && nsSel.setDatastore) {
        nsSel.setDatastore(values.store);
      }
    }

    return values;
  },
});

// --- Spot Check Settings tab ---

Ext.define("PBS.D2DVerification.SpotCheckInputPanel", {
  extend: "Proxmox.panel.InputPanel",
  xtype: "pbsD2DVerificationSpotCheckPanel",

  column1: [
    {
      xtype: "combo",
      fieldLabel: gettext("Sample Mode"),
      name: "sample_count_mode",
      queryMode: "local",
      store: [
        ["absolute", "Absolute Count"],
        ["percent", "Percentage"],
      ],
      value: "absolute",
      editable: false,
      forceSelection: true,
      listeners: {
        change: function (combo, val) {
          var panel = combo.up("pbsD2DVerificationSpotCheckPanel") ||
            combo.up("panel");
          if (!panel) return;
          var absField = panel.down("numberfield[name=sample_count]");
          var pctField = panel.down("numberfield[name=sample_count_percent]");
          if (val === "percent") {
            if (absField) absField.disable().hide();
            if (pctField) pctField.enable().show();
          } else {
            if (absField) absField.enable().show();
            if (pctField) pctField.disable().hide();
          }
        },
      },
    },
    {
      xtype: "numberfield",
      fieldLabel: gettext("Sample Count"),
      name: "sample_count",
      minValue: 1,
      maxValue: 100000,
      value: 10,
      allowBlank: false,
    },
    {
      xtype: "numberfield",
      fieldLabel: gettext("Sample Percentage"),
      name: "sample_count_percent",
      minValue: 0.01,
      maxValue: 100,
      decimalPrecision: 2,
      value: 10,
      allowBlank: false,
      hidden: true,
      disabled: true,
    },
    {
      xtype: "combo",
      fieldLabel: gettext("Sampling Strategy"),
      name: "sampling_strategy",
      queryMode: "local",
      store: [
        ["random", "Random"],
        ["systematic", "Systematic"],
        ["stratified", "Stratified"],
      ],
      value: "random",
      editable: false,
      forceSelection: true,
      listeners: {
        change: function (combo, val) {
          var descBox = combo.up("pbsD2DVerificationSpotCheckPanel") ||
            combo.up("panel");
          if (descBox && descBox.down("[reference=strategyDesc]")) {
            descBox.down("[reference=strategyDesc]").setHtml(
              strategyDescriptions[val] || ""
            );
          }
        },
      },
    },
    {
      xtype: "component",
      reference: "strategyDesc",
      html: strategyDescriptions["random"],
      margin: "0 0 10 0",
      cls: "x-fieldset",
      style: {
        padding: "8px 10px",
        borderRadius: "4px",
        fontSize: "11px",
        lineHeight: "16px",
      },
    },
    {
      xtype: "checkbox",
      fieldLabel: gettext("Use Latest Snapshot"),
      name: "use_latest",
      inputValue: "true",
      uncheckedValue: "false",
      value: true,
    },
    {
      xtype: "fieldcontainer",
      fieldLabel: gettext("Date Range"),
      layout: "hbox",
      items: [
        {
          xtype: "datefield",
          name: "date_from",
          format: "Y-m-d",
          emptyText: gettext("From"),
          flex: 1,
        },
        {
          xtype: "component",
          html: "&nbsp;&ndash;&nbsp;",
        },
        {
          xtype: "datefield",
          name: "date_to",
          format: "Y-m-d",
          emptyText: gettext("To"),
          flex: 1,
        },
      ],
    },
  ],

  column2: [
    {
      xtype: "numberfield",
      fieldLabel: gettext("Fail Threshold"),
      name: "fail_threshold",
      minValue: 0,
      maxValue: 100000,
      value: 0,
      allowBlank: true,
      emptyText: gettext("No limit"),
      autoEl: {
        tag: "div",
        "data-qtip": gettext(
          "Stop verification after this many file failures. " +
          "Set to 0 to verify all sampled files regardless of failures."
        ),
      },
    },
  ],

  columnB: [
    {
      xtype: "hiddenfield",
      name: "filters",
      reference: "filtersHidden",
      value: "[]",
    },
    {
      xtype: "fieldset",
      title: gettext("File Filters"),
      collapsible: false,
      anchor: "100%",
      padding: "5 5 0 5",
      items: [
        {
          xtype: "grid",
          reference: "filterGrid",
          minHeight: 120,
          maxHeight: 250,
          margin: "0 0 5 0",
          emptyText: gettext("No filters defined — all files are eligible"),
          viewConfig: {
            deferEmptyText: false,
          },
          store: {
            fields: ["path_pattern", "min_size", "max_size"],
            data: [],
          },
          columns: [
            {
              text: gettext("Path Pattern"),
              dataIndex: "path_pattern",
              flex: 3,
              renderer: Ext.String.htmlEncode,
            },
            {
              text: gettext("Min Size"),
              dataIndex: "min_size",
              width: 110,
              renderer: function (v) {
                return v > 0 ? renderSizeValue(v) : "-";
              },
            },
            {
              text: gettext("Max Size"),
              dataIndex: "max_size",
              width: 110,
              renderer: function (v) {
                return v > 0 ? renderSizeValue(v) : "-";
              },
            },
          ],
          tbar: [
            {
              text: gettext("Add"),
              handler: function (btn) {
                var grid = btn.up("grid");
                Ext.create("PBS.D2DVerification.FilterEditWindow", {
                  filterGrid: grid,
                }).show();
              },
            },
            {
              text: gettext("Edit"),
              handler: function (btn) {
                var grid = btn.up("grid");
                var sel = grid.getSelection();
                if (!sel.length) return;
                Ext.create("PBS.D2DVerification.FilterEditWindow", {
                  filterGrid: grid,
                  editRecord: sel[0],
                }).show();
              },
              disabled: true,
              listeners: {
                render: function (btn) {
                  var grid = btn.up("grid");
                  grid.on("selectionchange", function () {
                    btn.setDisabled(grid.getSelection().length !== 1);
                  });
                },
              },
            },
            {
              xtype: "tbseparator",
            },
            {
              text: gettext("Remove"),
              handler: function (btn) {
                var grid = btn.up("grid");
                var sel = grid.getSelection();
                if (!sel.length) return;
                grid.getStore().remove(sel);
                grid.syncHiddenField();
              },
              disabled: true,
              listeners: {
                render: function (btn) {
                  var grid = btn.up("grid");
                  grid.on("selectionchange", function () {
                    btn.setDisabled(grid.getSelection().length === 0);
                  });
                },
              },
            },
          ],
          listeners: {
            boxready: function (grid) {
              var panel = grid.up("pbsD2DVerificationSpotCheckPanel") ||
                grid.up("panel");
              if (panel) {
                panel.filterGrid = grid;

                // Load any pending filters stored by setValues (handles deferred tab rendering)
                if (panel._pendingFilters) {
                  grid.getStore().loadData(panel._pendingFilters);
                  delete panel._pendingFilters;
                }

                // Sync hidden field and reset originalValue so dirty tracking works correctly
                var hiddenField = panel.down("hiddenfield[name=filters]");
                if (hiddenField) {
                  // Sync grid → hidden field
                  var records = [];
                  grid.getStore().each(function (rec) {
                    records.push({
                      path_pattern: rec.get("path_pattern") || "",
                      min_size: rec.get("min_size") || 0,
                      max_size: rec.get("max_size") || 0,
                    });
                  });
                  hiddenField.setValue(Ext.encode(records));
                  // Mark current value as the baseline for dirty detection
                  hiddenField.resetOriginalValue();
                }
              }

              grid.syncHiddenField = function () {
                var hidden = grid.up("pbsD2DVerificationSpotCheckPanel").down("hiddenfield[name=filters]");
                if (!hidden) return;
                var records = [];
                grid.getStore().each(function (rec) {
                  records.push({
                    path_pattern: rec.get("path_pattern") || "",
                    min_size: rec.get("min_size") || 0,
                    max_size: rec.get("max_size") || 0,
                  });
                });
                hidden.setValue(Ext.encode(records));
              };
            },
          },
        },
        {
          xtype: "component",
          html:
            '<span style="color:#555;font-size:11px;">' +
            gettext("Filters restrict which files are eligible for spot checks. " +
              "A file must match at least one filter to be included. " +
              "Leave empty to sample from all files.") +
            "</span>",
        },
      ],
    },
  ],

  setValues: function (values) {
    var me = this;

    // Flatten spot_config fields into top-level values so form fields can bind
    if (values.spot_config && Ext.isObject(values.spot_config)) {
      var sc = values.spot_config;
      if (sc.sample_count !== undefined && values.sample_count === undefined) {
        values.sample_count = sc.sample_count;
      }
      if (sc.sample_count_percent !== undefined && values.sample_count_percent === undefined) {
        values.sample_count_percent = sc.sample_count_percent;
      }
      if (sc.sampling_strategy !== undefined && values.sampling_strategy === undefined) {
        values.sampling_strategy = sc.sampling_strategy;
      }
      if (sc.use_latest !== undefined && values.use_latest === undefined) {
        values.use_latest = String(sc.use_latest);
      }
      if (sc.date_from !== undefined && values.date_from === undefined) {
        values.date_from = sc.date_from;
      }
      if (sc.date_to !== undefined && values.date_to === undefined) {
        values.date_to = sc.date_to;
      }
      if (sc.fail_threshold !== undefined && values.fail_threshold === undefined) {
        values.fail_threshold = sc.fail_threshold;
      }
      if (sc.filters !== undefined && values.filters === undefined) {
        values.filters = Ext.encode(sc.filters);
      }

      // Set sample_count_mode based on which field has a value
      if (sc.sample_count_percent > 0 && values.sample_count_mode === undefined) {
        values.sample_count_mode = "percent";
      }
    }

    me.callParent([values]);

    // Parse filters JSON into the grid
    if (values.filters) {
      try {
        var filters = Ext.decode(values.filters);
        if (Ext.isArray(filters) && filters.length > 0) {
          if (me.filterGrid) {
            // Grid is already rendered — load directly
            me.filterGrid.getStore().loadData(filters);
            if (me.filterGrid.syncHiddenField) {
              me.filterGrid.syncHiddenField();
            }
          } else {
            // Tab not rendered yet — store for boxready to pick up
            me._pendingFilters = filters;
          }
        }
      } catch (e) {
        // ignore bad JSON
      }
    }

    // Update strategy description
    if (values.sampling_strategy) {
      var descBox = me.down("[reference=strategyDesc]");
      if (descBox) {
        descBox.setHtml(strategyDescriptions[values.sampling_strategy] || "");
      }
    }

    // Apply sample_count_mode toggle visibility
    var modeField = me.down("combo[name=sample_count_mode]");
    if (modeField && modeField.getValue()) {
      modeField.fireEvent("change", modeField, modeField.getValue());
    }

    return values;
  },

  getValues: function () {
    var me = this;
    var vals = me.callParent(arguments);
    var mode = vals.sample_count_mode || "absolute";
    if (mode === "percent") {
      delete vals.sample_count;
    } else {
      delete vals.sample_count_percent;
    }
    delete vals.sample_count_mode;
    return vals;
  },
});

// --- Main edit window ---

Ext.define("PBS.D2DVerification.JobEdit", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsD2DVerificationJobEdit",
  mixins: ["Proxmox.Mixin.CBind"],

  userid: undefined,

  isAdd: true,

  subject: gettext("Verification Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  cbindData: function (initialConfig) {
    var me = this;

    var baseurl = "/api2/extjs/config/d2d-verification";
    var id = initialConfig.id;

    me.isCreate = !id;
    me.url = id
      ? baseurl + "/" + encodeURIComponent(encodePathValue(id))
      : baseurl;
    me.method = id ? "PUT" : "POST";
    me.autoLoad = !!id;
    me.scheduleValue = id ? null : "";
    me.modeValue = "random_spot";
    return {};
  },

  controller: {
    xclass: "Ext.app.ViewController",
    control: {
      "pbsDataStoreSelector[name=store]": {
        change: "storeChange",
      },
    },

    storeChange: function (field, value) {
      var me = this;
      var nsSelector = me.lookup("nsNamespace");
      if (nsSelector && nsSelector.setDatastore) {
        nsSelector.setDatastore(value);
      }
    },
  },

  initComponent: function () {
    var me = this;
    me.callParent();

    if (me.jobData) {
      var data = Ext.apply({}, me.jobData);
      me.setValues(data);
    }
  },

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "pbsD2DVerificationOptionsPanel",
        cbind: {
          isCreate: "{isCreate}",
        },
      },
      {
        title: gettext("Spot Check Settings"),
        xtype: "pbsD2DVerificationSpotCheckPanel",
      },
    ],
  },
});
