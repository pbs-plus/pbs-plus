var verificationModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Random Spot Check", value: "random_spot" },
    // Future: { display: "Metadata Verification", value: "metadata" },
    // Future: { display: "Full Verification", value: "full" },
  ],
});

// --- Size unit helpers ---

var sizeUnits = [
  { display: "B", factor: 1 },
  { display: "KiB", factor: 1024 },
  { display: "MiB", factor: 1048576 },
  { display: "GiB", factor: 1073741824 },
  { display: "TiB", factor: 1099511627776 },
];

var sizeUnitStore = Ext.create("Ext.data.Store", {
  fields: ["display", "factor"],
  data: sizeUnits,
});

function renderSizeValue(bytes) {
  if (!bytes) return "-";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KiB";
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MiB";
  return (bytes / 1073741824).toFixed(2) + " GiB";
}

function bestUnitForBytes(bytes) {
  if (!bytes || bytes < 1024) return sizeUnits[0];
  for (var i = sizeUnits.length - 1; i >= 1; i--) {
    if (bytes >= sizeUnits[i].factor) return sizeUnits[i];
  }
  return sizeUnits[0];
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

    // Resolve best unit for existing values
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
                value: minBytes > 0 ? (minBytes / minUnit.factor) : 0,
                listeners: {
                  change: function (f) {
                    f.up("form").down("[name=min_size_unit]").setReadOnly(f.getValue() === 0);
                  },
                  afterrender: function (f) {
                    f.up("form").down("[name=min_size_unit]").setReadOnly(f.getValue() === 0);
                  },
                },
              },
              {
                xtype: "combobox",
                name: "min_size_unit",
                store: sizeUnitStore,
                displayField: "display",
                valueField: "factor",
                value: minUnit.factor,
                width: 70,
                editable: false,
                forceSelection: true,
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
                value: maxBytes > 0 ? (maxBytes / maxUnit.factor) : 0,
                listeners: {
                  change: function (f) {
                    f.up("form").down("[name=max_size_unit]").setReadOnly(f.getValue() === 0);
                  },
                  afterrender: function (f) {
                    f.up("form").down("[name=max_size_unit]").setReadOnly(f.getValue() === 0);
                  },
                },
              },
              {
                xtype: "combobox",
                name: "max_size_unit",
                store: sizeUnitStore,
                displayField: "display",
                valueField: "factor",
                value: maxUnit.factor,
                width: 70,
                editable: false,
                forceSelection: true,
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
      xtype: "combobox",
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
      cbind: {
        value: "{modeValue}",
      },
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
});

// --- Spot Check Settings tab ---

Ext.define("PBS.D2DVerification.SpotCheckInputPanel", {
  extend: "Proxmox.panel.InputPanel",
  xtype: "pbsD2DVerificationSpotCheckPanel",

  column1: [
    {
      xtype: "numberfield",
      fieldLabel: gettext("Sample Count"),
      name: "sample_count",
      minValue: 1,
      maxValue: 10000,
      value: 10,
      allowBlank: false,
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
      xtype: "datefield",
      fieldLabel: gettext("Date From"),
      name: "date_from",
      format: "Y-m-d",
      emptyText: gettext("Optional"),
    },
    {
      xtype: "datefield",
      fieldLabel: gettext("Date To"),
      name: "date_to",
      format: "Y-m-d",
      emptyText: gettext("Optional"),
    },
  ],

  column2: [],

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
              // Stash a reference so the parent panel can access it
              var panel = grid.up("pbsD2DVerificationSpotCheckPanel") ||
                grid.up("panel");
              if (panel) {
                panel.filterGrid = grid;
              }

              // Provide sync helper
              grid.syncHiddenField = function () {
                var hidden = grid.up("fieldset").down("hiddenfield[name=filters]");
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
    me.callParent([values]);

    // Parse filters JSON into the grid
    if (values.filters && me.filterGrid) {
      try {
        var filters = Ext.decode(values.filters);
        if (Ext.isArray(filters)) {
          me.filterGrid.getStore().loadData(filters);
        }
      } catch (e) {
        // ignore bad JSON
      }
    }

    return values;
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
    me.modeValue = id ? null : "random_spot";
    return {};
  },

  controller: {
    xclass: "Ext.app.ViewController",
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
