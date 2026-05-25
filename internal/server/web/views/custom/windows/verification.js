var verificationModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Random Spot Check", value: "random_spot" },
    // Future: { display: "Metadata Verification", value: "metadata" },
    // Future: { display: "Full Verification", value: "full" },
  ],
});

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
        xtype: "proxmoxPanel",
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
          {
            xtype: "pbsDataStoreSelector",
            fieldLabel: gettext("Datastore"),
            name: "store",
          },
          {
            xtype: "pbsD2DNamespaceSelector",
            fieldLabel: gettext("Namespace"),
            emptyText: gettext("Root"),
            name: "ns",
            reference: "namespace",
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
      },
      {
        title: gettext("Spot Check Settings"),
        xtype: "proxmoxPanel",
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
            xtype: "textarea",
            name: "filters",
            height: 100,
            fieldLabel: gettext("Filters (JSON)"),
            value: "",
            emptyText: gettext(
              'JSON array: [{"path_pattern":"/data","min_size":1024}]'
            ),
          },
        ],
      },
    ],
  },
});
