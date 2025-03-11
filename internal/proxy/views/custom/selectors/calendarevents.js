var backupScheduleDefaults = Ext.create("Ext.data.Store", {
  fields: ["value", "text"],
  data: [
    // Weekday Evening Options (backups after office hours)
    {
      value: "mon..fri 18:00",
      text: gettext("Weekdays at 6:00 PM"),
    },
    {
      value: "mon..fri 18:30",
      text: gettext("Weekdays at 6:30 PM"),
    },
    {
      value: "mon..fri 19:00",
      text: gettext("Weekdays at 7:00 PM"),
    },
    {
      value: "mon..fri 19:30",
      text: gettext("Weekdays at 7:30 PM"),
    },
    {
      value: "mon..fri 20:00",
      text: gettext("Weekdays at 8:00 PM"),
    },
    {
      value: "mon..fri 20:30",
      text: gettext("Weekdays at 8:30 PM"),
    },
    {
      value: "mon..fri 21:00",
      text: gettext("Weekdays at 9:00 PM"),
    },
    {
      value: "mon..fri 21:30",
      text: gettext("Weekdays at 9:30 PM"),
    },
    {
      value: "mon..fri 22:00",
      text: gettext("Weekdays at 10:00 PM"),
    },
    // Weekday Early Morning Options
    {
      value: "mon..fri 0:00",
      text: gettext("Weekdays at 12:00 AM"),
    },
    {
      value: "mon..fri 0:30",
      text: gettext("Weekdays at 12:30 AM"),
    },
    {
      value: "mon..fri 1:00",
      text: gettext("Weekdays at 1:00 AM"),
    },
    {
      value: "mon..fri 1:30",
      text: gettext("Weekdays at 1:30 AM"),
    },
    {
      value: "mon..fri 2:00",
      text: gettext("Weekdays at 2:00 AM"),
    },
    {
      value: "mon..fri 2:30",
      text: gettext("Weekdays at 2:30 AM"),
    },
    {
      value: "mon..fri 3:00",
      text: gettext("Weekdays at 3:00 AM"),
    },
    // Daily Overnight Options
    {
      value: "22:00",
      text: gettext("Daily at 10:00 PM"),
    },
    {
      value: "22:30",
      text: gettext("Daily at 10:30 PM"),
    },
    // Weekend Options
    {
      value: "sat 2:00",
      text: gettext("Saturday at 2:00 AM"),
    },
    {
      value: "sat 2:30",
      text: gettext("Saturday at 2:30 AM"),
    },
    {
      value: "sat 3:00",
      text: gettext("Saturday at 3:00 AM"),
    },
    {
      value: "sun 2:00",
      text: gettext("Sunday at 2:00 AM"),
    },
    {
      value: "sun 2:30",
      text: gettext("Sunday at 2:30 AM"),
    },
    {
      value: "sun 3:00",
      text: gettext("Sunday at 3:00 AM"),
    },
  ],
});

Ext.define("PBS.form.D2DCalendarEvent", {
  extend: "Ext.form.field.ComboBox",
  xtype: "pbsD2DCalendarEvent",

  editable: true,

  valueField: "value",
  queryMode: "local",

  matchFieldWidth: false,

  config: {
    deleteEmpty: true,
  },

  // override framework function to implement deleteEmpty behavior
  getSubmitData: function () {
    let me = this,
      data = null;
    if (!me.disabled && me.submitValue) {
      let val = me.getSubmitValue();
      if (val !== null && val !== "" && val !== "__default__") {
        data = {};
        data[me.getName()] = val;
      } else if (me.getDeleteEmpty()) {
        data = {};
        data.delete = me.getName();
      }
    }
    return data;
  },

  triggers: {
    clear: {
      cls: "pmx-clear-trigger",
      weight: -1,
      hidden: true,
      handler: function () {
        this.triggers.clear.setVisible(false);
        this.setValue("");
      },
    },
  },

  listeners: {
    afterrender: function (field) {
      let value = field.getValue();
      let canClear = (value ?? "") !== "";
      field.triggers.clear.setVisible(canClear);
    },
    change: function (field, value) {
      let canClear = (value ?? "") !== "";
      field.triggers.clear.setVisible(canClear);
    },
  },

  store: backupScheduleDefaults,

  tpl: [
    '<ul class="x-list-plain"><tpl for=".">',
    '<li role="option" class="x-boundlist-item">{text}</li>',
    "</tpl></ul>",
  ],

  displayTpl: ['<tpl for=".">', "{value}", "</tpl>"],
});
