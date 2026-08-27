package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var calendarEventSelector = js.Selector{
	Name: "PBS.form.D2DCalendarEvent", XType: "pbsD2DCalendarEvent", Extend: "Ext.form.field.ComboBox",
	ValueField: "value", QueryMode: "local", Editable: new(true), MatchFieldWidth: new(false),
	DeleteEmpty: new(true), Clearable: true,
	SubmitData: js.Func("", `
		let data = null;
		if (!this.disabled && this.submitValue) {
			let val = this.getSubmitValue();
			if (val !== null && val !== "" && val !== "__default__") { data = {}; data[this.getName()] = val; }
			else if (this.getDeleteEmpty()) { data = {}; data.delete = this.getName(); }
		}
		return data;
	`),
	AfterRender: js.Func("field", `field.triggers.clear.setVisible((field.getValue() ?? "") !== "");`),
	OnChange:    js.Func("field, value", `field.triggers.clear.setVisible((value ?? "") !== "");`),
	Options: []js.Option{
		{Value: "mon..fri 18:00", Text: "Weekdays at 6:00 PM"}, {Value: "mon..fri 18:30", Text: "Weekdays at 6:30 PM"},
		{Value: "mon..fri 19:00", Text: "Weekdays at 7:00 PM"}, {Value: "mon..fri 19:30", Text: "Weekdays at 7:30 PM"},
		{Value: "mon..fri 20:00", Text: "Weekdays at 8:00 PM"}, {Value: "mon..fri 20:30", Text: "Weekdays at 8:30 PM"},
		{Value: "mon..fri 21:00", Text: "Weekdays at 9:00 PM"}, {Value: "mon..fri 21:30", Text: "Weekdays at 9:30 PM"},
		{Value: "mon..fri 22:00", Text: "Weekdays at 10:00 PM"}, {Value: "mon..fri 0:00", Text: "Weekdays at 12:00 AM"},
		{Value: "mon..fri 0:30", Text: "Weekdays at 12:30 AM"}, {Value: "mon..fri 1:00", Text: "Weekdays at 1:00 AM"},
		{Value: "mon..fri 1:30", Text: "Weekdays at 1:30 AM"}, {Value: "mon..fri 2:00", Text: "Weekdays at 2:00 AM"},
		{Value: "mon..fri 2:30", Text: "Weekdays at 2:30 AM"}, {Value: "mon..fri 3:00", Text: "Weekdays at 3:00 AM"},
		{Value: "22:00", Text: "Daily at 10:00 PM"}, {Value: "22:30", Text: "Daily at 10:30 PM"},
		{Value: "sat 2:00", Text: "Saturday at 2:00 AM"}, {Value: "sat 2:30", Text: "Saturday at 2:30 AM"},
		{Value: "sat 3:00", Text: "Saturday at 3:00 AM"}, {Value: "sun 2:00", Text: "Sunday at 2:00 AM"},
		{Value: "sun 2:30", Text: "Sunday at 2:30 AM"}, {Value: "sun 3:00", Text: "Sunday at 3:00 AM"},
	},
	Template:        []string{"<ul class=\"x-list-plain\"><tpl for=\".\">", "<li role=\"option\" class=\"x-boundlist-item\">{text}</li>", "</tpl></ul>"},
	DisplayTemplate: []string{"<tpl for=\".\">", "{value}", "</tpl>"},
}
