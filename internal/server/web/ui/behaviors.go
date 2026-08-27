package ui

import (
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// enableOnSelection enables a toolbar button while the grid has a selection.
var enableOnSelection = js.Func("", `
	let recs = this.up("grid").getSelection();
	return recs.length > 0;
`)

// enableOnRecord enables a button for any record; Proxmox passes null when nothing is selected.
var enableOnRecord = js.Func("rec", `return true;`)

var dashIfEmpty = js.Func("value", `return value || "-";`)

// jobIdle and jobStoppable are selectionEvery expressions over a job record r.
const (
	jobIdle      = `!r.data["last-run-upid"] || !!r.data["last-run-state"]`
	jobStoppable = `!!r.data["last-run-upid"] && ((r.data["last-run-state"] || "") === "" || (r.data["last-run-state"] || "").startsWith("QUEUED:"))`
)

var countIfSet = js.Func("value", `
	if (!value && value !== 0) return "-";
	return value.toLocaleString();
`)

// openTaskLog shows the PBS task viewer for the UPID in the named record field.
func openTaskLog(upidField string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let selection = this.getView().getSelection();
		if (!selection || selection.length < 1) {
			return;
		}
		let upid = selection[0].data[%q];
		if (upid) {
			Ext.create("PBS.plusWindow.TaskViewer", { upid }).show();
		}
	`, upidField))
}

func searchTool() js.Tool {
	return js.Tool{XType: js.XTextField, Reference: "searchField", EmptyText: "Search...", Width: 200, KeyUp: "onSearchKeyUp"}
}

func searchFilter(fields ...string) js.Raw {
	tests := make([]string, len(fields))
	for i, f := range fields {
		tests[i] = fmt.Sprintf(`re.test(rec.get(%q))`, f)
	}
	return js.Func("field", fmt.Sprintf(`
		const val = field.getValue().trim();
		const store = this.getView().getStore();
		store.clearFilter(true);
		if (!val) {
			return;
		}
		const re = new RegExp(Ext.String.escapeRegex(val), "i");
		store.filterBy((rec) => %s);
	`, strings.Join(tests, " || ")))
}

// selectionEvery enables a button while every selected record satisfies expr, with the record bound to r.
func selectionEvery(expr string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let recs = this.up("grid").getSelection();
		return recs.length > 0 && recs.every((r) => %s);
	`, expr))
}

// selectionOne enables a button on a single selected record satisfying expr, with the record bound to r.
func selectionOne(expr string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let recs = this.up("grid").getSelection();
		return recs.length === 1 && (%s);
	`, expr))
}

var enableOnSingleSelection = js.Func("", `
	let recs = this.up("grid").getSelection();
	return recs.length === 1;
`)

// openEditWindow builds a controller handler that shows class and reloads the
// grid on close. idField is empty for add, or the record field carrying the
// record id for edit.
// editSelection opens class for the selected record; windows differ on both the id property name and autoLoad vs autoShow.
func editSelection(class, prop, field, show string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let me = this;
		let selection = me.getView().getSelection();
		if (!selection || selection.length < 1) {
			return;
		}
		Ext.create(%q, {
			%s: selection[0].data.%s,
			%s: true,
			listeners: { destroy: () => me.reload() },
		}).show();
	`, class, prop, field, show))
}

func groupHeader(noun string) string {
	return fmt.Sprintf(`{name:this.formatName} ({rows.length} %s{[values.rows.length > 1 ? "s" : ""]})`, noun)
}

// applyJobData seeds values from a duplicated job, which arrive after callParent.
var applyJobData = js.Func("", `
	let me = this;
	me.callParent();
	if (me.jobData) {
		let data = Ext.apply({}, me.jobData);
		me.setValues(data);
	}
`)

var changerExtraParams = js.Func("", `
	let me = this;
	me.store.proxy.extraParams = me.changer ? { changer: me.changer } : {};
	me.callParent();
`)

var dropDeleteOnCreate = js.Func("values", `
	let me = this;
	if (me.isCreate) {
		delete values.delete;
	}
	return values;
`)

func openEditWindow(class, idField string) js.Raw {
	if idField == "" {
		return js.Func("", fmt.Sprintf(`
			let me = this;
			Ext.create(%q, {
				listeners: { destroy: () => me.reload() },
			}).show();
		`, class))
	}
	return editSelection(class, "contentid", idField, "autoLoad")
}

// editJobWindow opens class on the selected record; duplicateJobWindow drops the id so it saves as new.
func editJobWindow(class string) js.Raw {
	return editSelection(class, "id", "id", "autoShow")
}

func duplicateJobWindow(class string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let me = this;
		let selection = me.getView().getSelection();
		if (!selection || selection.length < 1) {
			return;
		}
		let jobData = Ext.Object.merge({}, selection[0].data);
		delete jobData.id;
		Ext.create(%q, {
			autoShow: true,
			jobData: jobData,
			listeners: { destroy: () => me.reload() },
		}).show();
	`, class))
}

// runJobs POSTs every selected job id to baseURL as repeated job= parameters.
func runJobs(baseURL, singular, plural string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		const me = this;
		const view = me.getView();
		const recs = view.getSelection();
		if (!recs.length) return;
		const ids = recs.map((r) => r.getId());
		const list = ids.map(Ext.String.htmlEncode).join("', '");
		const msg = ids.length > 1
			? Ext.String.format(gettext(%q), list)
			: Ext.String.format(gettext(%q), list);
		Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
			if (btn !== "yes") return;
			PBS.PlusUtils.API2Request({
				url: %q + ids.map((id) => "job=" + encodeURIComponent(encodePathValue(id))).join("&"),
				method: "POST",
				waitMsgTarget: view,
				success: () => me.reload(),
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
			});
		});
	`, plural, singular, baseURL))
}

// A prompt containing {0} is formatted with the list of selected record ids.
func confirmRemove(baseURL, idExpr, prompt string) js.Raw {
	msg := fmt.Sprintf("gettext(%q)", prompt)
	if strings.Contains(prompt, "{0}") {
		msg = fmt.Sprintf(`Ext.String.format(gettext(%q), recs.map((rec) => Ext.String.htmlEncode(rec.getId())).join("', '"))`, prompt)
	}
	return js.Func("", fmt.Sprintf(`
		const me = this;
		const view = me.getView();
		const recs = view.getSelection();
		if (!recs.length) {
			return;
		}
		Ext.Msg.confirm(
			gettext("Confirm"),
			%s,
			(btn) => {
				if (btn !== "yes") {
					return;
				}
				recs.forEach((rec) => {
					PBS.PlusUtils.API2Request({
						url: %q + encodeURIComponent(%s),
						method: "DELETE",
						waitMsgTarget: view,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: () => me.reload(),
					});
				});
			},
		);
	`, msg, baseURL, idExpr))
}

// copySelectionWindow shows one field of the selected record in a copyable dialog.
func copySelectionWindow(title, field string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		let selection = this.getView().getSelection();
		if (!selection || selection.length < 1) {
			return;
		}
		let value = selection[0].data.%s;
		Ext.create("Ext.window.Window", {
			modal: true,
			width: 600,
			title: gettext(%q),
			layout: "form",
			bodyPadding: "10 0",
			items: [{ xtype: "textfield", value: value, editable: false }],
			buttons: [
				{
					xtype: "button",
					iconCls: "fa fa-clipboard",
					handler: async () => await navigator.clipboard.writeText(value),
					text: gettext("Copy"),
				},
				{ text: gettext("Ok"), handler: function () { this.up("window").close(); } },
			],
		}).show();
	`, field, title))
}

func pathKeyedURL(baseURL string) js.Raw {
	return js.Func("initialConfig", fmt.Sprintf(`
		let me = this;
		let contentid = initialConfig.contentid;
		me.isCreate = !contentid;
		me.url = contentid
			? %q + "/" + encodeURIComponent(encodePathValue(contentid))
			: %q;
		me.method = contentid ? "PUT" : "POST";
		return {};
	`, baseURL, baseURL))
}

// codeMirrorField builds the CodeMirror-backed editor component, wired to the
// ExtJS field contract so the parent form can read and write its value.
func codeMirrorField(name, mode string, height int) js.Field {
	return js.Field{
		XType:  js.XComponent,
		Name:   name,
		Height: height,
		ItemID: "scriptEditor",
		Anchor: "100%",
		HTML:   `<div style="height: 100%;"></div>`,
		AfterRender: js.Func("component", fmt.Sprintf(`
			PBS.PlusUtils.LoadCodeMirror(function () {
				let isDark =
					window.matchMedia &&
					window.matchMedia("(prefers-color-scheme: dark)").matches;
				if (Proxmox.Utils && Proxmox.Utils.theme) {
					isDark =
						Proxmox.Utils.theme === "auto"
							? isDark
							: Proxmox.Utils.theme === "dark";
				}
				let editor = CodeMirror(component.el.dom.firstChild, {
					mode: %q,
					lineNumbers: true,
					indentUnit: 2,
					tabSize: 2,
					indentWithTabs: false,
					lineWrapping: false,
					theme: isDark ? "material-darker" : "default",
				});
				component.codeMirror = editor;
				component.setValue = (val) => editor.setValue(val || "");
				component.getValue = () => editor.getValue();
				component.isValid = () => true;
				component.validate = () => true;
				setTimeout(() => editor.refresh(), 1);
			});
		`, mode)),
	}
}
