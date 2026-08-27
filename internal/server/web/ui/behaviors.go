package ui

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// enableOnSelection enables a toolbar button while the grid has a selection.
var enableOnSelection = js.Func("", `
	let recs = this.up("grid").getSelection();
	return recs.length > 0;
`)

// openEditWindow builds a controller handler that shows class and reloads the
// grid on close. idField is empty for add, or the record field carrying the
// record id for edit.
func openEditWindow(class, idField string) js.Raw {
	if idField == "" {
		return js.Func("", fmt.Sprintf(`
			let me = this;
			Ext.create(%q, {
				listeners: { destroy: () => me.reload() },
			}).show();
		`, class))
	}
	return js.Func("", fmt.Sprintf(`
		let me = this;
		let selection = me.getView().getSelection();
		if (!selection || selection.length < 1) {
			return;
		}
		Ext.create(%q, {
			contentid: selection[0].data.%s,
			autoLoad: true,
			listeners: { destroy: () => me.reload() },
		}).show();
	`, class, idField))
}

// confirmRemove builds a controller handler that deletes every selected record
// after a confirmation prompt. idExpr is the JavaScript expression producing
// the record id, evaluated with rec in scope.
func confirmRemove(baseURL, idExpr string) js.Raw {
	return js.Func("", fmt.Sprintf(`
		const me = this;
		const view = me.getView();
		const recs = view.getSelection();
		if (!recs.length) {
			return;
		}
		Ext.Msg.confirm(
			gettext("Confirm"),
			gettext("Remove selected entries?"),
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
	`, baseURL, idExpr))
}

// pathKeyedURL builds the cbindData hook for dialogs whose record id is a
// filesystem path: POST to baseURL when creating, PUT to the encoded path
// otherwise.
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
