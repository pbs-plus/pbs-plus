package management

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var showFingerprint = js.Func("", `
	let win = Ext.create("Ext.window.Window", {
		modal: true,
		width: 600,
		title: gettext("Server CA Fingerprint"),
		layout: "form",
		bodyPadding: "10 0",
		items: [
			{
				xtype: "displayfield",
				value: gettext(
					"SHA-256 fingerprint of the server CA certificate. " +
					"Provide this to agents during bootstrap to pin the server identity " +
					"and prevent MITM attacks.",
				),
			},
			{
				xtype: "textfield",
				itemId: "fingerprint-field",
				value: gettext("Loading..."),
				editable: false,
				readOnly: true,
			},
		],
		buttons: [
			{
				xtype: "button",
				iconCls: "fa fa-clipboard",
				handler: async function () {
					let val = win.down("#fingerprint-field").getValue();
					if (val && val !== gettext("Loading...") && val !== gettext("Failed to load CA fingerprint")) {
						await navigator.clipboard.writeText(val);
					}
				},
				text: gettext("Copy"),
			},
			{ text: gettext("Ok"), handler: () => win.close() },
		],
	});
	win.show();
	fetch(pbsPlusBaseUrl + "/api2/json/plus/ca-fingerprint", { credentials: "include" })
		.then(function (resp) {
			if (!resp.ok) throw new Error(resp.statusText);
			return resp.text();
		})
		.then((fingerprint) => win.down("#fingerprint-field").setValue(fingerprint.trim()))
		.catch(() => win.down("#fingerprint-field").setValue(gettext("Failed to load CA fingerprint")));
`)

var tokenPanel = js.Panel{
	Name: "PBS.D2DManagement.TokenPanel", XType: "pbsDiskTokenPanel",
	Store:     js.Store{StoreID: "proxmox-agent-tokens", Model: "pbs-model-tokens", APIPath: "/api2/json/d2d/token", Sorters: "name"},
	Listeners: js.Listeners{ItemDblClick: "onCopy"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onAdd":             js.OpenEditWindow("PBS.D2DManagement.TokenEditWindow", ""),
		"onCopy":            js.CopySelectionWindow("Bootstrap Token", "token"),
		"onDeploy":          js.CopySelectionWindow("Deployment Scripts", "win_install"),
		"onShowFingerprint": showFingerprint,
		"revokeTokens":      js.ConfirmRemove("/api2/extjs/config/d2d-token/", "encodePathValue(rec.getId())", "Revoke selected tokens?"),
		"render_valid": js.Func("value", `
			let icon = value.toString() == "false" ? "check good" : "times critical";
			let text = value.toString() == "false" ? "Valid" : "Invalid";
			return '<i class="fa fa-' + icon + '"></i> ' + text;
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Generate Token", Handler: "onAdd", SelModel: new(false)}, js.Sep(),
		{Text: "Copy Token", Handler: "onCopy", Disabled: true},
		{Text: "Deploy with Token", Handler: "onDeploy", Disabled: true},
		{Text: "Show Fingerprint", Handler: "onShowFingerprint", SelModel: new(false)}, js.Sep(),
		{Text: "Revoke Token", Handler: "revokeTokens", Disabled: true, EnableFn: js.EnableOnSelection},
	},
	Columns: []js.Column{
		{Text: "Token", DataIndex: "token", Flex: 1},
		{Text: "Comment", DataIndex: "comment", Flex: 2},
		{Text: "Duration", DataIndex: "duration", Flex: 2},
		{Text: "Validity", DataIndex: "revoked", Flex: 3, RendererMethod: "render_valid"},
		{Text: "Created At", DataIndex: "created_at", Flex: 4, Renderer: "PBS.Utils.render_optional_timestamp"},
	},
}
