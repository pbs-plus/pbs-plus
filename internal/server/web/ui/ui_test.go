package ui

import (
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	source := string(Render())

	for _, want := range []string{
		`Ext.define("pbs-model-scripts", {`,
		`Ext.define("PBS.form.D2DScriptSelector", {`,
		`Ext.define("PBS.D2DManagement.ScriptEditWindow", {`,
		`Ext.define("PBS.D2DManagement.ScriptPanel", {`,
		`url: pbsPlusBaseUrl + "/api2/json/d2d/script",`,
		`storeid: "proxmox-disk-scripts",`,
		`beforedestroy: "stopStore",`,
		`deactivate: "stopStore",`,
		`activate: "startStore",`,
		`Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);`,
		`PBS.PlusUtils.LoadCodeMirror(function () {`,
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered UI does not contain %q:\n%s", want, source)
		}
	}
}
