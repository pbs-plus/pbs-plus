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

	for _, want := range []string{
		`edit: function (view, rowIdx, colIdx, item, e, rec)`,
		`remove: function (view, rowIdx, colIdx, item, e, rec)`,
		`proxy: { type: "pbsplus", url: pbsPlusBaseUrl + "/api2/extjs/config/d2d-outposts" },`,
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered UI does not contain %q", want)
		}
	}
	if strings.Contains(source, `function (table, rec, el, rowIdx, colIdx, item, e, rec)`) {
		t.Error("rendered UI still contains the duplicated-rec outpost remove signature")
	}
	if n := strings.Count(source, `proxy: { type: "proxmox", url: "/api2/extjs/config/d2d-outposts" }`); n != 0 {
		t.Errorf("%d outpost combobox store(s) still target the PBS origin instead of pbsPlusBaseUrl", n)
	}
}
