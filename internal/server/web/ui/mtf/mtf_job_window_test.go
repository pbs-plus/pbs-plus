package mtf

import (
	"os/exec"
	"testing"
)

func TestMigrationDeferredLoadLifecycle(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	for _, state := range []string{"open", "destroying", "destroyed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			script := `
				const assert = require("node:assert/strict");
				const init = eval("(" + process.argv[1] + ")");
				const state = process.argv[2];
				let pending, loads = 0;
				global.Ext = { defer: callback => { pending = callback; } };
				const view = {
					down: () => ({ getForm: () => ({}) }),
					getController: () => ({ defaultDatastore: () => loads++ }),
				};
				init(view);
				if (state !== "open") {
					view[state] = true;
					view.down = () => { throw Error("queried a closed window"); };
				}
				pending();
				assert.equal(loads, state === "open" ? 1 : 0);
			`
			output, err := exec.Command(node, "-e", script, string(mtfJobEdit.Controller.Methods["onAfterRender"]), state).CombinedOutput()
			if err != nil {
				t.Fatalf("deferred migration load failed: %v\n%s", err, output)
			}
		})
	}
}
