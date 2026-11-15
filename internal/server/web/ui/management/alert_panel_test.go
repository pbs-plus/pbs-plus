package management

import (
	"os/exec"
	"testing"
)

func TestAlertDeferredLoadLifecycle(t *testing.T) {
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
				const view = {};
				const controller = { loadExclusions: () => loads++ };
				init.call(controller, view);
				if (state !== "open") {
					view[state] = true;
					delete controller.loadExclusions;
				}
				pending();
				assert.equal(loads, state === "open" ? 1 : 0);
			`
			output, err := exec.Command(node, "-e", script, string(alertEditWindow.Controller.Methods["init"]), state).CombinedOutput()
			if err != nil {
				t.Fatalf("deferred alert load failed: %v\n%s", err, output)
			}
		})
	}
}
