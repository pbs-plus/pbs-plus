//go:build linux

package web

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const formatRequestErrorHarness = `
const cases = %CASES%;

globalThis.gettext = (s) => s;
globalThis.PBS = {};
globalThis.Ext = {
	define: (name, obj) => {
		let parts = name.split(".");
		let target = globalThis;
		for (let p of parts.slice(0, -1)) {
			target[p] = target[p] ?? {};
			target = target[p];
		}
		target[parts[parts.length - 1]] = obj;
	},
	htmlEncode: (s) =>
		String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"),
	decode: (s) => JSON.parse(s),
	apply: Object.assign,
	applyIf: Object.assign,
	isFunction: (f) => typeof f === "function",
	isArray: Array.isArray,
	isObject: (o) => o !== null && typeof o === "object",
	Object: { each: () => {} },
	Array: { each: () => {} },
	Msg: { alert: () => {} },
	Ajax: { request: () => {} },
	callback: () => {},
	String: { htmlEncode: (s) => String(s) },
};
globalThis.Proxmox = {
	Utils: {
		// mirrors proxmox-widget-toolkit src/Utils.js extractRequestError
		extractRequestError: (result, verbose) => {
			if (result.success) {
				return "Successful";
			}
			let msg = "Unknown error";
			if (result.message) {
				msg = Ext.htmlEncode(result.message);
				if (result.status) {
					msg += " (" + result.status + ")";
				}
			}
			return msg;
		},
		setErrorMask: () => {},
	},
	RestProxy: class {},
	window: { Edit: class {} },
};

%SOURCE%

console.log(
	JSON.stringify(cases.map((c) => PBS.PlusUtils.formatRequestError(c))),
);
`

func TestFormatRequestErrorReadsJSONEnvelope(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}

	src, err := os.ReadFile(filepath.Join("views", "pre", "2_utils.js"))
	if err != nil {
		t.Fatalf("read 2_utils.js: %v", err)
	}

	cases := []map[string]any{
		{
			"status":       400,
			"statusText":   "",
			"responseText": `{"message":"samba outpost: valid users names a domain account","status":400,"success":false}`,
		},
		{"status": 0, "statusText": "", "responseText": ""},
		{"status": 500, "statusText": "", "responseText": "internal server error"},
		{"status": 0, "aborted": true},
		{"status": 0, "timedout": true},
		{"status": 502, "statusText": "", "responseText": "<html><body>bad gateway</body></html>"},
	}
	want := []string{
		"samba outpost: valid users names a domain account (400)",
		"Connection error - server offline?",
		"internal server error (500)",
		"Connection error - aborted.",
		"Connection error - Timeout.",
		"Error (502)",
	}

	encoded, err := json.Marshal(cases)
	if err != nil {
		t.Fatalf("marshal cases: %v", err)
	}

	script := strings.ReplaceAll(formatRequestErrorHarness, "%CASES%", string(encoded))
	script = strings.ReplaceAll(script, "%SOURCE%", string(src))

	cmd := exec.Command(node, "--input-type=module", "-e", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node failed: %v\n%s", err, out)
	}

	var got []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &got); err != nil {
		t.Fatalf("decode node output: %v\n%s", err, out)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("case %d: got %q, want %q", i, got[i], want[i])
		}
	}
}
