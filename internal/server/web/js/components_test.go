package js

import "testing"

func TestResponsiveComponents(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		component Component
	}{
		{name: "grid", component: Panel{Name: "PBS.TestGrid", Columns: []Column{{Text: "Name", Flex: 1}}}},
		{name: "tabs", component: Panel{Name: "PBS.TestTabs", Extend: ExtTabPanel}},
		{name: "inline input panel", component: Panel{Extend: ExtInputPanel}},
		{name: "file browser", component: Panel{Name: "PBS.TestBrowser", Extend: "Proxmox.window.FileBrowser"}},
		{name: "selector", component: Selector{Name: "PBS.TestSelector"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugins, ok := tc.component.Config()["plugins"].(Arr)
			if !ok || len(plugins) != 1 || plugins[0] != "pbsPlusResponsive" {
				t.Fatalf("responsive plugin missing: %#v", tc.component.Config())
			}
		})
	}
}
