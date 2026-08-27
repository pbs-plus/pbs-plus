package js

import (
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	source := string(Render(Define("PBS.Example", Obj{
		"extend": "Ext.grid.Panel",
		"items": Arr{
			Obj{"xtype": "panel"},
		},
		"initComponent": Func("", `
			let me = this;
			me.callParent();
		`),
	})))

	for _, want := range []string{
		`Ext.define("PBS.Example", {`,
		`extend: "Ext.grid.Panel",`,
		"initComponent: function () {",
		"let me = this;",
		"me.callParent();",
		"items: [",
		`xtype: "panel",`,
		"});",
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered source does not contain %q:\n%s", want, source)
		}
	}
}

func TestRenderQuotesUnsafeKeysAndStrings(t *testing.T) {
	source := string(Render(Define("PBS.Example", Obj{
		"api-path": "a\"b\n",
	})))
	if !strings.Contains(source, `"api-path": "a\"b\n",`) {
		t.Fatalf("unsafe key or string was not encoded as JavaScript: %s", source)
	}
}

func TestBool(t *testing.T) {
	config := Field{XType: XCheckbox, AllowBlank: Bool(false)}.Config()
	if got, ok := config["allowBlank"].(bool); !ok || got {
		t.Fatalf("explicit false was omitted or changed: %#v", config)
	}
}
