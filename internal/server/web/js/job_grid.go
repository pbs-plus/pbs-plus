package js

import (
	"fmt"
	"maps"
	"strings"
)

func MergeMethods(sets ...map[string]Raw) map[string]Raw {
	out := map[string]Raw{}
	for _, s := range sets {
		maps.Copy(out, s)
	}
	return out
}

func SearchFilter(fields ...string) Raw {
	tests := make([]string, len(fields))
	for i, f := range fields {
		tests[i] = fmt.Sprintf(`re.test(rec.get(%q))`, f)
	}
	return Func("", fmt.Sprintf(`
		const me = this;
		const store = me.getView().getStore();
		const search = me.lookup("searchField");
		const val = search ? (search.getValue() || "").trim() : "";
		const re = val ? new RegExp(Ext.String.escapeRegex(val), "i") : null;
		const statusBtn = me.lookup("statusFilter");
		const status = (statusBtn && statusBtn.statusValue) || "all";
		if (!re && status === "all") {
			store.clearFilter();
			return;
		}
		store.clearFilter(true);
		store.filterBy((rec) => (!re || %s) && PBS.PlusUtils.jobMatchesStatus(rec.data, status));
	`, strings.Join(tests, " || ")))
}

func JobFilterMethods(fields ...string) map[string]Raw {
	return map[string]Raw{
		"onSearchKeyUp": SearchFilter(fields...),
		"initJobFilters": Func("view", `
			const me = this;
			const store = view.getStore();
			view.mon(store.rstore || store, "load", () => me.updateStatusCounts());
			me.updateStatusCounts();
		`),
		"onStatusFilter": Func("item, checked", `
			if (!checked) {
				return;
			}
			const btn = this.lookup("statusFilter");
			btn.statusValue = item.statusValue;
			btn.setText(gettext("Status") + ": " + item.statusLabel);
			this.onSearchKeyUp();
		`),
		"updateStatusCounts": Func("", `
			const btn = this.lookup("statusFilter");
			if (!btn) {
				return;
			}
			const store = this.getView().getStore();
			const records = (store.rstore || store).getRange();
			const counts = { all: records.length };
			records.forEach((rec) => {
				counts[rec.data.job_status] = (counts[rec.data.job_status] || 0) + 1;
				if (rec.data.needs_attention) {
					counts.overdue = (counts.overdue || 0) + 1;
				}
			});
			btn.getMenu().items.each((item) => {
				item.setText(item.statusLabel + " (" + (counts[item.statusValue] || 0) + ")");
			});
		`),
	}
}

func StatusFilterTool(options ...Option) Tool {
	items := make(Arr, len(options))
	for i, o := range options {
		items[i] = Obj{
			"xtype":        "menucheckitem",
			"text":         T(o.Text),
			"statusLabel":  T(o.Text),
			"statusValue":  o.Value,
			"group":        "jobStatus",
			"checked":      i == 0,
			"checkHandler": "onStatusFilter",
		}
	}
	return Tool{XType: XButton, Reference: "statusFilter", IconCls: "fa fa-filter", Text: "Status: " + options[0].Text, Menu: items}
}

// ContextMenu builds an itemcontextmenu handler that mirrors toolbar tools, reusing their handlers and enable rules.
func ContextMenu(tools ...Tool) Raw {
	items := make([]string, 0, len(tools))
	for _, t := range tools {
		if t.separator != "" {
			items = append(items, `"-"`)
			continue
		}
		disabled := "false"
		if t.EnableFn != "" {
			disabled = fmt.Sprintf("!(%s).call(scope, record)", t.EnableFn)
		}
		icon := ""
		if t.IconCls != "" {
			icon = fmt.Sprintf(", iconCls: %q", t.IconCls)
		}
		items = append(items, fmt.Sprintf(`{ text: gettext(%q)%s, disabled: %s, handler: () => me[%q]() }`, t.Text, icon, disabled, t.Handler))
	}
	return Func("view, record, item, index, e", fmt.Sprintf(`
		e.stopEvent();
		const me = this;
		const grid = me.getView();
		const sm = grid.getSelectionModel();
		if (!sm.isSelected(record)) {
			sm.select(record);
		}
		const scope = { up: () => grid };
		Ext.create("Ext.menu.Menu", {
			items: [%s],
			listeners: { hide: (menu) => Ext.defer(() => menu.destroy(), 1) },
		}).showAt(e.getXY());
	`, strings.Join(items, ", ")))
}
