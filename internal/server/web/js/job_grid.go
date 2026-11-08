package js

import (
	"fmt"
	"maps"
	"strings"
)

type JobStatus string

const (
	JobStatusAll     JobStatus = "all"
	JobStatusRunning JobStatus = "running"
	JobStatusFailed  JobStatus = "failed"
	JobStatusWarning JobStatus = "warning"
	JobStatusNever   JobStatus = "never"
	JobStatusOverdue JobStatus = "overdue"
)

var jobStatusLabels = map[JobStatus]string{
	JobStatusAll:     "All",
	JobStatusRunning: "Running / Queued",
	JobStatusFailed:  "Failed",
	JobStatusWarning: "Warnings",
	JobStatusNever:   "Never run",
	JobStatusOverdue: "Overdue / Stale",
}

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
				const status = PBS.PlusUtils.jobStatus(rec.data);
				counts[status] = (counts[status] || 0) + 1;
				if (PBS.PlusUtils.jobNeedsAttention(rec.data)) {
					counts.overdue = (counts.overdue || 0) + 1;
				}
			});
			btn.getMenu().items.each((item) => {
				item.setText(item.statusLabel + " (" + (counts[item.statusValue] || 0) + ")");
			});
		`),
	}
}

func StatusFilterTool(statuses ...JobStatus) Tool {
	items := make(Arr, 0, len(statuses)+1)
	for _, s := range append([]JobStatus{JobStatusAll}, statuses...) {
		items = append(items, Obj{
			"xtype":        "menucheckitem",
			"text":         T(jobStatusLabels[s]),
			"statusLabel":  T(jobStatusLabels[s]),
			"statusValue":  string(s),
			"group":        "jobStatus",
			"checked":      s == JobStatusAll,
			"checkHandler": "onStatusFilter",
		})
	}
	return Tool{XType: XButton, Reference: "statusFilter", IconCls: "fa fa-filter", Text: "Status: All", Menu: items}
}
