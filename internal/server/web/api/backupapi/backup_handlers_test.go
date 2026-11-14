//go:build linux

package backupapi

import (
	"net/url"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestParseExpandLimit(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		fallback int
		want     int
		wantErr  bool
	}{
		{name: "fallback", fallback: 8, want: 8},
		{name: "unlimited", raw: "-1", want: -1},
		{name: "large user limit", raw: "50000000", want: 50_000_000},
		{name: "below unlimited", raw: "-2", wantErr: true},
		{name: "invalid", raw: "many", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExpandLimit(tt.raw, "limit", tt.fallback)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseExpandLimit error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseExpandLimit = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseBulkBackupChanges(t *testing.T) {
	apply := func(form url.Values) (coredb.Backup, error) {
		b := coredb.Backup{Schedule: "daily", Comment: "old", Retry: 3, RetryInterval: 5, Store: "ds"}
		changes, err := parseBulkBackupChanges(form)
		if err != nil {
			return b, err
		}
		for _, c := range changes {
			c(&b)
		}
		return b, nil
	}

	got, err := apply(url.Values{"schedule": {"weekly"}, "retry": {"1"}, "delete": {"comment", "retry-interval"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Schedule != "weekly" || got.Retry != 1 || got.Comment != "" || got.RetryInterval != 1 || got.Store != "ds" {
		t.Fatalf("unexpected result %#v", got)
	}

	got, err = apply(url.Values{"stall-timeout": {"-1"}, "stall-min-progress": {"512"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.StallTimeout != coredb.StallTimeoutDisabled || got.StallMinProgress != 512 {
		t.Fatalf("stall rule = (%d, %d), want (-1, 512)", got.StallTimeout, got.StallMinProgress)
	}

	got, err = apply(url.Values{"delete": {"stall-timeout", "stall-min-progress"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.StallTimeout != coredb.DefaultStallTimeout || got.StallMinProgress != coredb.DefaultStallMinProgress {
		t.Fatalf("deleted stall rule = (%d, %d), want defaults", got.StallTimeout, got.StallMinProgress)
	}

	for name, form := range map[string]url.Values{
		"zero stall timeout":     {"stall-timeout": {"0"}},
		"below -1 stall timeout": {"stall-timeout": {"-2"}},
		"non-numeric stall":      {"stall-timeout": {"soon"}},
		"zero stall progress":    {"stall-min-progress": {"0"}},
		"no changes":             {},
		"unknown delete":         {"delete": {"target"}},
		"invalid schedule":       {"schedule": {"not a schedule"}},
		"negative retry":         {"retry": {"-1"}},
		"empty datastore":        {"delete": {"store"}},
		"bad retry interval":     {"retry-interval": {"0"}},
	} {
		if _, err := apply(form); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
