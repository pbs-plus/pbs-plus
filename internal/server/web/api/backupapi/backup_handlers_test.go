//go:build linux

package backupapi

import "testing"

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
