package calendarevent

import (
	"testing"
	"time"
)

func TestParseNamedSchedules(t *testing.T) {
	cases := []struct {
		input string
	}{
		{"minutely"},
		{"hourly"},
		{"daily"},
		{"weekly"},
		{"monthly"},
		{"yearly"},
		{"annually"},
		{"quarterly"},
		{"semiannually"},
		{"semi-annually"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			ev, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			if ev == nil {
				t.Fatalf("Parse(%q) returned nil", tc.input)
			}
		})
	}
}

func TestParseDocExamples(t *testing.T) {
	cases := []string{
		"mon,tue,wed,thu,fri",
		"mon..fri",
		"sat,sun",
		"sat..sun",
		"mon,wed,fri",
		"12:05",
		"*:00/5",
		"mon..wed *:30/10",
		"mon..fri 8..17,22:0/15",
		"fri 12..13:5/20",
		"12/2:5",
		"*:*",
		"*-05",
		"sat *-1..7 15:00",
		"2015-10-21",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			ev, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", input, err)
			}
			if ev == nil {
				t.Fatalf("Parse(%q) returned nil", input)
			}
		})
	}
}

func TestComputeNextEvent(t *testing.T) {
	base := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)

	cases := []struct {
		input    string
		expected string
	}{
		{"daily", "2025-06-16 00:00:00"},
		{"hourly", "2025-06-15 11:00:00"},
		{"minutely", "2025-06-15 10:31:00"},
		{"12:05", "2025-06-15 12:05:00"},
		{"*:00/5", "2025-06-15 10:35:00"},
		{"weekly", "2025-06-16 00:00:00"},
		{"monthly", "2025-07-01 00:00:00"},
		{"quarterly", "2025-07-01 00:00:00"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			ev, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			next, err := ComputeNextEvent(ev, base, time.UTC)
			if err != nil {
				t.Fatalf("ComputeNextEvent error: %v", err)
			}
			got := next.Format("2006-01-02 15:04:05")
			if got != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestEveryFiveMinutes(t *testing.T) {
	ev, err := Parse("*:00/5")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	origin := base

	for i := range 12 {
		next, err := ComputeNextEvent(ev, base, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		expected := origin.Add(time.Duration(i+1) * 5 * time.Minute)
		got := next.Format("2006-01-02 15:04:05")
		want := expected.Format("2006-01-02 15:04:05")
		if got != want {
			t.Errorf("iteration %d: expected %s, got %s", i, want, got)
		}
		base = next
	}
}

func TestFirstSaturdayOfMonth(t *testing.T) {
	ev, err := Parse("sat *-1..7 15:00")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 6 {
		next, err := ComputeNextEvent(ev, base, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		if next.Weekday() != time.Saturday {
			t.Errorf("iteration %d: expected Saturday, got %s", i, next.Weekday())
		}
		if next.Day() > 7 {
			t.Errorf("iteration %d: expected day <= 7, got %d", i, next.Day())
		}
		base = next
	}
}

func TestWeekdayRange(t *testing.T) {
	ev, err := Parse("mon..fri 08:00")
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.Weekdays) != 5 {
		t.Fatalf("expected 5 weekdays, got %d", len(ev.Weekdays))
	}
}

func TestRepetition(t *testing.T) {
	ev, err := Parse("12/2:5")
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.Hours) != 1 || ev.Hours[0].Value != 12 || ev.Hours[0].Repeat != 2 {
		t.Fatalf("unexpected hours: %+v", ev.Hours)
	}
}

func TestSpecificDate(t *testing.T) {
	ev, err := Parse("2025-10-21")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := ComputeNextEvent(ev, base, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	expected := "2025-10-21 00:00:00"
	got := next.Format("2006-01-02 15:04:05")
	if got != expected {
		t.Errorf("expected %s, got %s", expected, got)
	}
}
