package targetplugin

import (
	"strings"
	"testing"
)

func TestParseFormValues(t *testing.T) {
	minimum := int64(1)
	maximum := int64(10)
	defaultMode := NewStringScalar("safe")
	detailsVisible := NewBooleanScalar(true)
	schema := FormSchema{Version: 1, Fields: []FormField{
		{Key: "path", Label: "Path", Control: ControlPath, Required: true, Pattern: `^/`},
		{Key: "retries", Label: "Retries", Control: ControlInteger, Minimum: &minimum, Maximum: &maximum},
		{Key: "enabled", Label: "Enabled", Control: ControlBoolean},
		{Key: "mode", Label: "Mode", Control: ControlSelect, Default: &defaultMode, Options: []SelectOption{
			{Label: "Safe", Value: NewStringScalar("safe")},
			{Label: "Fast", Value: NewStringScalar("fast")},
		}},
		{Key: "password", Label: "Password", Control: ControlSecret, Required: true},
		{Key: "show_details", Label: "Show details", Control: ControlBoolean},
		{Key: "details", Label: "Details", Control: ControlGroup, VisibleWhen: &FieldVisibility{Field: "show_details", Equals: detailsVisible}, Fields: []FormField{
			{Key: "note", Label: "Note", Control: ControlText, Required: true},
		}},
		{Key: "status", Label: "Status", Control: ControlStatus},
	}}

	values, secrets, err := ParseFormValues(schema, map[string][]string{
		"path":         {"/srv/data"},
		"retries":      {"3"},
		"enabled":      {"false"},
		"password":     {"secret"},
		"show_details": {"true"},
		"note":         {"mounted"},
	}, nil)
	if err != nil {
		t.Fatalf("ParseFormValues: %v", err)
	}
	if path, _ := values["path"].StringValue(); path != "/srv/data" {
		t.Fatalf("path = %q", path)
	}
	if retries, _ := values["retries"].IntegerValue(); retries != 3 {
		t.Fatalf("retries = %d", retries)
	}
	if enabled, ok := values["enabled"].BooleanValue(); !ok || enabled {
		t.Fatalf("enabled = %v, %v", enabled, ok)
	}
	if mode, _ := values["mode"].StringValue(); mode != "safe" {
		t.Fatalf("mode = %q", mode)
	}
	if string(secrets["password"]) != "secret" {
		t.Fatalf("secrets = %#v", secrets)
	}

	values, secrets, err = ParseFormValues(schema, map[string][]string{
		"path":         {"/srv/data"},
		"show_details": {"false"},
	}, map[string]bool{"password": true})
	if err != nil {
		t.Fatalf("ParseFormValues retained secret: %v", err)
	}
	if _, ok := values["note"]; ok || len(secrets) != 0 {
		t.Fatalf("hidden values = %#v, secrets = %#v", values, secrets)
	}
}

func TestParseFormValuesRejectsInvalidInput(t *testing.T) {
	minimum := int64(1)
	maximum := int64(10)
	visible := NewBooleanScalar(true)
	schema := FormSchema{Version: 1, Fields: []FormField{
		{Key: "count", Label: "Count", Control: ControlInteger, Required: true, Minimum: &minimum, Maximum: &maximum},
		{Key: "choice", Label: "Choice", Control: ControlSelect, Options: []SelectOption{{Label: "One", Value: NewIntegerScalar(1)}}},
		{Key: "password", Label: "Password", Control: ControlSecret, Required: true},
		{Key: "show", Label: "Show", Control: ControlBoolean},
		{Key: "hidden", Label: "Hidden", Control: ControlText, VisibleWhen: &FieldVisibility{Field: "show", Equals: visible}},
		{Key: "status", Label: "Status", Control: ControlStatus},
	}}

	tests := []struct {
		name      string
		form      map[string][]string
		wantError string
	}{
		{name: "unknown", form: map[string][]string{"extra": {"x"}}, wantError: "unknown form field"},
		{name: "repeated", form: map[string][]string{"count": {"1", "2"}}, wantError: "must occur once"},
		{name: "bad integer", form: map[string][]string{"count": {"x"}}, wantError: "not an integer"},
		{name: "integer bounds", form: map[string][]string{"count": {"11"}}, wantError: "outside its bounds"},
		{name: "bad select", form: map[string][]string{"count": {"1"}, "choice": {"2"}}, wantError: "not a select option"},
		{name: "missing required", form: map[string][]string{"choice": {"1"}}, wantError: `field "count" is required`},
		{name: "missing secret", form: map[string][]string{"count": {"1"}}, wantError: `field "password" is required`},
		{name: "hidden input", form: map[string][]string{"count": {"1"}, "password": {"x"}, "hidden": {"x"}}, wantError: "is not visible"},
		{name: "status input", form: map[string][]string{"status": {"ok"}}, wantError: "is read-only"},
		{name: "oversized", form: map[string][]string{"count": {"1"}, "password": {strings.Repeat("x", maxFormValueBytes+1)}}, wantError: "exceeds"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := ParseFormValues(schema, test.form, nil)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("ParseFormValues error = %v, want %q", err, test.wantError)
			}
		})
	}
}
