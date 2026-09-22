//go:build linux

package targetapi

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestPluginTargetFormUsesConfigNamespace(t *testing.T) {
	form := url.Values{
		"name":        {"archive"},
		"plugin_id":   {"example.storage"},
		"target_type": {"example"},
		"config.path": {"/srv/archive"},
		"config.flag": {"true"},
	}
	request := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := request.ParseForm(); err != nil {
		t.Fatal(err)
	}
	got, err := pluginTargetForm(request)
	if err != nil {
		t.Fatalf("pluginTargetForm: %v", err)
	}
	want := map[string][]string{"path": {"/srv/archive"}, "flag": {"true"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pluginTargetForm = %#v, want %#v", got, want)
	}

	request.Form.Set("unexpected", "value")
	if _, err := pluginTargetForm(request); err == nil {
		t.Fatal("pluginTargetForm accepted an unnamespaced field")
	}
	deleted, err := pluginDeleteFields([]string{"config.password"})
	if err != nil || !reflect.DeepEqual(deleted, []string{"password"}) {
		t.Fatalf("pluginDeleteFields = %#v, %v", deleted, err)
	}
	if _, err := pluginDeleteFields([]string{"password"}); err == nil {
		t.Fatal("pluginDeleteFields accepted an unnamespaced field")
	}
}

func TestPluginTargetTypeResponseIncludesJobSchemas(t *testing.T) {
	response := pluginTargetTypeResponse{
		PluginID:      "example.storage",
		PluginVersion: "1.0.0",
		TargetType:    "example",
		Schema:        pluginFormResponse{Version: 1},
		BackupSchema:  pluginFormResponse{Version: 2},
		RestoreSchema: pluginFormResponse{Version: 3},
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"backup_schema":{"version":2`, `"restore_schema":{"version":3`} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("response %s does not contain %s", encoded, field)
		}
	}
}

func TestPluginTargetResponseUsesPrimitiveValues(t *testing.T) {
	config, err := targetplugin.MarshalProtocol(targetplugin.Values{
		"path":    targetplugin.NewStringScalar("/srv/archive"),
		"retries": targetplugin.NewIntegerScalar(3),
		"enabled": targetplugin.NewBooleanScalar(false),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := newPluginTargetResponse(coredb.PluginTarget{
		Name:          "archive",
		PluginID:      "example.storage",
		PluginVersion: "1.0.0",
		TargetType:    "example",
		SchemaVersion: 1,
		Config:        config,
		SecretFields:  []string{"password"},
	})
	if err != nil {
		t.Fatalf("newPluginTargetResponse: %v", err)
	}
	if response["config.path"] != "/srv/archive" || response["config.retries"] != int64(3) || response["config.enabled"] != false {
		t.Fatalf("response = %#v", response)
	}
}
