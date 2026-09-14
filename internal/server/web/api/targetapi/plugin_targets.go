//go:build linux

package targetapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type pluginTargetResponse struct {
	Name          string         `json:"name"`
	PluginID      string         `json:"plugin_id"`
	PluginVersion string         `json:"plugin_version"`
	TargetType    string         `json:"target_type"`
	SchemaVersion uint32         `json:"schema_version"`
	Config        map[string]any `json:"config"`
	SecretFields  []string       `json:"secret_fields"`
}

type pluginTargetTypeResponse struct {
	PluginID      string             `json:"plugin_id"`
	PluginVersion string             `json:"plugin_version"`
	TargetType    string             `json:"target_type"`
	Schema        pluginFormResponse `json:"schema"`
}

type pluginFormResponse struct {
	Version uint32                `json:"version"`
	Fields  []pluginFieldResponse `json:"fields"`
}

type pluginFieldResponse struct {
	Key         string                    `json:"key"`
	Label       string                    `json:"label"`
	Control     targetplugin.FieldControl `json:"control"`
	Required    bool                      `json:"required,omitempty"`
	Default     any                       `json:"default,omitempty"`
	Minimum     *int64                    `json:"minimum,omitempty"`
	Maximum     *int64                    `json:"maximum,omitempty"`
	Pattern     string                    `json:"pattern,omitempty"`
	Help        string                    `json:"help,omitempty"`
	Order       int32                     `json:"order,omitempty"`
	VisibleWhen *pluginVisibilityResponse `json:"visible_when,omitempty"`
	Options     []pluginOptionResponse    `json:"options,omitempty"`
	Fields      []pluginFieldResponse     `json:"fields,omitempty"`
}

type pluginVisibilityResponse struct {
	Field  string `json:"field"`
	Equals any    `json:"equals"`
}

type pluginOptionResponse struct {
	Label string `json:"label"`
	Value any    `json:"value"`
}

func ExtJsPluginTargetTypesHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respond.MethodNotAllowed(w, r)
			return
		}
		targetTypes, err := plugins.ListTargetTypes(r.Context(), app.CoreDB)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		data := make([]pluginTargetTypeResponse, len(targetTypes))
		for index, targetType := range targetTypes {
			data[index] = pluginTargetTypeResponse{
				PluginID:      targetType.PluginID,
				PluginVersion: targetType.PluginVersion,
				TargetType:    targetType.TargetType,
				Schema:        pluginFormSchemaResponse(targetType.Schema),
			}
		}
		writePluginTargetResponse(w, data)
	}
}

func ExtJsPluginTargetsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			pluginID := r.URL.Query().Get("plugin_id")
			if pluginID == "" {
				respond.WriteErrorResponse(w, errors.New("plugin_id is required"))
				return
			}
			targets, err := app.CoreDB.ListPluginTargets(r.Context(), pluginID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			data := make([]pluginTargetResponse, len(targets))
			for index, target := range targets {
				data[index], err = newPluginTargetResponse(target)
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
			}
			writePluginTargetResponse(w, data)
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			form, err := pluginTargetForm(r)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if err := plugins.CreateTarget(r.Context(), app.CoreDB, app.PluginSupervisor, r.FormValue("name"), r.FormValue("plugin_id"), r.FormValue("target_type"), form); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, nil)
		default:
			respond.MethodNotAllowed(w, r)
		}
	}
}

func ExtJsPluginTargetHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := validate.DecodePath(r.PathValue("target"))
		switch r.Method {
		case http.MethodGet:
			target, err := app.CoreDB.GetPluginTarget(r.Context(), name)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			data, err := newPluginTargetResponse(target)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, data)
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			form, err := pluginTargetForm(r)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			deleteFields, err := pluginDeleteFields(r.Form["delete"])
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if err := plugins.UpdateTarget(r.Context(), app.CoreDB, app.PluginSupervisor, name, form, deleteFields); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, nil)
		case http.MethodDelete:
			if err := app.Target.DeleteTarget(nil, name); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, nil)
		default:
			respond.MethodNotAllowed(w, r)
		}
	}
}

func pluginTargetForm(r *http.Request) (map[string][]string, error) {
	form := make(map[string][]string, len(r.Form))
	for key, values := range r.Form {
		switch key {
		case "name", "plugin_id", "target_type", "delete":
			continue
		}
		field, ok := strings.CutPrefix(key, "config.")
		if !ok || field == "" {
			return nil, fmt.Errorf("unknown request field %q", key)
		}
		form[field] = values
	}
	return form, nil
}

func pluginDeleteFields(values []string) ([]string, error) {
	fields := make([]string, len(values))
	for index, value := range values {
		field, ok := strings.CutPrefix(value, "config.")
		if !ok || field == "" {
			return nil, fmt.Errorf("invalid deleted field %q", value)
		}
		fields[index] = field
	}
	return fields, nil
}

func newPluginTargetResponse(target coredb.PluginTarget) (pluginTargetResponse, error) {
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(target.Config, &config); err != nil {
		return pluginTargetResponse{}, err
	}
	values := make(map[string]any, len(config))
	for key, value := range config {
		values[key] = pluginScalarValue(value)
	}
	return pluginTargetResponse{
		Name:          target.Name,
		PluginID:      target.PluginID,
		PluginVersion: target.PluginVersion,
		TargetType:    target.TargetType,
		SchemaVersion: target.SchemaVersion,
		Config:        values,
		SecretFields:  target.SecretFields,
	}, nil
}

func pluginFormSchemaResponse(schema targetplugin.FormSchema) pluginFormResponse {
	fields := make([]pluginFieldResponse, len(schema.Fields))
	for index, field := range schema.Fields {
		fields[index] = pluginFormFieldResponse(field)
	}
	return pluginFormResponse{Version: schema.Version, Fields: fields}
}

func pluginFormFieldResponse(field targetplugin.FormField) pluginFieldResponse {
	response := pluginFieldResponse{
		Key:      field.Key,
		Label:    field.Label,
		Control:  field.Control,
		Required: field.Required,
		Minimum:  field.Minimum,
		Maximum:  field.Maximum,
		Pattern:  field.Pattern,
		Help:     field.Help,
		Order:    field.Order,
	}
	if field.Default != nil {
		response.Default = pluginScalarValue(*field.Default)
	}
	if field.VisibleWhen != nil {
		response.VisibleWhen = &pluginVisibilityResponse{Field: field.VisibleWhen.Field, Equals: pluginScalarValue(field.VisibleWhen.Equals)}
	}
	response.Options = make([]pluginOptionResponse, len(field.Options))
	for index, option := range field.Options {
		response.Options[index] = pluginOptionResponse{Label: option.Label, Value: pluginScalarValue(option.Value)}
	}
	response.Fields = make([]pluginFieldResponse, len(field.Fields))
	for index, child := range field.Fields {
		response.Fields[index] = pluginFormFieldResponse(child)
	}
	return response
}

func pluginScalarValue(value targetplugin.Scalar) any {
	switch value.Kind() {
	case targetplugin.ScalarString:
		result, _ := value.StringValue()
		return result
	case targetplugin.ScalarInteger:
		result, _ := value.IntegerValue()
		return result
	case targetplugin.ScalarBoolean:
		result, _ := value.BooleanValue()
		return result
	default:
		return nil
	}
}

func writePluginTargetResponse(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data}); err != nil {
		log.Error(err, "encode plugin target response")
	}
}
