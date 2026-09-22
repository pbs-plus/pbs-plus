package targetplugin

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestScalarRoundTrip(t *testing.T) {
	type document struct {
		Value Scalar `toml:"value"`
	}

	tests := []struct {
		name  string
		value Scalar
	}{
		{name: "string", value: NewStringScalar("quoted \"value\"")},
		{name: "integer", value: NewIntegerScalar(-42)},
		{name: "boolean", value: NewBooleanScalar(true)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tomlData, err := toml.Marshal(document{Value: test.value})
			if err != nil {
				t.Fatalf("marshal TOML: %v", err)
			}
			var tomlValue document
			metadata, err := toml.Decode(string(tomlData), &tomlValue)
			if err != nil {
				t.Fatalf("decode TOML: %v", err)
			}
			if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
				t.Fatalf("undecoded TOML keys: %v", undecoded)
			}
			if !tomlValue.Value.Equal(test.value) {
				t.Fatalf("TOML value = %#v, want %#v", tomlValue.Value, test.value)
			}

			cborData, err := MarshalProtocol(test.value)
			if err != nil {
				t.Fatalf("marshal CBOR: %v", err)
			}
			var cborValue Scalar
			if err := UnmarshalProtocol(cborData, &cborValue); err != nil {
				t.Fatalf("decode CBOR: %v", err)
			}
			if !cborValue.Equal(test.value) {
				t.Fatalf("CBOR value = %#v, want %#v", cborValue, test.value)
			}
			roundTrip, err := MarshalProtocol(cborValue)
			if err != nil {
				t.Fatalf("remarshal CBOR: %v", err)
			}
			if !bytes.Equal(roundTrip, cborData) {
				t.Fatalf("CBOR round trip = %x, want %x", roundTrip, cborData)
			}
		})
	}
}

func TestFormSchemaRoundTrip(t *testing.T) {
	schema := validFormSchema()
	if err := schema.Validate(); err != nil {
		t.Fatalf("validate schema: %v", err)
	}

	tomlData, err := toml.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal TOML: %v", err)
	}
	var tomlSchema FormSchema
	metadata, err := toml.Decode(string(tomlData), &tomlSchema)
	if err != nil {
		t.Fatalf("decode TOML: %v", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		t.Fatalf("undecoded TOML keys: %v", undecoded)
	}
	if !reflect.DeepEqual(tomlSchema, schema) {
		t.Fatalf("TOML schema = %#v, want %#v", tomlSchema, schema)
	}

	cborData, err := MarshalProtocol(schema)
	if err != nil {
		t.Fatalf("marshal CBOR: %v", err)
	}
	var cborSchema FormSchema
	if err := UnmarshalProtocol(cborData, &cborSchema); err != nil {
		t.Fatalf("decode CBOR: %v", err)
	}
	if !reflect.DeepEqual(cborSchema, schema) {
		t.Fatalf("CBOR schema = %#v, want %#v", cborSchema, schema)
	}
}

func TestFormSchemaValidateAllowsEmptyFields(t *testing.T) {
	if err := (FormSchema{Version: 1}).Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestFormSchemaValidateRejectsInvalidSchema(t *testing.T) {
	minimum := int64(1)
	maximum := int64(10)

	tests := []struct {
		name      string
		schema    func() FormSchema
		wantError string
	}{
		{
			name: "missing version",
			schema: func() FormSchema {
				schema := validFormSchema()
				schema.Version = 0
				return schema
			},
			wantError: "version is required",
		},
		{
			name: "duplicate key",
			schema: func() FormSchema {
				return FormSchema{Version: 1, Fields: []FormField{
					{Key: "same", Label: "First", Control: ControlText},
					{Key: "same", Label: "Second", Control: ControlBoolean},
				}}
			},
			wantError: "duplicate field key",
		},
		{
			name: "too many fields",
			schema: func() FormSchema {
				fields := make([]FormField, maxFormFields+1)
				for index := range fields {
					fields[index] = FormField{Key: fmt.Sprintf("field_%d", index), Label: "Field", Control: ControlText}
				}
				return FormSchema{Version: 1, Fields: fields}
			},
			wantError: "more than 128 fields",
		},
		{
			name: "key too long",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: strings.Repeat("a", maxFormKeyBytes+1), Label: "Field", Control: ControlText})
			},
			wantError: "field key exceeds",
		},
		{
			name: "label too long",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: strings.Repeat("a", maxFormLabelBytes+1), Control: ControlText})
			},
			wantError: "field label exceeds",
		},
		{
			name: "help too long",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Help: strings.Repeat("a", maxFormHelpBytes+1)})
			},
			wantError: "field help exceeds",
		},
		{
			name: "pattern too long",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Pattern: strings.Repeat("a", maxFormPatternBytes+1)})
			},
			wantError: "field pattern exceeds",
		},
		{
			name: "scalar too long",
			schema: func() FormSchema {
				value := NewStringScalar(strings.Repeat("a", maxFormValueBytes+1))
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Default: &value})
			},
			wantError: "form scalar exceeds",
		},
		{
			name: "unsupported control",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: "custom"})
			},
			wantError: "unsupported field control",
		},
		{
			name: "default type mismatch",
			schema: func() FormSchema {
				value := NewStringScalar("one")
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlInteger, Default: &value})
			},
			wantError: "requires scalar type",
		},
		{
			name: "secret default",
			schema: func() FormSchema {
				value := NewStringScalar("secret")
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlSecret, Default: &value})
			},
			wantError: "secret field cannot declare a default",
		},
		{
			name: "mixed select types",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlSelect, Options: []SelectOption{
					{Label: "String", Value: NewStringScalar("one")},
					{Label: "Integer", Value: NewIntegerScalar(2)},
				}})
			},
			wantError: "requires scalar type",
		},
		{
			name: "duplicate select option",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlSelect, Options: []SelectOption{
					{Label: "One", Value: NewStringScalar("one")},
					{Label: "Again", Value: NewStringScalar("one")},
				}})
			},
			wantError: "duplicate select option value",
		},
		{
			name: "too many select options",
			schema: func() FormSchema {
				options := make([]SelectOption, maxFormOptions+1)
				for index := range options {
					options[index] = SelectOption{Label: "Option", Value: NewIntegerScalar(int64(index))}
				}
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlSelect, Options: options})
			},
			wantError: "more than 64 options",
		},
		{
			name: "select default not present",
			schema: func() FormSchema {
				value := NewStringScalar("two")
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlSelect, Default: &value, Options: []SelectOption{
					{Label: "One", Value: NewStringScalar("one")},
				}})
			},
			wantError: "default is not one of its options",
		},
		{
			name: "nested group",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "outer", Label: "Outer", Control: ControlGroup, Fields: []FormField{
					{Key: "inner", Label: "Inner", Control: ControlGroup, Fields: []FormField{
						{Key: "field", Label: "Field", Control: ControlText},
					}},
				}})
			},
			wantError: "groups cannot be nested",
		},
		{
			name: "bounds on text",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Minimum: &minimum})
			},
			wantError: "only an integer field",
		},
		{
			name: "reversed bounds",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlInteger, Minimum: &maximum, Maximum: &minimum})
			},
			wantError: "minimum exceeds maximum",
		},
		{
			name: "default outside bounds",
			schema: func() FormSchema {
				value := NewIntegerScalar(11)
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlInteger, Default: &value, Minimum: &minimum, Maximum: &maximum})
			},
			wantError: "default is outside its bounds",
		},
		{
			name: "invalid pattern",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Pattern: "["})
			},
			wantError: "invalid field pattern",
		},
		{
			name: "default pattern mismatch",
			schema: func() FormSchema {
				value := NewStringScalar("no")
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, Default: &value, Pattern: "^yes$"})
			},
			wantError: "default does not match",
		},
		{
			name: "unknown visibility field",
			schema: func() FormSchema {
				return schemaWithField(FormField{Key: "field", Label: "Field", Control: ControlText, VisibleWhen: &FieldVisibility{
					Field: "missing", Equals: NewBooleanScalar(true),
				}})
			},
			wantError: "references unknown field",
		},
		{
			name: "visibility type mismatch",
			schema: func() FormSchema {
				return FormSchema{Version: 1, Fields: []FormField{
					{Key: "enabled", Label: "Enabled", Control: ControlBoolean},
					{Key: "field", Label: "Field", Control: ControlText, VisibleWhen: &FieldVisibility{Field: "enabled", Equals: NewStringScalar("true")}},
				}}
			},
			wantError: "visibility value",
		},
		{
			name: "visibility select option absent",
			schema: func() FormSchema {
				return FormSchema{Version: 1, Fields: []FormField{
					{Key: "mode", Label: "Mode", Control: ControlSelect, Options: []SelectOption{{Label: "One", Value: NewStringScalar("one")}}},
					{Key: "field", Label: "Field", Control: ControlText, VisibleWhen: &FieldVisibility{Field: "mode", Equals: NewStringScalar("two")}},
				}}
			},
			wantError: "visibility value is not a select option",
		},
		{
			name: "visibility cycle",
			schema: func() FormSchema {
				return FormSchema{Version: 1, Fields: []FormField{
					{Key: "first", Label: "First", Control: ControlBoolean, VisibleWhen: &FieldVisibility{Field: "second", Equals: NewBooleanScalar(true)}},
					{Key: "second", Label: "Second", Control: ControlBoolean, VisibleWhen: &FieldVisibility{Field: "first", Equals: NewBooleanScalar(true)}},
				}}
			},
			wantError: "cyclic visibility",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.schema().Validate()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Validate() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func validFormSchema() FormSchema {
	minimum := int64(1)
	maximum := int64(65535)
	port := NewIntegerScalar(8007)
	tls := NewBooleanScalar(true)
	mode := NewStringScalar("direct")

	return FormSchema{Version: 1, Fields: []FormField{
		{Key: "endpoint", Label: "Endpoint", Control: ControlText, Required: true, Pattern: `^https://`, Help: "Server URL", Order: 10},
		{Key: "password", Label: "Password", Control: ControlSecret, Required: true, Order: 20},
		{Key: "port", Label: "Port", Control: ControlInteger, Default: &port, Minimum: &minimum, Maximum: &maximum, Order: 30},
		{Key: "tls", Label: "TLS", Control: ControlBoolean, Default: &tls, Order: 40},
		{Key: "mode", Label: "Mode", Control: ControlSelect, Default: &mode, Options: []SelectOption{
			{Label: "Direct", Value: NewStringScalar("direct")},
			{Label: "Proxy", Value: NewStringScalar("proxy")},
		}, Order: 50},
		{Key: "paths", Label: "Paths", Control: ControlGroup, Help: "Filesystem settings", Order: 60, VisibleWhen: &FieldVisibility{Field: "mode", Equals: NewStringScalar("direct")}, Fields: []FormField{
			{Key: "path", Label: "Path", Control: ControlPath, Pattern: `^/`, Order: 10},
			{Key: "certificate", Label: "Certificate", Control: ControlCertificatePath, Order: 20},
			{Key: "status", Label: "Status", Control: ControlStatus, Order: 30},
		}},
	}}
}

func schemaWithField(field FormField) FormSchema {
	return FormSchema{Version: 1, Fields: []FormField{field}}
}
