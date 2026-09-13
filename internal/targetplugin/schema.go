package targetplugin

import (
	"errors"
	"fmt"
	"regexp"
)

const (
	maxFormFields       = 128
	maxFormOptions      = 64
	maxFormKeyBytes     = 64
	maxFormLabelBytes   = 255
	maxFormHelpBytes    = 2048
	maxFormPatternBytes = 512
	maxFormValueBytes   = 4096
	maxFormGroupDepth   = 1
)

// FieldControl identifies a host-rendered form control.
type FieldControl string

const (
	ControlText            FieldControl = "text"
	ControlSecret          FieldControl = "secret"
	ControlInteger         FieldControl = "integer"
	ControlBoolean         FieldControl = "boolean"
	ControlSelect          FieldControl = "select"
	ControlPath            FieldControl = "path"
	ControlCertificatePath FieldControl = "certificate_path"
	ControlStatus          FieldControl = "status"
	ControlGroup           FieldControl = "group"
)

// FormSchema describes one versioned host-rendered form.
type FormSchema struct {
	Version uint32      `toml:"version" cbor:"version"`
	Fields  []FormField `toml:"field" cbor:"fields"`
}

// FormField describes one bounded form value or group.
type FormField struct {
	Key         string           `toml:"key" cbor:"key"`
	Label       string           `toml:"label" cbor:"label"`
	Control     FieldControl     `toml:"control" cbor:"control"`
	Required    bool             `toml:"required,omitempty" cbor:"required,omitempty"`
	Default     *Scalar          `toml:"default,omitempty" cbor:"default,omitempty"`
	Minimum     *int64           `toml:"minimum,omitempty" cbor:"minimum,omitempty"`
	Maximum     *int64           `toml:"maximum,omitempty" cbor:"maximum,omitempty"`
	Pattern     string           `toml:"pattern,omitempty" cbor:"pattern,omitempty"`
	Help        string           `toml:"help,omitempty" cbor:"help,omitempty"`
	Order       int32            `toml:"order,omitempty" cbor:"order,omitempty"`
	VisibleWhen *FieldVisibility `toml:"visible_when,omitempty" cbor:"visible_when,omitempty"`
	Options     []SelectOption   `toml:"option,omitempty" cbor:"options,omitempty"`
	Fields      []FormField      `toml:"field,omitempty" cbor:"fields,omitempty"`
}

// FieldVisibility shows a field when another scalar has an equal value.
type FieldVisibility struct {
	Field  string `toml:"field" cbor:"field"`
	Equals Scalar `toml:"equals" cbor:"equals"`
}

// SelectOption is one typed fixed-select choice.
type SelectOption struct {
	Label string `toml:"label" cbor:"label"`
	Value Scalar `toml:"value" cbor:"value"`
}

// Validate checks form structure, value types, constraints, and references.
func (schema FormSchema) Validate() error {
	if schema.Version == 0 {
		return errors.New("form schema version is required")
	}
	fields := make(map[string]FormField)
	count := 0
	for index := range schema.Fields {
		if err := validateFormField(schema.Fields[index], 0, fields, &count); err != nil {
			return fmt.Errorf("field %d: %w", index, err)
		}
	}
	if err := validateVisibility(schema.Fields, fields); err != nil {
		return err
	}
	return validateVisibilityCycles(schema.Fields)
}

func validateFormField(field FormField, depth int, fields map[string]FormField, count *int) error {
	*count++
	if *count > maxFormFields {
		return fmt.Errorf("form contains more than %d fields", maxFormFields)
	}
	if err := validateIdentifier("field key", field.Key, maxFormKeyBytes); err != nil {
		return err
	}
	if _, ok := fields[field.Key]; ok {
		return fmt.Errorf("duplicate field key %q", field.Key)
	}
	fields[field.Key] = field
	if err := validateText("field label", field.Label, maxFormLabelBytes); err != nil {
		return err
	}
	if field.Help != "" {
		if err := validateText("field help", field.Help, maxFormHelpBytes); err != nil {
			return err
		}
	}
	if len(field.Pattern) > maxFormPatternBytes {
		return fmt.Errorf("field pattern exceeds %d bytes", maxFormPatternBytes)
	}

	switch field.Control {
	case ControlText, ControlSecret, ControlInteger, ControlBoolean, ControlSelect, ControlPath, ControlCertificatePath, ControlStatus:
		if len(field.Fields) != 0 {
			return errors.New("only a group may contain fields")
		}
	case ControlGroup:
		if depth > maxFormGroupDepth-1 {
			return fmt.Errorf("form groups cannot be nested more than %d level", maxFormGroupDepth)
		}
		if len(field.Fields) == 0 {
			return errors.New("field group must contain at least one field")
		}
		if field.Required || field.Default != nil || field.Minimum != nil || field.Maximum != nil || field.Pattern != "" || len(field.Options) != 0 {
			return errors.New("field group contains a value constraint")
		}
		for index := range field.Fields {
			if err := validateFormField(field.Fields[index], depth+1, fields, count); err != nil {
				return fmt.Errorf("group field %d: %w", index, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported field control %q", field.Control)
	}

	if field.Control != ControlInteger && (field.Minimum != nil || field.Maximum != nil) {
		return errors.New("only an integer field may declare minimum or maximum")
	}
	if field.Minimum != nil && field.Maximum != nil && *field.Minimum > *field.Maximum {
		return errors.New("field minimum exceeds maximum")
	}
	if field.Control != ControlText && field.Control != ControlSecret && field.Control != ControlPath && field.Control != ControlCertificatePath && field.Pattern != "" {
		return errors.New("field control does not support a pattern")
	}
	var pattern *regexp.Regexp
	if field.Pattern != "" {
		compiled, err := regexp.Compile(field.Pattern)
		if err != nil {
			return fmt.Errorf("invalid field pattern: %w", err)
		}
		pattern = compiled
	}
	if field.Control == ControlSecret && field.Default != nil {
		return errors.New("secret field cannot declare a default")
	}
	if field.Control == ControlStatus && (field.Required || field.Default != nil || field.Pattern != "" || len(field.Options) != 0) {
		return errors.New("status field contains an input constraint")
	}
	if field.Control == ControlSelect {
		if err := validateSelectOptions(field); err != nil {
			return err
		}
	} else if len(field.Options) != 0 {
		return errors.New("only a select field may declare options")
	}
	if field.Default != nil {
		if err := validateFieldScalar(field, *field.Default); err != nil {
			return fmt.Errorf("invalid field default: %w", err)
		}
		if field.Control == ControlInteger {
			value, _ := field.Default.IntegerValue()
			if field.Minimum != nil && value < *field.Minimum || field.Maximum != nil && value > *field.Maximum {
				return errors.New("field default is outside its bounds")
			}
		}
		if pattern != nil {
			value, _ := field.Default.StringValue()
			if !pattern.MatchString(value) {
				return errors.New("field default does not match its pattern")
			}
		}
	}
	return nil
}

func validateSelectOptions(field FormField) error {
	if len(field.Options) == 0 {
		return errors.New("select field must contain at least one option")
	}
	if len(field.Options) > maxFormOptions {
		return fmt.Errorf("select field contains more than %d options", maxFormOptions)
	}
	kind := field.Options[0].Value.Kind()
	if kind == ScalarUnset {
		return errors.New("select option value is unset")
	}
	for index := range field.Options {
		option := field.Options[index]
		if err := validateText("select option label", option.Label, maxFormLabelBytes); err != nil {
			return fmt.Errorf("option %d: %w", index, err)
		}
		if err := validateFieldScalar(field, option.Value); err != nil {
			return fmt.Errorf("option %d: %w", index, err)
		}
		for previous := range index {
			if option.Value.Equal(field.Options[previous].Value) {
				return errors.New("duplicate select option value")
			}
		}
	}
	if field.Default != nil && !selectContains(field, *field.Default) {
		return errors.New("select default is not one of its options")
	}
	return nil
}

func validateFieldScalar(field FormField, scalar Scalar) error {
	if err := scalar.validate(); err != nil {
		return err
	}
	var expected ScalarKind
	switch field.Control {
	case ControlInteger:
		expected = ScalarInteger
	case ControlBoolean:
		expected = ScalarBoolean
	case ControlSelect:
		expected = field.Options[0].Value.Kind()
	case ControlText, ControlSecret, ControlPath, ControlCertificatePath, ControlStatus:
		expected = ScalarString
	default:
		return errors.New("field does not accept a scalar")
	}
	if scalar.Kind() != expected {
		return fmt.Errorf("field requires scalar type %d, got %d", expected, scalar.Kind())
	}
	return nil
}

func selectContains(field FormField, value Scalar) bool {
	for _, option := range field.Options {
		if value.Equal(option.Value) {
			return true
		}
	}
	return false
}

func validateVisibility(formFields []FormField, fields map[string]FormField) error {
	for _, field := range flattenFormFields(formFields) {
		if field.VisibleWhen == nil {
			continue
		}
		if field.VisibleWhen.Field == field.Key {
			return fmt.Errorf("field %q visibility references itself", field.Key)
		}
		reference, ok := fields[field.VisibleWhen.Field]
		if !ok {
			return fmt.Errorf("field %q visibility references unknown field %q", field.Key, field.VisibleWhen.Field)
		}
		if reference.Control == ControlGroup {
			return fmt.Errorf("field %q visibility references a group", field.Key)
		}
		if err := validateFieldScalar(reference, field.VisibleWhen.Equals); err != nil {
			return fmt.Errorf("field %q visibility value: %w", field.Key, err)
		}
		if reference.Control == ControlSelect && !selectContains(reference, field.VisibleWhen.Equals) {
			return fmt.Errorf("field %q visibility value is not a select option", field.Key)
		}
	}
	return nil
}

func validateVisibilityCycles(formFields []FormField) error {
	fields := flattenFormFields(formFields)
	references := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.VisibleWhen != nil {
			references[field.Key] = field.VisibleWhen.Field
		}
	}
	for key := range references {
		seen := make(map[string]struct{})
		for current := key; current != ""; current = references[current] {
			if _, ok := seen[current]; ok {
				return fmt.Errorf("field %q has cyclic visibility", key)
			}
			seen[current] = struct{}{}
		}
	}
	return nil
}

func flattenFormFields(fields []FormField) []FormField {
	flattened := make([]FormField, 0, len(fields))
	for _, field := range fields {
		flattened = append(flattened, field)
		flattened = append(flattened, flattenFormFields(field.Fields)...)
	}
	return flattened
}
