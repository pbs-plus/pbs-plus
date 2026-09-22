package targetplugin

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// MaxTCPPort bounds integer port fields to the TCP range.
func MaxTCPPort() *int64 {
	maximum := int64(65535)
	return &maximum
}
func ParseFormValues(schema FormSchema, submitted map[string][]string, secretPresent map[string]bool) (Values, Secrets, error) {
	if err := schema.Validate(); err != nil {
		return nil, nil, fmt.Errorf("validate form schema: %w", err)
	}
	fields := flattenFormFields(schema.Fields)
	byKey := make(map[string]FormField, len(fields))
	for _, field := range fields {
		byKey[field.Key] = field
	}
	for key := range submitted {
		field, ok := byKey[key]
		if !ok {
			return nil, nil, fmt.Errorf("unknown form field %q", key)
		}
		if field.Control == ControlGroup || field.Control == ControlStatus {
			return nil, nil, fmt.Errorf("form field %q is read-only", key)
		}
	}

	values := make(Values)
	secrets := make(Secrets)
	for _, field := range fields {
		if field.Control == ControlGroup || field.Control == ControlStatus || field.Control == ControlSecret {
			continue
		}
		raw, present, err := singleFormValue(submitted, field.Key)
		if err != nil {
			return nil, nil, err
		}
		if !present {
			if field.Default != nil {
				values[field.Key] = *field.Default
			}
			continue
		}
		scalar, err := parseFormScalar(field, raw)
		if err != nil {
			return nil, nil, fmt.Errorf("form field %q: %w", field.Key, err)
		}
		values[field.Key] = scalar
	}

	visible := make(map[string]bool, len(fields))
	markFormFieldVisibility(schema.Fields, values, true, visible)
	for _, field := range fields {
		if !visible[field.Key] {
			if _, ok := submitted[field.Key]; ok {
				return nil, nil, fmt.Errorf("form field %q is not visible", field.Key)
			}
			delete(values, field.Key)
			continue
		}
		if field.Control == ControlSecret {
			raw, present, err := singleFormValue(submitted, field.Key)
			if err != nil {
				return nil, nil, err
			}
			if present && raw != "" {
				if len(raw) > maxFormValueBytes {
					return nil, nil, fmt.Errorf("form field %q exceeds %d bytes", field.Key, maxFormValueBytes)
				}
				if field.Pattern != "" && !regexp.MustCompile(field.Pattern).MatchString(raw) {
					return nil, nil, fmt.Errorf("form field %q does not match its pattern", field.Key)
				}
				secrets[field.Key] = []byte(raw)
			}
			if field.Required && len(secrets[field.Key]) == 0 && !secretPresent[field.Key] {
				return nil, nil, fmt.Errorf("form field %q is required", field.Key)
			}
			continue
		}
		if field.Control == ControlGroup || field.Control == ControlStatus {
			continue
		}
		value, present := values[field.Key]
		if field.Required && (!present || scalarEmpty(value)) {
			return nil, nil, fmt.Errorf("form field %q is required", field.Key)
		}
	}
	return values, secrets, nil
}

// ValidateFormConfig checks typed config and secret presence against a descriptor schema.
func ValidateFormConfig(schema FormSchema, config Values, secretPresent map[string]bool) error {
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("validate form schema: %w", err)
	}
	fields := flattenFormFields(schema.Fields)
	byKey := make(map[string]FormField, len(fields))
	for _, field := range fields {
		byKey[field.Key] = field
	}
	for key, value := range config {
		field, ok := byKey[key]
		if !ok {
			return fmt.Errorf("unknown config field %q", key)
		}
		if field.Control == ControlGroup || field.Control == ControlStatus || field.Control == ControlSecret {
			return fmt.Errorf("config field %q does not accept a value", key)
		}
		if err := validateFormScalar(field, value); err != nil {
			return fmt.Errorf("config field %q: %w", key, err)
		}
	}
	for key, present := range secretPresent {
		if !present {
			continue
		}
		field, ok := byKey[key]
		if !ok || field.Control != ControlSecret {
			return fmt.Errorf("unknown secret field %q", key)
		}
	}
	visible := make(map[string]bool, len(fields))
	markFormFieldVisibility(schema.Fields, config, true, visible)
	for _, field := range fields {
		if !visible[field.Key] {
			if _, ok := config[field.Key]; ok || secretPresent[field.Key] {
				return fmt.Errorf("field %q is not visible", field.Key)
			}
			continue
		}
		if !field.Required {
			continue
		}
		if field.Control == ControlSecret {
			if !secretPresent[field.Key] {
				return fmt.Errorf("field %q is required", field.Key)
			}
			continue
		}
		value, ok := config[field.Key]
		if !ok || scalarEmpty(value) {
			return fmt.Errorf("field %q is required", field.Key)
		}
	}
	return nil
}

func validateFormScalar(field FormField, value Scalar) error {
	if err := validateFieldScalar(field, value); err != nil {
		return err
	}
	if field.Control == ControlInteger {
		integer, _ := value.IntegerValue()
		if field.Minimum != nil && integer < *field.Minimum || field.Maximum != nil && integer > *field.Maximum {
			return errors.New("value is outside its bounds")
		}
	}
	if field.Control == ControlSelect && !selectContains(field, value) {
		return errors.New("value is not a select option")
	}
	if field.Pattern != "" {
		text, _ := value.StringValue()
		if !regexp.MustCompile(field.Pattern).MatchString(text) {
			return errors.New("value does not match its pattern")
		}
	}
	return nil
}

func singleFormValue(submitted map[string][]string, key string) (string, bool, error) {
	values, ok := submitted[key]
	if !ok {
		return "", false, nil
	}
	if len(values) != 1 {
		return "", false, fmt.Errorf("form field %q must occur once", key)
	}
	return values[0], true, nil
}

func parseFormScalar(field FormField, raw string) (Scalar, error) {
	if len(raw) > maxFormValueBytes {
		return Scalar{}, fmt.Errorf("value exceeds %d bytes", maxFormValueBytes)
	}
	var scalar Scalar
	switch field.Control {
	case ControlInteger:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Scalar{}, errors.New("value is not an integer")
		}
		if field.Minimum != nil && value < *field.Minimum || field.Maximum != nil && value > *field.Maximum {
			return Scalar{}, errors.New("value is outside its bounds")
		}
		scalar = NewIntegerScalar(value)
	case ControlBoolean:
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return Scalar{}, errors.New("value is not a boolean")
		}
		scalar = NewBooleanScalar(value)
	case ControlSelect:
		var err error
		scalar, err = parseSelectScalar(field, raw)
		if err != nil {
			return Scalar{}, err
		}
	case ControlText, ControlPath, ControlCertificatePath:
		if field.Pattern != "" && !regexp.MustCompile(field.Pattern).MatchString(raw) {
			return Scalar{}, errors.New("value does not match its pattern")
		}
		scalar = NewStringScalar(raw)
	default:
		return Scalar{}, errors.New("field does not accept input")
	}
	return scalar, nil
}

func parseSelectScalar(field FormField, raw string) (Scalar, error) {
	var scalar Scalar
	switch field.Options[0].Value.Kind() {
	case ScalarString:
		scalar = NewStringScalar(raw)
	case ScalarInteger:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Scalar{}, errors.New("value is not a select option")
		}
		scalar = NewIntegerScalar(value)
	case ScalarBoolean:
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return Scalar{}, errors.New("value is not a select option")
		}
		scalar = NewBooleanScalar(value)
	default:
		return Scalar{}, errors.New("select has an unsupported value type")
	}
	if !selectContains(field, scalar) {
		return Scalar{}, errors.New("value is not a select option")
	}
	return scalar, nil
}

func markFormFieldVisibility(fields []FormField, values Values, parentVisible bool, visible map[string]bool) {
	for _, field := range fields {
		fieldVisible := parentVisible
		if fieldVisible && field.VisibleWhen != nil {
			value, ok := values[field.VisibleWhen.Field]
			fieldVisible = ok && value.Equal(field.VisibleWhen.Equals)
		}
		visible[field.Key] = fieldVisible
		markFormFieldVisibility(field.Fields, values, fieldVisible, visible)
	}
}

func scalarEmpty(value Scalar) bool {
	text, ok := value.StringValue()
	return ok && text == ""
}
