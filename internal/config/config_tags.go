//go:build linux

package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ConfigTag represents the parsed configuration tags from struct fields
type ConfigTag struct {
	Type      PropertyType
	Required  bool
	MinLength *int
	MaxLength *int
	IsID      bool // New field to mark which field is the section ID
}

// parseConfigTags parses struct field tags to extract configuration metadata
func parseConfigTags(tag string) (ConfigTag, error) {
	result := ConfigTag{}

	if tag == "" {
		return result, nil
	}

	tags := strings.Split(tag, ",")
	for _, t := range tags {
		parts := strings.SplitN(t, "=", 2)
		key := parts[0]

		switch key {
		case "type":
			if len(parts) != 2 {
				return result, fmt.Errorf("type tag requires a value")
			}
			result.Type = PropertyType(parts[1])
		case "required":
			result.Required = true
		case "min":
			if len(parts) != 2 {
				return result, fmt.Errorf("min tag requires a value")
			}
			val, err := strconv.Atoi(parts[1])
			if err != nil {
				return result, fmt.Errorf("invalid min value: %w", err)
			}
			result.MinLength = &val
		case "max":
			if len(parts) != 2 {
				return result, fmt.Errorf("max tag requires a value")
			}
			val, err := strconv.Atoi(parts[1])
			if err != nil {
				return result, fmt.Errorf("invalid max value: %w", err)
			}
			result.MaxLength = &val
		case "id":
			result.IsID = true
		}
	}

	return result, nil
}

// validateFieldWithTags validates a field value against its configuration tags
func validateFieldWithTags(value interface{}, tags ConfigTag) error {
	switch tags.Type {
	case TypeString:
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string value")
		}

		if tags.Required && str == "" {
			return fmt.Errorf("required field is empty")
		}

		if tags.MinLength != nil && len(str) < *tags.MinLength {
			return fmt.Errorf("value length %d is less than minimum %d", len(str), *tags.MinLength)
		}

		if tags.MaxLength != nil && len(str) > *tags.MaxLength {
			return fmt.Errorf("value length %d is greater than maximum %d", len(str), *tags.MaxLength)
		}

	case TypeInt:
		num, ok := value.(int)
		if !ok {
			return fmt.Errorf("expected integer value")
		}

		if tags.Required && num == 0 {
			return fmt.Errorf("required field is zero")
		}

	case TypeBool:
		_, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected boolean value")
		}

	case TypeArray:
		val := reflect.ValueOf(value)
		if val.Kind() != reflect.Slice {
			return fmt.Errorf("expected array value")
		}
		length := val.Len()
		if tags.Required && length == 0 {
			return fmt.Errorf("required array is empty")
		}
		if tags.MinLength != nil && length < *tags.MinLength {
			return fmt.Errorf("array length %d is less than minimum %d", length, *tags.MinLength)
		}
		if tags.MaxLength != nil && length > *tags.MaxLength {
			return fmt.Errorf("array length %d is greater than maximum %d", length, *tags.MaxLength)
		}
	}

	return nil
}
