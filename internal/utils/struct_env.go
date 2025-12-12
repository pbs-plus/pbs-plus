package utils

import (
	"fmt"
	"reflect"
	"strings"
)

func StructToEnvVars(s any) ([]string, error) {
	tagName := "env"

	v := reflect.ValueOf(s)

	// Ensure it's a struct (or a pointer to a struct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input is not a struct")
	}

	envVars := []string{}
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Only process exported fields
		if field.PkgPath != "" {
			continue
		}

		fieldValue := v.Field(i)

		// Get the environment variable key
		key := ""
		if tagName != "" {
			key = field.Tag.Get(tagName)
		}
		if key == "" {
			// Convert field name to uppercase with underscores
			key = toSnakeUpper(field.Name)
		}

		// Skip if the key is empty
		if key == "" {
			continue
		}

		// Get the value as a string
		value := fmt.Sprintf("%v", fieldValue.Interface())

		// Append to the envVars slice
		envVars = append(envVars, fmt.Sprintf("PBS_PLUS__%s=%s", key, value))
	}

	return envVars, nil
}

// toSnakeUpper converts a CamelCase string to SNAKE_UPPER_CASE.
func toSnakeUpper(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && strings.ToUpper(string(r)) == string(r) && strings.ToLower(string(s[i-1])) == string(s[i-1]) {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToUpper(string(result))
}
