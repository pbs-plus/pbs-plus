package jobs

import (
	"fmt"
	"reflect"
	"strings"
)

// StructToEnvVars converts a struct's fields to PBS_PLUS__ prefixed env vars.
func StructToEnvVars(s any) ([]string, error) {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input is not a struct")
	}
	envVars := []string{}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		fieldValue := v.Field(i)
		key := field.Tag.Get("env")
		if key == "" {
			key = toSnakeUpper(field.Name)
		}
		if key == "" {
			continue
		}
		value := fmt.Sprintf("%v", fieldValue.Interface())
		envVars = append(envVars, fmt.Sprintf("PBS_PLUS__%s=%s", key, value))
	}
	return envVars, nil
}

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
