//go:build linux

package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

// PropertyType represents the type of a configuration property
type PropertyType string

const (
	TypeString PropertyType = "string"
	TypeInt    PropertyType = "int"
	TypeBool   PropertyType = "bool"
	TypeArray  PropertyType = "array"
	TypeObject PropertyType = "object"
)

// SectionPlugin defines the schema and behavior for a specific section type
type SectionPlugin[T any] struct {
	TypeName   string
	IDProperty string
	Validate   func(T) error
	FolderPath string
}

// Section represents a single configuration section
type Section[T any] struct {
	Type       string
	ID         string
	Properties T
}

// ConfigData holds all sections and their ordering
type ConfigData[T any] struct {
	FilePath string
	Sections map[string]*Section[T]
	Order    []string
}

type SectionConfig[T any] struct {
	mu               sync.RWMutex
	fileMutex        *FileMutexManager
	plugin           *SectionPlugin[T]
	allowUnknown     bool
	includeFiles     []string
	watcher          *fsnotify.Watcher
	onConfigChange   func(*ConfigData[T])
	parseSectionHead func(string) (string, string, error)
	parseSectionLine func(string) (string, string, error)
}

func NewSectionConfig[T any](plugin *SectionPlugin[T]) *SectionConfig[T] {
	return &SectionConfig[T]{
		plugin:           plugin,
		allowUnknown:     false,
		parseSectionHead: defaultParseSectionHeader,
		parseSectionLine: defaultParseSectionContent,
		fileMutex:        NewFileMutexManager(),
	}
}

// Parse reads and parses a configuration file
func (sc *SectionConfig[T]) Parse(filename string) (*ConfigData[T], error) {
	var config *ConfigData[T]
	err := sc.fileMutex.WithReadLock(filename, func() error {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		config = &ConfigData[T]{
			Sections: make(map[string]*Section[T]),
			Order:    make([]string, 0),
			FilePath: filename,
		}

		reader := bufio.NewReader(file)
		var currentSection *Section[T]
		var currentProps map[string]string
		lineNum := 0

		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return fmt.Errorf("error reading line %d: %w", lineNum, err)
			}

			line = strings.TrimSpace(line)
			lineNum++

			if line == "" {
				if currentSection != nil && currentProps != nil {
					props, err := sc.unmarshal(currentProps)
					if err != nil {
						return fmt.Errorf("error unmarshaling properties: %w", err)
					}
					currentSection.Properties = props

					if err := sc.validateSection(currentSection); err != nil {
						return fmt.Errorf("validation error in section %s: %w", currentSection.ID, err)
					}

					config.Sections[currentSection.ID] = currentSection
					config.Order = append(config.Order, currentSection.ID)
					currentSection = nil
					currentProps = nil
				}
				if err == io.EOF {
					break
				}
				continue
			}

			if currentSection == nil {
				sectionType, sectionID, err := sc.parseSectionHead(line)
				if err != nil {
					return fmt.Errorf("error parsing section header at line %d: %w", lineNum, err)
				}

				currentSection = &Section[T]{
					Type: sectionType,
					ID:   sectionID,
				}
				currentProps = make(map[string]string)
			} else {
				key, value, err := sc.parseSectionLine(line)
				if err != nil {
					return fmt.Errorf("error parsing line %d: %w", lineNum, err)
				}
				currentProps[key] = value
			}

			if err == io.EOF {
				break
			}
		}

		if currentSection != nil && currentProps != nil {
			props, err := sc.unmarshal(currentProps)
			if err != nil {
				return fmt.Errorf("error unmarshaling final properties: %w", err)
			}
			currentSection.Properties = props

			if err := sc.validateSection(currentSection); err != nil {
				return fmt.Errorf("validation error in section %s: %w", currentSection.ID, err)
			}

			config.Sections[currentSection.ID] = currentSection
			config.Order = append(config.Order, currentSection.ID)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return config, nil
}

func (sc *SectionConfig[T]) Write(config *ConfigData[T]) error {
	// First validate all sections
	for sectionID, section := range config.Sections {
		if err := sc.validateSection(section); err != nil {
			return fmt.Errorf("validation error in section %s: %w", sectionID, err)
		}
	}

	// If no FilePath is provided, write each section to a separate file
	if config.FilePath == "" && !sc.allowUnknown {
		for _, sectionID := range config.Order {
			section := config.Sections[sectionID]
			if section == nil {
				continue
			}

			filename := filepath.Join(sc.plugin.FolderPath, utils.EncodePath(sectionID)+".cfg")
			singleConfig := &ConfigData[T]{
				FilePath: filename,
				Sections: map[string]*Section[T]{sectionID: section},
				Order:    []string{sectionID},
			}
			if err := sc.Write(singleConfig); err != nil {
				return err
			}
		}
		return nil
	}

	var output strings.Builder

	for _, sectionID := range config.Order {
		section := config.Sections[sectionID]
		if section == nil {
			continue
		}

		props, err := sc.marshal(section.Properties)
		if err != nil {
			return fmt.Errorf("error marshaling properties for section %s: %w", sectionID, err)
		}

		output.WriteString(fmt.Sprintf("%s: %s\n", section.Type, sectionID))

		// Sort keys for stable output order
		keys := make([]string, 0, len(props))
		for k := range props {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			value := props[key]
			if value != "" {
				output.WriteString(fmt.Sprintf("\t%s %s\n", key, value))
			} else if key == "tags" {
				output.WriteString(fmt.Sprintf("\t%s\n", key))
			}
		}

		output.WriteString("\n")
	}

	err := sc.fileMutex.WithWriteLock(config.FilePath, func() error {
		dir := filepath.Dir(config.FilePath)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		return os.WriteFile(config.FilePath, []byte(output.String()), 0644)
	})

	return err
}

// marshalValue converts a reflected value to its string representation
func marshalValue(value reflect.Value, tag ConfigTag) (string, error) {
	switch tag.Type {
	case TypeString:
		return value.String(), nil
	case TypeInt:
		return strconv.FormatInt(value.Int(), 10), nil
	case TypeBool:
		return strconv.FormatBool(value.Bool()), nil
	case TypeArray:
		if value.Kind() != reflect.Slice {
			return "", fmt.Errorf("expected slice for array type")
		}
		var items []string
		for i := 0; i < value.Len(); i++ {
			item := value.Index(i)
			str, err := marshalValue(item, ConfigTag{Type: TypeString})
			if err != nil {
				return "", fmt.Errorf("error marshaling array item %d: %w", i, err)
			}
			items = append(items, str)
		}
		return strings.Join(items, ","), nil
	default:
		return "", fmt.Errorf("unsupported type: %s", tag.Type)
	}
}

// unmarshalValue converts a string to the appropriate type based on the field's type
func unmarshalValue(str string, fieldType reflect.Type, tag ConfigTag) (reflect.Value, error) {
	switch tag.Type {
	case TypeString:
		return reflect.ValueOf(str), nil
	case TypeInt:
		if str == "" && !tag.Required {
			return reflect.Zero(fieldType), nil
		}
		val, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("invalid integer: %w", err)
		}
		return reflect.ValueOf(val).Convert(fieldType), nil
	case TypeBool:
		if str == "" && !tag.Required {
			return reflect.Zero(fieldType), nil
		}
		val, err := strconv.ParseBool(str)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("invalid boolean: %w", err)
		}
		return reflect.ValueOf(val), nil
	case TypeArray:
		sliceType := reflect.SliceOf(fieldType.Elem())
		slice := reflect.MakeSlice(sliceType, 0, 0)

		if str == "" {
			return slice, nil
		}

		items := strings.Split(str, ",")
		for _, item := range items {
			val, err := unmarshalValue(strings.TrimSpace(item), fieldType.Elem(), ConfigTag{Type: TypeString})
			if err != nil {
				return reflect.Value{}, fmt.Errorf("error unmarshaling array item: %w", err)
			}
			slice = reflect.Append(slice, val)
		}
		return slice, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported type: %s", tag.Type)
	}
}

func (sc *SectionConfig[T]) marshal(data T) (map[string]string, error) {
	result := make(map[string]string)
	val := reflect.ValueOf(data)
	typ := reflect.TypeOf(data)

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("data must be a struct")
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)

		tag := field.Tag.Get("config")
		if tag == "-" || tag == "" {
			continue
		}

		configTag, err := parseConfigTags(tag)
		if err != nil {
			return nil, fmt.Errorf("invalid config tags for field %s: %w", field.Name, err)
		}

		// Skip zero values for non-required fields
		if !configTag.Required && value.IsZero() {
			continue
		}

		key := strings.ToLower(field.Name)
		if value.Kind() == reflect.Bool && !value.Bool() {
			continue // Skip false boolean values
		}

		str, err := marshalValue(value, configTag)
		if err != nil {
			return nil, fmt.Errorf("error marshaling field %s: %w", field.Name, err)
		}

		result[key] = str
	}

	return result, nil
}

func (sc *SectionConfig[T]) unmarshal(data map[string]string) (T, error) {
	var result T
	resultVal := reflect.New(reflect.TypeOf(result)).Elem()
	typ := reflect.TypeOf(result)

	if typ.Kind() != reflect.Struct {
		return result, fmt.Errorf("result must be a struct")
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		tag := field.Tag.Get("config")
		if tag == "-" || tag == "" {
			continue
		}

		configTag, err := parseConfigTags(tag)
		if err != nil {
			return result, fmt.Errorf("invalid config tags for field %s: %w", field.Name, err)
		}

		key := strings.ToLower(field.Name)
		str, ok := data[key]
		if !ok {
			if !ok {
				if configTag.Required { // Only error if the field is explicitly marked as required
					return result, fmt.Errorf("required field %s is missing", field.Name)
				}
				continue // Skip optional fields that aren't present
			}
		}

		val, err := unmarshalValue(str, field.Type, configTag)
		if err != nil {
			if configTag.Required {
				return result, fmt.Errorf("error unmarshaling field %s: %w", field.Name, err)
			}

			continue
		}

		resultVal.Field(i).Set(val)
	}

	return resultVal.Interface().(T), nil
}

func (sc *SectionConfig[T]) validateSection(section *Section[T]) error {
	val := reflect.ValueOf(section.Properties)
	typ := reflect.TypeOf(section.Properties)

	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("properties must be a struct")
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)

		tag := field.Tag.Get("config")
		if tag == "-" || tag == "" {
			continue
		}

		configTags, err := parseConfigTags(tag)
		if err != nil {
			return fmt.Errorf("invalid config tags for field %s: %w", field.Name, err)
		}

		if err := validateFieldWithTags(value.Interface(), configTags); err != nil {
			return fmt.Errorf("validation failed for field %s: %w", field.Name, err)
		}
	}

	if sc.plugin.Validate != nil {
		if err := sc.plugin.Validate(section.Properties); err != nil {
			return fmt.Errorf("custom validation failed: %w", err)
		}
	}

	return nil
}

// Helper functions remain unchanged
func defaultParseSectionHeader(line string) (string, string, error) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid section header format")
	}

	sectionType := strings.TrimSpace(parts[0])
	sectionID := strings.TrimSpace(parts[1])

	if sectionType == "" || sectionID == "" {
		return "", "", fmt.Errorf("empty section type or ID")
	}

	return sectionType, sectionID, nil
}

func defaultParseSectionContent(line string) (string, string, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", "", fmt.Errorf("empty line")
	}

	// Split on first whitespace (space or tab)
	var key, value string
	parts := strings.Fields(line)

	if len(parts) == 0 {
		return "", "", fmt.Errorf("empty property key")
	}

	key = parts[0]
	if len(parts) > 1 {
		// Reconstruct value by joining remaining parts to preserve original spacing
		originalIndex := strings.Index(line, key) + len(key)
		value = strings.TrimSpace(line[originalIndex:])
	}

	if key == "" {
		return "", "", fmt.Errorf("empty property key")
	}

	return key, value, nil
}
