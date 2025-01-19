package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

// Schema represents a configuration schema definition
type Schema struct {
	Type        PropertyType
	Description string
	Required    bool
	MinLength   *int
	MaxLength   *int
	Properties  map[string]*Schema
	Items       *Schema // For array types
	Pattern     *string // For string validation
}

// SectionPlugin defines the schema and behavior for a specific section type
type SectionPlugin struct {
	TypeName    string
	Properties  map[string]*Schema
	IDProperty  string
	Validations []ValidationFunc
	FolderPath  string
}

type ValidationFunc func(data map[string]string) error

// SectionConfig manages the configuration system
// FormatStyle defines different configuration file formats
type FormatStyle int

const (
	FormatDefault FormatStyle = iota
	FormatSystemd
	FormatINI
)

type SectionConfig struct {
	mu               sync.RWMutex
	plugins          map[string]*SectionPlugin
	idSchema         *Schema
	allowUnknown     bool
	formatStyle      FormatStyle
	includeFiles     []string
	watcher          *fsnotify.Watcher
	onConfigChange   func(*ConfigData)
	parseSectionHead func(string) (string, string, error)
	parseSectionLine func(string) (string, string, error)
}

// Section represents a single configuration section
type Section struct {
	Type       string
	ID         string
	Properties map[string]string
}

// ConfigData holds all sections and their ordering
type ConfigData struct {
	FilePath string
	Sections map[string]*Section
	Order    []string
}

// WatchCallback is called when configuration changes are detected
type WatchCallback func(*ConfigData)

func NewSectionConfig(idSchema *Schema) *SectionConfig {
	return &SectionConfig{
		plugins:          make(map[string]*SectionPlugin),
		idSchema:         idSchema,
		allowUnknown:     false,
		parseSectionHead: defaultParseSectionHeader,
		parseSectionLine: defaultParseSectionContent,
	}
}

func (sc *SectionConfig) RegisterPlugin(plugin *SectionPlugin) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.plugins[plugin.TypeName] = plugin
}

func (sc *SectionConfig) GetPlugin(typeName string) *SectionPlugin {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	plugin, exists := sc.plugins[typeName]
	if exists {
		return plugin
	}

	return nil
}

// Parse reads and parses a configuration file
func (sc *SectionConfig) Parse(filename string) (*ConfigData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &ConfigData{
		Sections: make(map[string]*Section),
		Order:    make([]string, 0),
		FilePath: filename,
	}

	reader := bufio.NewReader(file)
	var currentSection *Section
	lineNum := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading line %d: %w", lineNum, err)
		}

		line = strings.TrimSpace(line)
		lineNum++

		if line == "" {
			if currentSection != nil {
				if err := sc.validateSection(currentSection); err != nil {
					return nil, fmt.Errorf("validation error in section %s: %w", currentSection.ID, err)
				}
				config.Sections[currentSection.ID] = currentSection
				config.Order = append(config.Order, currentSection.ID)
				currentSection = nil
			}
			if err == io.EOF {
				break
			}
			continue
		}

		if currentSection == nil {
			// Try to parse section header
			sectionType, sectionID, err := sc.parseSectionHead(line)
			if err != nil {
				return nil, fmt.Errorf("error parsing section header at line %d: %w", lineNum, err)
			}

			if err := sc.validateSectionType(sectionType); err != nil {
				return nil, fmt.Errorf("invalid section type at line %d: %w", lineNum, err)
			}

			currentSection = &Section{
				Type:       sectionType,
				ID:         sectionID,
				Properties: make(map[string]string),
			}
		} else {
			// Parse section content
			key, value, err := sc.parseSectionLine(line)
			if err != nil {
				return nil, fmt.Errorf("error parsing line %d: %w", lineNum, err)
			}

			if err := sc.validateProperty(currentSection.Type, key, value); err != nil {
				return nil, fmt.Errorf("invalid property at line %d: %w", lineNum, err)
			}

			currentSection.Properties[key] = value
		}

		if err == io.EOF {
			break
		}
	}

	if currentSection != nil {
		if err := sc.validateSection(currentSection); err != nil {
			return nil, fmt.Errorf("validation error in section %s: %w", currentSection.ID, err)
		}
		config.Sections[currentSection.ID] = currentSection
		config.Order = append(config.Order, currentSection.ID)
	}

	return config, nil
}

func (sc *SectionConfig) Write(config *ConfigData) error {
	// First validate all sections
	for sectionID, section := range config.Sections {
		plugin, exists := sc.plugins[section.Type]
		if !exists && !sc.allowUnknown {
			return fmt.Errorf("unknown section type '%s'", section.Type)
		}

		if exists {
			// Validate properties
			for key, value := range section.Properties {
				if err := sc.validateProperty(section.Type, key, value); err != nil {
					return fmt.Errorf("section '%s', property '%s': %w", sectionID, key, err)
				}
			}

			// Check required properties
			for propName, propSchema := range plugin.Properties {
				if propSchema.Required && section.Properties[propName] == "" {
					return fmt.Errorf("section '%s': required property '%s' is missing", sectionID, propName)
				}
			}
		}
	}

	// Then write the file once using the order
	var output strings.Builder
	for _, sectionID := range config.Order {
		section := config.Sections[sectionID]
		if section == nil {
			continue
		}

		plugin := sc.plugins[section.Type]

		filename := config.FilePath
		if !sc.allowUnknown && config.FilePath == "" {
			filename = filepath.Join(plugin.FolderPath, utils.EncodePath(sectionID))
		}

		dir := filepath.Dir(filename)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		output.WriteString(fmt.Sprintf("%s: %s\n", section.Type, sectionID))
		for key, value := range section.Properties {
			output.WriteString(fmt.Sprintf("\t%s %s\n", key, value))
		}
		output.WriteString("\n")

		err := os.WriteFile(filename, []byte(output.String()), 0644)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sc *SectionConfig) validateSectionType(sectionType string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if _, exists := sc.plugins[sectionType]; !exists && !sc.allowUnknown {
		return fmt.Errorf("unknown section type: %s", sectionType)
	}
	return nil
}

// ParseValue parses a string value according to its schema type
func parseValue(value string, schema *Schema) (interface{}, error) {
	switch schema.Type {
	case TypeString:
		if schema.Pattern != nil {
			matched, err := regexp.MatchString(*schema.Pattern, value)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern: %w", err)
			}
			if !matched {
				return nil, fmt.Errorf("value doesn't match pattern %s", *schema.Pattern)
			}
		}
		return value, nil

	case TypeInt:
		return strconv.Atoi(value)

	case TypeBool:
		return strconv.ParseBool(value)

	case TypeArray:
		// Split by commas if the value contains any, otherwise treat as single item
		var items []string
		if strings.Contains(value, ",") {
			items = strings.Split(value, ",")
		} else {
			items = []string{value}
		}

		result := make([]interface{}, len(items))
		for i, item := range items {
			parsed, err := parseValue(strings.TrimSpace(item), schema.Items)
			if err != nil {
				return nil, fmt.Errorf("error parsing array item %d: %w", i, err)
			}
			result[i] = parsed
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported type: %s", schema.Type)
	}
}

func (sc *SectionConfig) validateProperty(sectionType, key, value string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	plugin, exists := sc.plugins[sectionType]
	if !exists {
		if sc.allowUnknown {
			return nil
		}
		return fmt.Errorf("unknown section type: %s", sectionType)
	}

	propSchema, exists := plugin.Properties[key]
	if !exists {
		return fmt.Errorf("unknown property '%s' for section type '%s'", key, sectionType)
	}

	if propSchema.Pattern != nil {
		matched, err := regexp.MatchString(*propSchema.Pattern, value)
		if err != nil {
			return fmt.Errorf("invalid pattern for '%s': %w", key, err)
		}
		if !matched {
			return fmt.Errorf("value '%s' for property '%s' does not match pattern '%s'", value, key, *propSchema.Pattern)
		}
	}

	if propSchema.MinLength != nil && len(value) < *propSchema.MinLength {
		return fmt.Errorf("value for '%s' is too short (minimum length: %d)", key, *propSchema.MinLength)
	}

	if propSchema.MaxLength != nil && len(value) > *propSchema.MaxLength {
		return fmt.Errorf("value for '%s' is too long (maximum length: %d)", key, *propSchema.MaxLength)
	}

	return nil
}

func (sc *SectionConfig) validateSection(section *Section) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	plugin, exists := sc.plugins[section.Type]
	if !exists {
		if sc.allowUnknown {
			return nil
		}
		return fmt.Errorf("unknown section type: %s", section.Type)
	}

	// Check required properties
	for propName, schema := range plugin.Properties {
		if schema.Required {
			if _, exists := section.Properties[propName]; !exists {
				return fmt.Errorf("required property %s is missing", propName)
			}
		}
	}

	// Run custom validations
	for _, validate := range plugin.Validations {
		if err := validate(section.Properties); err != nil {
			return err
		}
	}

	return nil
}

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

	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid property format")
	}

	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	if key == "" {
		return "", "", fmt.Errorf("empty property key")
	}

	return key, value, nil
}
