//go:build linux

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test struct definitions
type BasicTestConfig struct {
	Name     string `config:"type=string,required,desc=Test name"`
	Value    string `config:"type=string,required,desc=Test value"`
	Optional string `config:"type=string,desc=Optional value"`
}

type ArrayTestConfig struct {
	Tags []string `config:"type=array,required,desc=Test tags"`
}

type ValidationTestConfig struct {
	Email string `config:"type=string,required,desc=Email address,pattern=^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"`
}

type CompatTestConfig struct {
	Path    string   `config:"type=string,required,desc=Test path"`
	Comment string   `config:"type=string,desc=Optional comment"`
	Count   int      `config:"type=int,desc=A number"`
	Enabled bool     `config:"type=bool,desc=Boolean flag"`
	Tags    []string `config:"type=array,desc=String array"`
}

func TestSectionConfig_BasicOperations(t *testing.T) {
	// Setup
	tempDir := t.TempDir()

	// Create plugin with basic config
	testPlugin := &SectionPlugin[BasicTestConfig]{
		TypeName:   "test",
		FolderPath: tempDir,
		Validate:   nil,
	}
	config := NewSectionConfig(testPlugin)

	t.Run("Create and Read", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-basic-cr")+".cfg")
		testData := &ConfigData[BasicTestConfig]{
			Sections: map[string]*Section[BasicTestConfig]{
				"test-basic-cr": {
					Type: "test",
					ID:   "test-basic-cr",
					Properties: BasicTestConfig{
						Name:  "Test 1",
						Value: "Value 1",
					},
				},
			},
			Order: []string{"test-basic-cr"},
		}

		// Write config
		err := config.Write(testData)
		require.NoError(t, err)

		// Read config
		readData, err := config.Parse(testFile)
		require.NoError(t, err)

		// Verify data
		assert.Equal(t, testData.Order, readData.Order)
		assert.Equal(t, testData.Sections["test-basic-cr"].Properties.Name,
			readData.Sections["test-basic-cr"].Properties.Name)
	})

	t.Run("Missing Required Property", func(t *testing.T) {
		testData := &ConfigData[BasicTestConfig]{
			Sections: map[string]*Section[BasicTestConfig]{
				"test-missing": {
					Type: "test",
					ID:   "test-missing",
					Properties: BasicTestConfig{
						Name: "Test 1",
						// Missing required Value
					},
				},
			},
			Order: []string{"test-missing"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})
}

func TestSectionConfig_ArrayProperties(t *testing.T) {
	// Setup
	tempDir := t.TempDir()

	// Create plugin with array config
	arrayPlugin := &SectionPlugin[ArrayTestConfig]{
		TypeName:   "array-test",
		FolderPath: tempDir,
		Validate:   nil,
	}
	config := NewSectionConfig(arrayPlugin)

	t.Run("Array Property Handling", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-array")+".cfg")
		testData := &ConfigData[ArrayTestConfig]{
			Sections: map[string]*Section[ArrayTestConfig]{
				"test-array": {
					Type: "array-test",
					ID:   "test-array",
					Properties: ArrayTestConfig{
						Tags: []string{"tag1", "tag2", "tag3"},
					},
				},
			},
			Order: []string{"test-array"},
		}

		err := config.Write(testData)
		require.NoError(t, err)

		readData, err := config.Parse(testFile)
		require.NoError(t, err)

		assert.Equal(t, testData.Sections["test-array"].Properties.Tags,
			readData.Sections["test-array"].Properties.Tags)
	})
}

func TestSectionConfig_ValidationRules(t *testing.T) {
	// Setup
	tempDir := t.TempDir()

	// Create plugin with validation config
	validationPlugin := &SectionPlugin[ValidationTestConfig]{
		TypeName:   "validation-test",
		FolderPath: tempDir,
		Validate: func(c ValidationTestConfig) error {
			if len(c.Email) > 254 {
				return fmt.Errorf("email too long")
			}
			return nil
		},
	}
	config := NewSectionConfig(validationPlugin)

	t.Run("Valid Pattern", func(t *testing.T) {
		testData := &ConfigData[ValidationTestConfig]{
			Sections: map[string]*Section[ValidationTestConfig]{
				"test-validate": {
					Type: "validation-test",
					ID:   "test-validate",
					Properties: ValidationTestConfig{
						Email: "test@example.com",
					},
				},
			},
			Order: []string{"test-validate"},
		}

		err := config.Write(testData)
		require.NoError(t, err)
	})

	t.Run("Invalid Pattern", func(t *testing.T) {
		testData := &ConfigData[ValidationTestConfig]{
			Sections: map[string]*Section[ValidationTestConfig]{
				"test-invalid": {
					Type: "validation-test",
					ID:   "test-invalid",
					Properties: ValidationTestConfig{
						Email: "not-an-email",
					},
				},
			},
			Order: []string{"test-invalid"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})

	t.Run("Email Too Long", func(t *testing.T) {
		longEmail := "very-long-email"
		for i := 0; i < 250; i++ {
			longEmail += "x"
		}
		longEmail += "@example.com"

		testData := &ConfigData[ValidationTestConfig]{
			Sections: map[string]*Section[ValidationTestConfig]{
				"test-long-email": {
					Type: "validation-test",
					ID:   "test-long-email",
					Properties: ValidationTestConfig{
						Email: longEmail,
					},
				},
			},
			Order: []string{"test-long-email"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})
}

func TestBackwardCompatibility(t *testing.T) {
	// Create temp directory for test
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test.cfg")

	// Create a config file in the old format
	oldFormatConfig := `test: test-item
	path /test/path
	comment test comment
	count 42
	enabled true
	tags tag1,tag2,tag3

`
	err := os.WriteFile(configPath, []byte(oldFormatConfig), 0644)
	require.NoError(t, err)

	// Create config manager with our test config type
	plugin := &SectionPlugin[CompatTestConfig]{
		TypeName:   "test",
		FolderPath: tempDir,
	}
	config := NewSectionConfig(plugin)

	// Read the old format config
	configData, err := config.Parse(configPath)
	require.NoError(t, err)

	// Verify the parsed data
	section, exists := configData.Sections["test-item"]
	require.True(t, exists)
	require.Equal(t, "test", section.Type)
	require.Equal(t, "test-item", section.ID)

	// Verify all fields were parsed correctly
	props := section.Properties
	require.Equal(t, "/test/path", props.Path)
	require.Equal(t, "test comment", props.Comment)
	require.Equal(t, 42, props.Count)
	require.True(t, props.Enabled)
	require.Equal(t, []string{"tag1", "tag2", "tag3"}, props.Tags)

	// Now write it back and verify it maintains the same format
	err = config.Write(configData)
	require.NoError(t, err)

	// Read the file contents directly
	written, err := os.ReadFile(configPath)
	require.NoError(t, err)

	// The written format should match what we expect from the old format
	// (although it might have slightly different whitespace)
	expectedFormat := `test: test-item
	path /test/path
	comment test comment
	count 42
	enabled true
	tags tag1,tag2,tag3

`
	// Compare the content (ignoring whitespace differences)
	require.Equal(t,
		normalizeWhitespace(expectedFormat),
		normalizeWhitespace(string(written)),
	)
}

// Test edge cases from old format
func TestEdgeCaseCompatibility(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "edge.cfg")

	// Create a config file with edge cases
	oldFormatConfig := `test: edge-case
	path /test/path
	comment 
	count 
	enabled false
	tags 

`
	err := os.WriteFile(configPath, []byte(oldFormatConfig), 0644)
	require.NoError(t, err)

	plugin := &SectionPlugin[CompatTestConfig]{
		TypeName:   "test",
		FolderPath: tempDir,
	}
	config := NewSectionConfig(plugin)

	// Read and verify edge cases
	configData, err := config.Parse(configPath)
	require.NoError(t, err)

	section := configData.Sections["edge-case"]
	props := section.Properties

	// Verify empty fields are handled correctly
	require.Equal(t, "/test/path", props.Path)
	require.Equal(t, "", props.Comment)
	require.Equal(t, 0, props.Count)
	require.False(t, props.Enabled)
	require.Empty(t, props.Tags)

	// Write and verify it maintains format
	err = config.Write(configData)
	require.NoError(t, err)
}

// Helper to normalize whitespace for comparison
func normalizeWhitespace(s string) string {
	// Could implement if needed for comparing file contents
	return s
}
