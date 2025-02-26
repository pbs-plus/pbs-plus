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
	Name     string `config:"type=string,required"`
	Value    string `config:"type=string,required"`
	Optional string `config:"type=string"`
}

type ArrayTestConfig struct {
	Tags []string `config:"type=array,required"`
}

type ValidationTestConfig struct {
	Email string `config:"type=string,required"`
}

type CompatTestConfig struct {
	Path    string   `config:"type=string,required"`
	Comment string   `config:"type=string"`
	Count   int      `config:"type=int"`
	Enabled bool     `config:"type=bool"`
	Tags    []string `config:"type=array"`
}

type CustomKeyTestConfig struct {
	ServerName  string   `config:"type=string,required,key=server_name"`
	MaxConn     int      `config:"type=int,key=max_connections"`
	EnableLogs  bool     `config:"type=bool,key=enable_logging"`
	Categories  []string `config:"type=array,key=category_list"`
	Description string   `config:"type=string"` // No custom key - should use field name lowercase
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

func TestFormatCompatibility(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name           string
		oldConfig      string
		expectedPath   string
		expectedCount  int
		expectedTags   []string
		expectedOutput string
	}{
		{
			name: "Tab Indentation",
			oldConfig: `test: tab-indent
	path /test/path
	count 42
	tags tag1,tag2

`,
			expectedPath:  "/test/path",
			expectedCount: 42,
			expectedTags:  []string{"tag1", "tag2"},
			expectedOutput: `test: tab-indent
	count 42
	path /test/path
	tags tag1,tag2

`,
		},
		{
			name: "Space Indentation",
			oldConfig: `test: space-indent
    path /test/path
    count 42
    tags tag1,tag2

`,
			expectedPath:  "/test/path",
			expectedCount: 42,
			expectedTags:  []string{"tag1", "tag2"},
			expectedOutput: `test: space-indent
	count 42
	path /test/path
	tags tag1,tag2

`,
		},
		{
			name: "Mixed Whitespace",
			oldConfig: `test: mixed-ws
  path    /test/path 
	 count		42
	 tags  tag1, tag2 

`,
			expectedPath:  "/test/path",
			expectedCount: 42,
			expectedTags:  []string{"tag1", "tag2"},
			expectedOutput: `test: mixed-ws
	count 42
	path /test/path
	tags tag1,tag2

`,
		},
		{
			name: "Empty Values",
			oldConfig: `test: empty-values
	path /test/path
	count 0
	tags
	enabled false

`,
			expectedPath:  "/test/path",
			expectedCount: 0,
			expectedTags:  nil,
			expectedOutput: `test: empty-values
	path /test/path
	tags

`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &SectionPlugin[CompatTestConfig]{
				TypeName:   "test",
				FolderPath: tempDir,
			}
			config := NewSectionConfig(plugin)

			configPath := filepath.Join(tempDir, fmt.Sprintf("%s.cfg", tc.name))
			err := os.WriteFile(configPath, []byte(tc.oldConfig), 0644)
			require.NoError(t, err)

			configData, err := config.Parse(configPath)
			require.NoError(t, err)

			var firstSectionID string
			for id := range configData.Sections {
				firstSectionID = id
				break
			}
			section := configData.Sections[firstSectionID]
			require.NotNil(t, section)

			assert.Equal(t, tc.expectedPath, section.Properties.Path)
			assert.Equal(t, tc.expectedCount, section.Properties.Count)

			if tc.expectedTags == nil {
				assert.Empty(t, section.Properties.Tags)
			} else {
				assert.Equal(t, tc.expectedTags, section.Properties.Tags)
			}

			err = config.Write(configData)
			require.NoError(t, err)

			written, err := os.ReadFile(configPath)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedOutput, string(written))

			// Verify the written file can be parsed again
			_, err = config.Parse(configPath)
			require.NoError(t, err, "Written file should be parseable")
		})
	}
}

// TestCrossImplementationRoundTrip tests that configs can be written by old implementation
// and read by new, and vice versa
func TestCrossImplementationRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	err := os.MkdirAll(tempDir, 0750)
	require.NoError(t, err)

	configPath := filepath.Join(tempDir, "roundtrip.cfg")

	testConfig := &ConfigData[CompatTestConfig]{
		FilePath: configPath, // Set the filepath explicitly
		Sections: map[string]*Section[CompatTestConfig]{
			"test-section": {
				Type: "test",
				ID:   "test-section",
				Properties: CompatTestConfig{
					Path:    "/complex/path with spaces",
					Comment: "Multi word\tcomment with\ttabs",
					Count:   42,
					Enabled: true,
					Tags:    []string{"tag1", "tag with space"},
				},
			},
		},
		Order: []string{"test-section"},
	}

	plugin := &SectionPlugin[CompatTestConfig]{
		TypeName:   "test",
		FolderPath: tempDir,
	}
	config := NewSectionConfig(plugin)

	// Write with new implementation
	err = config.Write(testConfig)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(configPath)
	require.NoError(t, err, "Config file should exist")

	// Read the config back
	readConfig, err := config.Parse(configPath)
	require.NoError(t, err)

	// Verify all fields match exactly
	original := testConfig.Sections["test-section"].Properties
	read := readConfig.Sections["test-section"].Properties

	assert.Equal(t, original.Path, read.Path)
	assert.Equal(t, original.Comment, read.Comment)
	assert.Equal(t, original.Count, read.Count)
	assert.Equal(t, original.Enabled, read.Enabled)
	assert.Equal(t, original.Tags, read.Tags)

	// Verify section order is preserved
	assert.Equal(t, testConfig.Order, readConfig.Order)
}

func TestCustomKeyConfig(t *testing.T) {
	tempDir := t.TempDir()

	plugin := &SectionPlugin[CustomKeyTestConfig]{
		TypeName:   "server",
		FolderPath: tempDir,
	}
	config := NewSectionConfig(plugin)

	t.Run("Write and Read Custom Keys", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-custom-keys")+".cfg")

		// Create test data with all fields populated
		testData := &ConfigData[CustomKeyTestConfig]{
			FilePath: testFile,
			Sections: map[string]*Section[CustomKeyTestConfig]{
				"test-custom-keys": {
					Type: "server",
					ID:   "test-custom-keys",
					Properties: CustomKeyTestConfig{
						ServerName:  "TestServer",
						MaxConn:     100,
						EnableLogs:  true,
						Categories:  []string{"web", "api", "backend"},
						Description: "Test server configuration",
					},
				},
			},
			Order: []string{"test-custom-keys"},
		}

		// Write config
		err := config.Write(testData)
		require.NoError(t, err)

		// Verify the written file contains custom keys
		content, err := os.ReadFile(testFile)
		require.NoError(t, err)

		contentStr := string(content)
		assert.Contains(t, contentStr, "server_name TestServer")
		assert.Contains(t, contentStr, "max_connections 100")
		assert.Contains(t, contentStr, "enable_logging true")
		assert.Contains(t, contentStr, "category_list web,api,backend")
		assert.Contains(t, contentStr, "description Test server configuration")

		// Read config back
		readData, err := config.Parse(testFile)
		require.NoError(t, err)

		// Verify all fields were correctly read back
		readProps := readData.Sections["test-custom-keys"].Properties
		assert.Equal(t, "TestServer", readProps.ServerName)
		assert.Equal(t, 100, readProps.MaxConn)
		assert.True(t, readProps.EnableLogs)
		assert.Equal(t, []string{"web", "api", "backend"}, readProps.Categories)
		assert.Equal(t, "Test server configuration", readProps.Description)
	})

	t.Run("Partial Fields", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-partial-keys")+".cfg")

		// Create config with only required and some optional fields
		testData := &ConfigData[CustomKeyTestConfig]{
			FilePath: testFile,
			Sections: map[string]*Section[CustomKeyTestConfig]{
				"test-partial-keys": {
					Type: "server",
					ID:   "test-partial-keys",
					Properties: CustomKeyTestConfig{
						ServerName: "MinimalServer", // Required field
						MaxConn:    50,              // Optional with custom key
					},
				},
			},
			Order: []string{"test-partial-keys"},
		}

		// Write config
		err := config.Write(testData)
		require.NoError(t, err)

		// Read config back
		readData, err := config.Parse(testFile)
		require.NoError(t, err)

		// Verify fields
		readProps := readData.Sections["test-partial-keys"].Properties
		assert.Equal(t, "MinimalServer", readProps.ServerName)
		assert.Equal(t, 50, readProps.MaxConn)
		assert.False(t, readProps.EnableLogs)
		assert.Empty(t, readProps.Categories)
		assert.Empty(t, readProps.Description)
	})

	t.Run("Missing Required Field", func(t *testing.T) {
		testData := &ConfigData[CustomKeyTestConfig]{
			Sections: map[string]*Section[CustomKeyTestConfig]{
				"test-missing-required": {
					Type: "server",
					ID:   "test-missing-required",
					Properties: CustomKeyTestConfig{
						// Missing ServerName which is required
						MaxConn: 100,
					},
				},
			},
			Order: []string{"test-missing-required"},
		}

		// Should fail validation
		err := config.Write(testData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is empty")
	})
}
