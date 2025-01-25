//go:build linux

package config

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSectionConfig_BasicOperations(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	minLength := 3
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
		MinLength:   &minLength,
	}
	config := NewSectionConfig(idSchema)

	// Register a test plugin
	testPlugin := &SectionPlugin{
		FolderPath: tempDir,
		TypeName:   "test",
		Properties: map[string]*Schema{
			"name": {
				Type:        TypeString,
				Description: "Test name",
				Required:    true,
			},
			"value": {
				Type:        TypeString,
				Description: "Test value",
				Required:    true,
			},
			"optional": {
				Type:        TypeString,
				Description: "Optional value",
				Required:    false,
			},
		},
	}
	config.RegisterPlugin(testPlugin)

	t.Run("Create and Read", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-basic-cr")+".cfg")
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-basic-cr": {
					Type: "test",
					ID:   "test-basic-cr",
					Properties: map[string]string{
						"name":  "Test 1",
						"value": "Value 1",
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
		assert.Equal(t, testData.Sections["test-basic-cr"].Properties["name"],
			readData.Sections["test-basic-cr"].Properties["name"])
	})

	t.Run("Missing Required Property", func(t *testing.T) {
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-missing": {
					Type: "test",
					ID:   "test-missing",
					Properties: map[string]string{
						"name": "Test 1",
						// Missing required "value" property
					},
				},
			},
			Order: []string{"test-missing"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})

	t.Run("Unknown Section Type", func(t *testing.T) {
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-unknown": {
					Type: "unknown",
					ID:   "test-unknown",
					Properties: map[string]string{
						"name": "Test 1",
					},
				},
			},
			Order: []string{"test-unknown"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})

	t.Run("Allow Unknown Sections", func(t *testing.T) {
		config := NewSectionConfig(idSchema)
		config.allowUnknown = true

		testFile := filepath.Join(tempDir, utils.EncodePath("test-allow-unknown")+".cfg")
		testData := &ConfigData{
			FilePath: testFile,
			Sections: map[string]*Section{
				"test-allow-unknown": {
					Type: "unknown",
					ID:   "test-allow-unknown",
					Properties: map[string]string{
						"name": "Test 1",
					},
				},
			},
			Order: []string{"test-allow-unknown"},
		}

		err := config.Write(testData)
		require.NoError(t, err)

		readData, err := config.Parse(testFile)
		require.NoError(t, err)
		assert.Equal(t, testData.Sections["test-allow-unknown"].Properties["name"],
			readData.Sections["test-allow-unknown"].Properties["name"])
	})
}

func TestSectionConfig_ArrayProperties(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
	}
	config := NewSectionConfig(idSchema)

	// Register a plugin with array property
	arrayPlugin := &SectionPlugin{
		FolderPath: tempDir,
		TypeName:     "array-test",
		Properties: map[string]*Schema{
			"tags": {
				Type:        TypeArray,
				Description: "Test tags",
				Required:    true,
				Items: &Schema{
					Type: TypeString,
				},
			},
		},
	}
	config.RegisterPlugin(arrayPlugin)

	t.Run("Array Property Handling", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-array")+".cfg")
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-array": {
					Type: "array-test",
					ID:   "test-array",
					Properties: map[string]string{
						"tags": "tag1,tag2,tag3",
					},
				},
			},
			Order: []string{"test-array"},
		}

		err := config.Write(testData)
		require.NoError(t, err)

		readData, err := config.Parse(testFile)
		require.NoError(t, err)

		assert.Equal(t, testData.Sections["test-array"].Properties["tags"],
			readData.Sections["test-array"].Properties["tags"])
	})
}

func TestSectionConfig_ValidationRules(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
	}
	config := NewSectionConfig(idSchema)

	// Register a plugin with validation rules
	patternStr := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	validationPlugin := &SectionPlugin{
		FolderPath: tempDir,
		TypeName:   "validation-test",
		Properties: map[string]*Schema{
			"email": {
				Type:        TypeString,
				Description: "Email address",
				Required:    true,
				Pattern:     &patternStr,
			},
		},
		Validations: []ValidationFunc{
			func(data map[string]string) error {
				if len(data["email"]) > 254 {
					return fmt.Errorf("email too long")
				}
				return nil
			},
		},
	}
	config.RegisterPlugin(validationPlugin)

	t.Run("Valid Pattern", func(t *testing.T) {
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-validate": {
					Type: "validation-test",
					ID:   "test-validate",
					Properties: map[string]string{
						"email": "test@example.com",
					},
				},
			},
			Order: []string{"test-validate"},
		}

		err := config.Write(testData)
		require.NoError(t, err)
	})

	t.Run("Invalid Pattern", func(t *testing.T) {
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-invalid": {
					Type: "validation-test",
					ID:   "test-invalid",
					Properties: map[string]string{
						"email": "not-an-email",
					},
				},
			},
			Order: []string{"test-invalid"},
		}

		err := config.Write(testData)
		assert.Error(t, err)
	})
}
