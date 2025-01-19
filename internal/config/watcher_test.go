package config

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigWatcher_BasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
	}
	config := NewSectionConfig(idSchema)

	testPlugin := &SectionPlugin{
		FolderPath: tempDir,
		TypeName:   "test",
		Properties: map[string]*Schema{
			"value": {
				Type:        TypeString,
				Description: "Test value",
				Required:    true,
			},
		},
	}
	config.RegisterPlugin(testPlugin)

	t.Run("Watch File Creation", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-create")+".cfg")
		var mu sync.Mutex
		var capturedConfig *ConfigData

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		wg := sync.WaitGroup{}
		wg.Add(1)

		watcher, err := NewConfigWatcher(config, func(data *ConfigData) {
			mu.Lock()
			capturedConfig = data
			mu.Unlock()
			wg.Done()
		})
		require.NoError(t, err)
		defer watcher.Close()

		err = watcher.Watch(testFile)
		require.NoError(t, err)

		// Create test data
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-create": {
					Type: "test",
					ID:   "test-create",
					Properties: map[string]string{
						"value": "test",
					},
				},
			},
			Order: []string{"test-create"},
		}

		err = config.Write(testData)
		require.NoError(t, err)

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for watcher callback")
		case <-done:
			mu.Lock()
			assert.NotNil(t, capturedConfig)
			assert.Equal(t, testData.Sections["test-create"].Properties["value"],
				capturedConfig.Sections["test-create"].Properties["value"])
			mu.Unlock()
		}
	})

	t.Run("Watch File Modification", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-mod")+".cfg")
		var mu sync.Mutex
		var capturedConfig *ConfigData
		var callCount int

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		wg := sync.WaitGroup{}
		wg.Add(2) // One for initial write, one for modification

		watcher, err := NewConfigWatcher(config, func(data *ConfigData) {
			mu.Lock()
			capturedConfig = data
			callCount++
			mu.Unlock()
			wg.Done()
		})
		require.NoError(t, err)
		defer watcher.Close()

		err = watcher.Watch(testFile)
		require.NoError(t, err)

		// Initial write
		testData := &ConfigData{
			Sections: map[string]*Section{
				"test-mod": {
					Type: "test",
					ID:   "test-mod",
					Properties: map[string]string{
						"value": "initial",
					},
				},
			},
			Order: []string{"test-mod"},
		}

		err = config.Write(testData)
		require.NoError(t, err)

		// Wait for initial callback
		time.Sleep(100 * time.Millisecond)

		// Modify
		testData.Sections["test-mod"].Properties["value"] = "modified"
		err = config.Write(testData)
		require.NoError(t, err)

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for watcher callbacks")
		case <-done:
			mu.Lock()
			assert.Equal(t, 2, callCount)
			assert.Equal(t, "modified", capturedConfig.Sections["test-mod"].Properties["value"])
			mu.Unlock()
		}
	})
}

func TestConfigWatcher_Debouncing(t *testing.T) {
	tempDir := t.TempDir()
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
	}
	config := NewSectionConfig(idSchema)

	testPlugin := &SectionPlugin{
		FolderPath: tempDir,
		TypeName:   "test",
		Properties: map[string]*Schema{
			"value": {
				Type:        TypeString,
				Description: "Test value",
				Required:    true,
			},
		},
	}
	config.RegisterPlugin(testPlugin)

	t.Run("Rapid Changes", func(t *testing.T) {
		testFile := filepath.Join(tempDir, utils.EncodePath("test-rapid")+".cfg")
		var mu sync.Mutex
		callCount := 0

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		wg := sync.WaitGroup{}
		wg.Add(1) // We expect only one callback due to debouncing

		watcher, err := NewConfigWatcher(config, func(data *ConfigData) {
			mu.Lock()
			callCount++
			mu.Unlock()
			wg.Done()
		})
		require.NoError(t, err)
		defer watcher.Close()

		err = watcher.Watch(testFile)
		require.NoError(t, err)

		// Make rapid changes
		for i := 0; i < 5; i++ {
			testData := &ConfigData{
				Sections: map[string]*Section{
					"test-rapid": {
						Type: "test",
						ID:   "test-rapid",
						Properties: map[string]string{
							"value": "test",
						},
					},
				},
				Order: []string{"test-rapid"},
			}
			err = config.Write(testData)
			require.NoError(t, err)
			time.Sleep(10 * time.Millisecond)
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for watcher callback")
		case <-done:
			mu.Lock()
			assert.Equal(t, 1, callCount, "Should receive only one callback due to debouncing")
			mu.Unlock()
		}
	})
}

func TestConfigWatcher_ErrorHandling(t *testing.T) {
	tempDir := t.TempDir()
	idSchema := &Schema{
		Type:        TypeString,
		Description: "Test ID",
		Required:    true,
	}
	config := NewSectionConfig(idSchema)

	t.Run("Invalid File Path", func(t *testing.T) {
		watcher, err := NewConfigWatcher(config, func(data *ConfigData) {})
		require.NoError(t, err)
		defer watcher.Close()

		err = watcher.Watch("/nonexistent/path/that/cannot/exist")
		assert.Error(t, err)
	})

	t.Run("Invalid Config Content", func(t *testing.T) {
		testFile := filepath.Join(tempDir, "invalid.conf")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		callCount := 0
		wg := sync.WaitGroup{}
		wg.Add(1)

		watcher, err := NewConfigWatcher(config, func(data *ConfigData) {
			callCount++
			wg.Done()
		})
		require.NoError(t, err)
		defer watcher.Close()

		err = watcher.Watch(testFile)
		require.NoError(t, err)

		// Write invalid content
		err = os.WriteFile(testFile, []byte("invalid:config:content"), 0644)
		require.NoError(t, err)

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			assert.Equal(t, 0, callCount, "Should not receive callback for invalid content")
		case <-done:
			t.Fatal("should not receive callback for invalid content")
		}
	})
}
