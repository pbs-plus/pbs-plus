package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

type ConfigWatcher struct {
	mu            sync.Mutex
	watcher       *fsnotify.Watcher
	config        *SectionConfig
	callback      WatchCallback
	debounceTimer *time.Timer
	watching      map[string]bool
	events        map[string]bool
}

func NewConfigWatcher(config *SectionConfig, callback WatchCallback) (*ConfigWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	return &ConfigWatcher{
		watcher:  watcher,
		config:   config,
		callback: callback,
		watching: make(map[string]bool),
		events:   make(map[string]bool),
	}, nil
}

func (w *ConfigWatcher) Watch(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	absPath, err := filepath.Abs(filename)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	if w.watching[absPath] {
		return nil
	}

	// Create parent directory if it doesn't exist
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create empty file if it doesn't exist
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.WriteFile(absPath, []byte{}, 0644); err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
	}

	// Watch the directory for file creation/deletion
	if err := w.watcher.Add(dir); err != nil {
		return fmt.Errorf("failed to watch directory: %w", err)
	}

	// Watch the file for modifications
	if err := w.watcher.Add(absPath); err != nil {
		return fmt.Errorf("failed to watch file: %w", err)
	}

	w.watching[absPath] = true

	go w.watchLoop(absPath)

	return nil
}

func (w *ConfigWatcher) watchLoop(filename string) {
	const debounceInterval = 100 * time.Millisecond

	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}

			w.mu.Lock()

			// If the file was recreated, reattach the watcher
			if event.Op&fsnotify.Create == fsnotify.Create {
				if event.Name == filename {
					_ = w.watcher.Add(filename)
				}
			}

			// Track unique events
			w.events[event.Name] = true

			if w.debounceTimer != nil {
				w.debounceTimer.Stop()
			}

			// Create a new events map for the next debounce cycle
			currentEvents := make(map[string]bool)
			for k, v := range w.events {
				currentEvents[k] = v
			}
			w.events = make(map[string]bool)

			w.debounceTimer = time.AfterFunc(debounceInterval, func() {
				if _, exists := currentEvents[filename]; exists {
					w.handleConfigChange(filename)
				}
			})

			w.mu.Unlock()

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			fmt.Printf("Watcher error: %v\n", err)
		}
	}
}

func (w *ConfigWatcher) handleConfigChange(filename string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	configData, err := w.config.Parse(filename)
	if err != nil {
		fmt.Printf("Error parsing updated config: %v\n", err)
		return
	}

	if w.callback != nil {
		w.callback(configData)
	}
}

func (w *ConfigWatcher) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.debounceTimer != nil {
		w.debounceTimer.Stop()
	}

	return w.watcher.Close()
}
