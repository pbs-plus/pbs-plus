//go:build linux

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rpclocker "github.com/pbs-plus/pbs-plus/internal/proxy/locker"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testDbPath     string
	testLockerPath string
	testBasePath   string
)

// TestMain handles setup and teardown for all tests
func TestMain(m *testing.M) {
	var err error
	testBasePath, err = os.MkdirTemp("", "pbs-plus-test-*")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}

	testDbPath = filepath.Join(testBasePath, "test.db")
	testLockerPath = filepath.Join(testBasePath, "test.lock")

	ctx, cancel := context.WithCancel(context.Background())
	var serverErr error
	var serverWg sync.WaitGroup // Use WaitGroup to wait for server shutdown
	serverWg.Add(1)

	go func() {
		defer serverWg.Done()
		fmt.Printf("Attempting to start locker server at %s...\n", testLockerPath)
		serverErr = rpclocker.StartLockerServer(ctx, testLockerPath)
		if serverErr != nil && !errors.Is(serverErr, context.Canceled) && !errors.Is(serverErr, net.ErrClosed) {
			fmt.Printf("Locker server exited with error: %v\n", serverErr)
		} else {
			fmt.Println("Locker server shut down.")
		}
	}()

	const serverStartTimeout = 5 * time.Second
	const pollInterval = 50 * time.Millisecond
	startTime := time.Now()

	fmt.Printf("Waiting up to %v for locker socket at %s...\n", serverStartTimeout, testLockerPath)
	socketReady := false
	for time.Since(startTime) < serverStartTimeout {
		_, statErr := os.Stat(testLockerPath)
		if statErr == nil {
			conn, dialErr := net.DialTimeout("unix", testLockerPath, 50*time.Millisecond)
			if dialErr == nil {
				_ = conn.Close() // Close the test connection immediately
				fmt.Printf("Locker socket found and connectable after %v.\n", time.Since(startTime))
				socketReady = true
				break // Socket exists and is connectable
			}
		} else if !os.IsNotExist(statErr) {
			fmt.Printf("Error checking for socket file %s: %v\n", testLockerPath, statErr)
			cancel()        // Signal server to stop
			serverWg.Wait() // Wait for server goroutine to finish
			os.RemoveAll(testBasePath)
			os.Exit(1)
		}

		if serverErr != nil {
			fmt.Printf("Server failed to start while waiting for socket: %v\n", serverErr)
			cancel()
			serverWg.Wait()
			os.RemoveAll(testBasePath)
			os.Exit(1)
		}

		time.Sleep(pollInterval)
	}

	if !socketReady {
		fmt.Printf("Timed out waiting for locker socket file %s after %v\n", testLockerPath, serverStartTimeout)
		cancel()        // Signal server to stop
		serverWg.Wait() // Wait for server goroutine to finish
		if serverErr != nil {
			fmt.Printf("Server may have failed during startup: %v\n", serverErr)
		}
		os.RemoveAll(testBasePath)
		os.Exit(1) // Exit test suite
	}

	fmt.Println("Locker server ready. Running tests...")
	code := m.Run()

	fmt.Println("Tests finished. Cleaning up...")
	cancel() // Signal server to stop
	fmt.Println("Waiting for locker server to shut down...")
	serverWg.Wait()            // Wait for the server goroutine to fully exit
	os.RemoveAll(testBasePath) // Remove temp directory

	os.Exit(code)
}

// setupTestStore creates a new store instance with temporary paths
func setupTestStore(t *testing.T) *Store {
	paths := map[string]string{
		"sqlite": testDbPath,
		"locker": testLockerPath, // Use the same locker path
	}

	err := os.RemoveAll(paths["sqlite"])
	require.NoError(t, err)

	store, err := Initialize(t.Context(), paths)
	require.NoError(t, err)

	return store
}

// Job Tests
func TestJobCRUD(t *testing.T) {
	store := setupTestStore(t)

	t.Run("Basic CRUD Operations", func(t *testing.T) {
		job := types.Job{
			ID:               "test-job-1",
			Store:            "local",
			Target:           "test-target",
			Subpath:          "backups/test",
			Schedule:         "daily",
			Comment:          "Test backup job",
			NotificationMode: "always",
			Namespace:        "test",
		}

		err := store.Database.CreateJob(nil, job)
		assert.NoError(t, err)

		// Test Get
		retrievedJob, err := store.Database.GetJob(job.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrievedJob)
		assert.Equal(t, job.ID, retrievedJob.ID)
		assert.Equal(t, job.Store, retrievedJob.Store)
		assert.Equal(t, job.Target, retrievedJob.Target)

		// Test Update
		job.Comment = "Updated comment"
		err = store.Database.UpdateJob(nil, job)
		assert.NoError(t, err)

		updatedJob, err := store.Database.GetJob(job.ID)
		assert.NoError(t, err)
		assert.Equal(t, "Updated comment", updatedJob.Comment)

		// Test GetAll
		jobs, err := store.Database.GetAllJobs()
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)

		// Test Delete
		err = store.Database.DeleteJob(nil, job.ID)
		assert.NoError(t, err)

		_, err = store.Database.GetJob(job.ID)
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		var wg sync.WaitGroup
		jobCount := 10

		// Concurrent creation
		for i := 0; i < jobCount; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				job := types.Job{
					ID:               fmt.Sprintf("concurrent-job-%d", idx),
					Store:            "local",
					Target:           "test-target",
					Subpath:          fmt.Sprintf("backups/test-%d", idx),
					Schedule:         `mon..fri *-*-* 00:00:00`,
					Comment:          fmt.Sprintf("Concurrent test job %d", idx),
					NotificationMode: "always",
					Namespace:        "test",
				}
				err := store.Database.CreateJob(nil, job)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all jobs were created
		jobs, err := store.Database.GetAllJobs()
		assert.NoError(t, err)
		assert.Len(t, jobs, jobCount)
	})

	t.Run("Special Characters", func(t *testing.T) {
		job := types.Job{
			ID:               "test-job-special-!@#$%^",
			Store:            "local",
			Target:           "test-target",
			Subpath:          "backups/test/special/!@#$%^",
			Schedule:         `mon..fri *-*-* 00:00:00`,
			Comment:          "Test job with special characters !@#$%^",
			NotificationMode: "always",
			Namespace:        "test",
		}
		err := store.Database.CreateJob(nil, job)
		assert.Error(t, err) // Should reject special characters
	})
}

func TestJobValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name    string
		job     types.Job
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid job with all fields",
			job: types.Job{
				ID:               "test-valid",
				Store:            "local",
				Target:           "test",
				Subpath:          "valid/path",
				Schedule:         `*-*-* 00:00:00`,
				Comment:          "Valid test job",
				NotificationMode: "always",
				Namespace:        "test",
			},
			wantErr: false,
		},
		{
			name: "invalid schedule string",
			job: types.Job{
				ID:        "test-invalid-cron",
				Store:     "local",
				Target:    "test",
				Schedule:  "invalid-cron",
				Namespace: "test",
			},
			wantErr: true,
			errMsg:  "invalid schedule string",
		},
		{
			name: "empty required fields",
			job: types.Job{
				ID: "test-empty",
			},
			wantErr: true,
			errMsg:  "is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Database.CreateJob(nil, tt.job)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTargetValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name    string
		target  types.Target
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid local target",
			target: types.Target{
				Name: "local-target",
				Path: "/valid/path",
			},
			wantErr: false,
		},
		{
			name: "valid agent target",
			target: types.Target{
				Name: "agent-target",
				Path: "agent://192.168.1.100/C",
			},
			wantErr: false,
		},
		{
			name: "invalid agent URL",
			target: types.Target{
				Name: "invalid-agent",
				Path: "agent:/invalid-url",
			},
			wantErr: true,
			errMsg:  "invalid target path",
		},
		{
			name: "empty path",
			target: types.Target{
				Name: "empty-path",
				Path: "",
			},
			wantErr: true,
			errMsg:  "empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Database.CreateTarget(nil, tt.target)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExclusionPatternValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name      string
		exclusion types.Exclusion
		wantErr   bool
	}{
		{
			name: "valid glob pattern",
			exclusion: types.Exclusion{
				Path:    "*.tmp",
				Comment: "Temporary files",
			},
			wantErr: false,
		},
		{
			name: "valid regex pattern",
			exclusion: types.Exclusion{
				Path:    "^.*\\.bak$",
				Comment: "Backup files",
			},
			wantErr: false,
		},
		{
			name: "invalid pattern syntax",
			exclusion: types.Exclusion{
				Path:    "[invalid[pattern",
				Comment: "Invalid pattern",
			},
			wantErr: true,
		},
		{
			name: "empty pattern",
			exclusion: types.Exclusion{
				Path:    "",
				Comment: "Empty pattern",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Database.CreateExclusion(nil, tt.exclusion)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConcurrentOperations(t *testing.T) {
	store := setupTestStore(t)
	var wg sync.WaitGroup

	t.Run("Concurrent Target Operations", func(t *testing.T) {
		targetCount := 10
		for i := 0; i < targetCount; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				target := types.Target{
					Name: fmt.Sprintf("concurrent-target-%d", idx),
					Path: fmt.Sprintf("/path/to/target-%d", idx),
				}
				err := store.Database.CreateTarget(nil, target)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all targets were created
		targets, err := store.Database.GetAllTargets()
		assert.NoError(t, err)
		assert.Len(t, targets, targetCount)
	})

	t.Run("Concurrent Read/Write Operations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const opCount = 100
		readyCh := make(chan struct{})
		doneCh := make(chan struct{})

		// Writer goroutine
		go func() {
			<-readyCh
			for i := 0; i < opCount; i++ {
				select {
				case <-ctx.Done():
					return
				default:
					target := types.Target{
						Name: fmt.Sprintf("concurrent-target-%d", i),
						Path: fmt.Sprintf("/path/to/target-%d", i),
					}
					_ = store.Database.CreateTarget(nil, target)
				}
			}
			doneCh <- struct{}{}
		}()

		// Reader goroutine
		go func() {
			<-readyCh
			for i := 0; i < opCount; i++ {
				select {
				case <-ctx.Done():
					return
				default:
					_, _ = store.Database.GetAllTargets()
				}
			}
			doneCh <- struct{}{}
		}()

		close(readyCh)

		// Wait with timeout
		for i := 0; i < 2; i++ {
			select {
			case <-doneCh:
				continue
			case <-ctx.Done():
				t.Fatal("Test timed out")
			}
		}
	})
}
