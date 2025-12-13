//go:build linux

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sqlite "github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testDbPath string
)

func TestMain(m *testing.M) {
	var err error
	testBasePath, err := os.MkdirTemp("", "pbs-plus-test-*")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}

	testDbPath = filepath.Join(testBasePath, "test.db")

	code := m.Run()

	os.RemoveAll(testBasePath)

	os.Exit(code)
}

func setupTestStore(t *testing.T) *Store {
	err := os.RemoveAll(testDbPath)
	require.NoError(t, err)

	paths := map[string]string{
		"sqlite": testDbPath,
	}

	store, err := Initialize(t.Context(), paths)
	require.NoError(t, err)

	return store
}

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

		retrievedJob, err := store.Database.GetJob(job.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrievedJob)
		assert.Equal(t, job.ID, retrievedJob.ID)
		assert.Equal(t, job.Store, retrievedJob.Store)
		assert.Equal(t, job.Target, retrievedJob.Target)

		job.Comment = "Updated comment"
		err = store.Database.UpdateJob(nil, job)
		assert.NoError(t, err)

		updatedJob, err := store.Database.GetJob(job.ID)
		assert.NoError(t, err)
		assert.Equal(t, "Updated comment", updatedJob.Comment)

		jobs, err := store.Database.GetAllJobs()
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)

		err = store.Database.DeleteJob(nil, job.ID)
		assert.NoError(t, err)

		_, err = store.Database.GetJob(job.ID)
		assert.ErrorIs(t, err, sqlite.ErrJobNotFound)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		var wg sync.WaitGroup
		jobCount := 10

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
		assert.Error(t, err)
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
				Name:       "local-target",
				TargetType: "local",
				LocalPath:  "/valid/path",
			},
			wantErr: false,
		},
		{
			name: "valid agent target",
			target: types.Target{
				Name:       "agent-target",
				TargetType: "agent",
				AgentHost:  "192.168.1.100",
				Volumes: []types.Volume{
					{VolumeName: "C"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty path",
			target: types.Target{
				Name:       "empty-path",
				LocalPath:  "",
				TargetType: "local",
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

func TestTargetValidation_S3(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name    string
		target  types.Target
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid s3 target with https host",
			target: types.Target{
				Name:           "s3-valid-https",
				TargetType:     "s3",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "https://s3.amazonaws.com",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "my-valid.bucket-name-01",
			},
			wantErr: false,
		},
		{
			name: "valid s3 target with plain host and http",
			target: types.Target{
				Name:           "s3-valid-http",
				TargetType:     "s3",
				S3AccessID:     "MINIOEXAMPLE",
				S3Host:         "minio.local:9000",
				S3Region:       "us-east-1",
				S3UseSSL:       false,
				S3UsePathStyle: true,
				S3Bucket:       "backup-bucket",
			},
			wantErr: false,
		},
		{
			name: "invalid s3 bucket name",
			target: types.Target{
				Name:           "s3-invalid-bucket",
				TargetType:     "s3",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "s3.us-west-2.amazonaws.com",
				S3Region:       "us-west-2",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "Invalid_Bucket_Name",
			},
			wantErr: true,
			errMsg:  "invalid s3 bucket",
		},
		{
			name: "missing s3 access id",
			target: types.Target{
				Name:           "s3-missing-access",
				TargetType:     "s3",
				S3AccessID:     "",
				S3Host:         "s3.amazonaws.com",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "good-bucket-name",
			},
			wantErr: true,
			errMsg:  "s3 access id is empty",
		},
		{
			name: "missing s3 host",
			target: types.Target{
				Name:           "s3-missing-host",
				TargetType:     "s3",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "good-bucket-name",
			},
			wantErr: true,
			errMsg:  "s3 host is empty",
		},
		{
			name: "missing s3 bucket",
			target: types.Target{
				Name:           "s3-missing-bucket",
				TargetType:     "s3",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "s3.amazonaws.com",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "",
			},
			wantErr: true,
			errMsg:  "s3 bucket is empty",
		},
		{
			name: "reject agent fields on s3",
			target: types.Target{
				Name:           "s3-with-agent-host",
				TargetType:     "s3",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "s3.amazonaws.com",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
				S3Bucket:       "good-bucket-name",
				AgentHost:      "should-not-be-here",
			},
			wantErr: true,
			errMsg:  "agent host provided for non-agent target",
		},
		{
			name: "reject s3 fields on non-s3 target",
			target: types.Target{
				Name:           "local-with-s3-fields",
				TargetType:     "local",
				LocalPath:      "/data",
				S3AccessID:     "AKIAEXAMPLE",
				S3Host:         "s3.amazonaws.com",
				S3Bucket:       "bucket",
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3UsePathStyle: false,
			},
			wantErr: true,
			errMsg:  "s3 fields provided for non-s3 target",
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
				got, getErr := store.Database.GetTarget(tt.target.Name)
				assert.NoError(t, getErr)
				assert.Equal(t, tt.target.Name, got.Name)
				assert.Equal(t, "s3", got.TargetType)
				assert.Equal(t, tt.target.S3Bucket, got.S3Bucket)
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
					Name:       fmt.Sprintf("concurrent-target-%d", idx),
					LocalPath:  fmt.Sprintf("/path/to/target-%d", idx),
					TargetType: "local",
				}
				err := store.Database.CreateTarget(nil, target)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

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

		go func() {
			<-readyCh
			for i := 0; i < opCount; i++ {
				select {
				case <-ctx.Done():
					return
				default:
					target := types.Target{
						Name:       fmt.Sprintf("concurrent-target-%d", i),
						LocalPath:  fmt.Sprintf("/path/to/target-%d", i),
						TargetType: "local",
					}
					_ = store.Database.CreateTarget(nil, target)
				}
			}
			doneCh <- struct{}{}
		}()

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
