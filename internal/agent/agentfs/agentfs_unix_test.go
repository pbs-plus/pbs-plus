//go:build unix

package agentfs

import (
	"os"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
)

func TestAbsPathResolution(t *testing.T) {
	tests := []struct {
		name        string
		sourcePath  string // The original path (e.g., /home/test)
		snapPath    string // Where the snapshot is actually mounted
		request     string // The path requested by FUSE
		wantPath    string // Expected physical path
		wantVirtual bool   // Expected virtual status
		wantErr     error
	}{
		{
			name:        "Root of virtual tree",
			sourcePath:  "/home/sralmerol",
			snapPath:    "/tmp/snap_123",
			request:     "/",
			wantPath:    "",
			wantVirtual: true,
		},
		{
			name:        "Intermediate virtual directory",
			sourcePath:  "/home/sralmerol",
			snapPath:    "/tmp/snap_123",
			request:     "home",
			wantPath:    "home",
			wantVirtual: true,
		},
		{
			name:        "The snapshot entry point",
			sourcePath:  "/home/sralmerol",
			snapPath:    "/tmp/snap_123",
			request:     "home/sralmerol",
			wantPath:    "/tmp/snap_123",
			wantVirtual: false,
		},
		{
			name:        "Nested file in snapshot",
			sourcePath:  "/home/sralmerol",
			snapPath:    "/tmp/snap_123",
			request:     "home/sralmerol/documents/file.txt",
			wantPath:    "/tmp/snap_123/documents/file.txt",
			wantVirtual: false,
		},
		{
			name:        "Empty SourcePath - should map root to snap",
			sourcePath:  "",
			snapPath:    "/tmp/snap_123",
			request:     "/",
			wantPath:    "/tmp/snap_123",
			wantVirtual: false,
		},
		{
			name:        "Empty SourcePath - nested file",
			sourcePath:  "",
			snapPath:    "/tmp/snap_123",
			request:     "config.json",
			wantPath:    "/tmp/snap_123/config.json",
			wantVirtual: false,
		},
		{
			name:        "Request outside of hierarchy",
			sourcePath:  "/home/sralmerol",
			snapPath:    "/tmp/snap_123",
			request:     "etc/passwd",
			wantPath:    "",
			wantVirtual: false,
			wantErr:     os.ErrNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &AgentFSServer{
				snapshot: snapshots.Snapshot{
					SourcePath: tt.sourcePath,
					Path:       tt.snapPath,
				},
			}

			gotPath, gotVirtual, err := s.abs(tt.request)

			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("abs() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("abs() unexpected error: %v", err)
			}

			if gotPath != tt.wantPath {
				t.Errorf("abs() gotPath = %v, want %v", gotPath, tt.wantPath)
			}

			if gotVirtual != tt.wantVirtual {
				t.Errorf("abs() gotVirtual = %v, want %v", gotVirtual, tt.wantVirtual)
			}
		})
	}
}
