package targetplugin

import (
	"strings"
	"testing"
)

func TestDescriptorValidate(t *testing.T) {
	tests := []struct {
		name       string
		descriptor Descriptor
		wantError  string
	}{
		{
			name: "valid",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
		},
		{
			name: "protocol",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion + 1,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			wantError: "unsupported plugin protocol",
		},
		{
			name: "identity",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "Org.PBS-Plus.Filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			wantError: "invalid character",
		},
		{
			name: "version",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "latest",
				TargetTypes:     []string{"filesystem"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			wantError: "invalid plugin version",
		},
		{
			name: "duplicate target type",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem", "filesystem"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			wantError: "duplicate target type",
		},
		{
			name: "missing target schema",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem"},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			wantError: "target schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.descriptor.Validate()
			if test.wantError == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}
