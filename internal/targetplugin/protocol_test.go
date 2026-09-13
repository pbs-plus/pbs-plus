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
			},
		},
		{
			name: "protocol",
			descriptor: Descriptor{
				ProtocolVersion: CurrentProtocolVersion + 1,
				PluginID:        "org.pbs-plus.filesystem",
				Version:         "1.0.0",
				TargetTypes:     []string{"filesystem"},
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
			},
			wantError: "duplicate target type",
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
