//go:build linux

package backup

import "testing"

func TestDetermine(t *testing.T) {
	tests := []struct {
		name string
		cfg  DeterminationConfig
		want BackupStatus
	}{
		{
			name: "clean success with PBS confirmation",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ClientEv: ClientLogEvidence{HasDuration: true, HasEndTime: true},
				ProxyEv:  ProxyLogEvidence{HasCompletionMarker: true},
			},
			want: StatusOK,
		},
		{
			name: "spurious connection error race - client exit 0",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ClientEv: ClientLogEvidence{HasDuration: true, HasEndTime: true},
				ProxyEv: ProxyLogEvidence{
					TaskErrors:                 []string{"connection error: not connected"},
					HasSpuriousConnectionError: true,
					HasOnlySpuriousErrors:      true,
				},
				AgentConnected: false,
			},
			want: StatusOK,
		},
		{
			name: "spurious connection error race - client exit non-zero",
			cfg: DeterminationConfig{
				ExitCode: 1,
				ClientEv: ClientLogEvidence{HasDuration: true, HasEndTime: true},
				ProxyEv: ProxyLogEvidence{
					TaskErrors:                 []string{"connection error: not connected"},
					HasSpuriousConnectionError: true,
					HasOnlySpuriousErrors:      true,
				},
			},
			want: StatusOK,
		},
		{
			name: "real failure - non-zero exit no client completion",
			cfg: DeterminationConfig{
				ExitCode: 1,
				ProxyEv: ProxyLogEvidence{
					TaskErrors: []string{"some real error"},
				},
			},
			want: StatusFailed,
		},
		{
			name: "real failure - non-zero exit with real errors",
			cfg: DeterminationConfig{
				ExitCode: 1,
				ProxyEv: ProxyLogEvidence{
					TaskErrors:                 []string{"connection error: not connected", "backup failed: disk full"},
					HasSpuriousConnectionError: true,
					HasOnlySpuriousErrors:      false,
				},
			},
			want: StatusFailed,
		},
		{
			name: "client exit 0 no Duration - PBS confirms OK",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ProxyEv:  ProxyLogEvidence{HasCompletionMarker: true},
			},
			want: StatusOK,
		},
		{
			name: "client exit 0 no Duration - no PBS confirmation",
			cfg: DeterminationConfig{
				ExitCode: 0,
			},
			want: StatusFailed,
		},
		{
			name: "canceled",
			cfg: DeterminationConfig{
				Canceled: true,
			},
			want: StatusCanceled,
		},
		{
			name: "canceled overrides everything",
			cfg: DeterminationConfig{
				Canceled: true,
				ExitCode: 0,
				ClientEv: ClientLogEvidence{HasDuration: true, HasEndTime: true},
			},
			want: StatusCanceled,
		},
		{
			name: "success with warnings",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ClientEv: ClientLogEvidence{
					HasDuration:  true,
					HasEndTime:   true,
					WarningCount: 3,
				},
				ProxyEv: ProxyLogEvidence{HasCompletionMarker: true},
			},
			want: StatusWarnings,
		},
		{
			name: "success with upload errors counted as warnings",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ClientEv: ClientLogEvidence{
					HasDuration:  true,
					HasEndTime:   true,
					UploadErrors: []string{"upload failed: chunk 12345"},
				},
			},
			want: StatusWarnings,
		},
		{
			name: "client TASK ERROR in stdout becomes warning not failure",
			cfg: DeterminationConfig{
				ExitCode: 0,
				ClientEv: ClientLogEvidence{
					HasDuration:  true,
					HasEndTime:   true,
					HasTaskError: true,
				},
			},
			want: StatusWarnings,
		},
		{
			name: "non-zero exit with client completion but real proxy errors",
			cfg: DeterminationConfig{
				ExitCode: 1,
				ClientEv: ClientLogEvidence{HasDuration: true, HasEndTime: true},
				ProxyEv: ProxyLogEvidence{
					TaskErrors:            []string{"backup failed: no space left on device"},
					HasOnlySpuriousErrors: false,
				},
			},
			want: StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Determine(tt.cfg)
			if got != tt.want {
				t.Errorf("Determine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExitCodeFromErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil error", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exitCodeFromErr(tt.err)
			if got != tt.want {
				t.Errorf("exitCodeFromErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
