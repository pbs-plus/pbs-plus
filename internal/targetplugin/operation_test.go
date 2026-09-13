package targetplugin

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestOperationValidate(t *testing.T) {
	tests := []struct {
		name      string
		operation Operation
		wantError string
	}{
		{
			name: "target operation",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
				TargetType:        "filesystem",
				SchemaVersion:     1,
			},
		},
		{
			name: "plugin operation",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
			},
		},
		{
			name: "protocol",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion + 1,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
			},
			wantError: "unsupported plugin protocol",
		},
		{
			name: "missing ID",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
			},
			wantError: "operation ID is required",
		},
		{
			name: "missing deadline",
			operation: Operation{
				ProtocolVersion: CurrentProtocolVersion,
				ID:              "op-1",
				IdempotencyKey:  "retry-1",
				PluginVersion:   "1.0.0",
			},
			wantError: "operation deadline is required",
		},
		{
			name: "invalid broker token",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
				BrokerToken:       []byte("short"),
			},
			wantError: "broker token must be 32 bytes",
		},
		{
			name: "schema without target",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
				SchemaVersion:     1,
			},
			wantError: "schema version requires a target type",
		},
		{
			name: "target without schema",
			operation: Operation{
				ProtocolVersion:   CurrentProtocolVersion,
				ID:                "op-1",
				IdempotencyKey:    "retry-1",
				DeadlineUnixMilli: 1700000000000,
				PluginVersion:     "1.0.0",
				TargetType:        "filesystem",
			},
			wantError: "target schema version is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.operation.Validate()
			if test.wantError == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestProtocolErrorValidate(t *testing.T) {
	tests := []struct {
		name          string
		protocolError ProtocolError
		wantError     string
	}{
		{
			name:          "invalid request",
			protocolError: ProtocolError{Code: ErrorInvalidRequest, Message: "port is required"},
		},
		{
			name:          "retry delay",
			protocolError: ProtocolError{Code: ErrorUnavailable, Message: "service unavailable", RetryAfterMillis: 1000},
		},
		{
			name:          "unknown code",
			protocolError: ProtocolError{Code: "failed", Message: "failed"},
			wantError:     "unsupported plugin error code",
		},
		{
			name:          "empty message",
			protocolError: ProtocolError{Code: ErrorInternal},
			wantError:     "plugin error message is required",
		},
		{
			name:          "retry delay on conflict",
			protocolError: ProtocolError{Code: ErrorConflict, Message: "already exists", RetryAfterMillis: 1000},
			wantError:     "retry delay is only valid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.protocolError.Validate()
			if test.wantError == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestOperationCBORFixture(t *testing.T) {
	encoded, err := MarshalProtocol(Operation{
		ProtocolVersion:   CurrentProtocolVersion,
		ID:                "op-1",
		IdempotencyKey:    "retry-1",
		DeadlineUnixMilli: 1700000000000,
		PluginVersion:     "1.0.0",
		TargetType:        "test",
		SchemaVersion:     1,
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	const want = "a7626964646f702d316b7461726765745f7479706564746573746e706c7567696e5f76657273696f6e65312e302e306e736368656d615f76657273696f6e016f6964656d706f74656e63795f6b65796772657472792d317070726f746f636f6c5f76657273696f6e0173646561646c696e655f756e69785f6d696c6c691b0000018bcfe56800"
	if got := hex.EncodeToString(encoded); got != want {
		t.Fatalf("operation fixture = %s, want %s", got, want)
	}
}
