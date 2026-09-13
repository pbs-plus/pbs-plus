package targetplugin

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

func TestProtocolCBORFixtures(t *testing.T) {
	fixtures := []struct {
		name  string
		value any
		hex   string
	}{
		{
			name:  "describe request",
			value: DescribeRequest{ProtocolVersion: CurrentProtocolVersion},
			hex:   "a17070726f746f636f6c5f76657273696f6e01",
		},
		{
			name: "descriptor",
			value: Descriptor{
				ProtocolVersion: CurrentProtocolVersion,
				PluginID:        "org.pbs-plus.test",
				Version:         "1.0.0",
				TargetTypes:     []string{"test"},
				TargetSchema:    FormSchema{Version: 1},
				BackupSchema:    FormSchema{Version: 1},
				RestoreSchema:   FormSchema{Version: 1},
			},
			hex: "a76776657273696f6e65312e302e3069706c7567696e5f6964716f72672e7062732d706c75732e746573746c7461726765745f74797065738164746573746d6261636b75705f736368656d61a2666669656c6473f66776657273696f6e016d7461726765745f736368656d61a2666669656c6473f66776657273696f6e016e726573746f72655f736368656d61a2666669656c6473f66776657273696f6e017070726f746f636f6c5f76657273696f6e01",
		},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			encoded, err := MarshalProtocol(fixture.value)
			if err != nil {
				t.Fatalf("MarshalProtocol: %v", err)
			}
			if got := hex.EncodeToString(encoded); got != fixture.hex {
				t.Fatalf("encoded fixture = %s, want %s", got, fixture.hex)
			}
		})
	}
}

func TestUnmarshalProtocolRejectsInvalidCBOR(t *testing.T) {
	canonical, err := hex.DecodeString("a17070726f746f636f6c5f76657273696f6e01")
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	unknown, err := MarshalProtocol(struct {
		ProtocolVersion uint16 `cbor:"protocol_version"`
		Unknown         bool   `cbor:"unknown"`
	}{ProtocolVersion: CurrentProtocolVersion, Unknown: true})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}

	tests := []struct {
		name      string
		data      []byte
		wantError string
	}{
		{
			name:      "non-canonical integer",
			data:      append(append([]byte{}, canonical[:len(canonical)-1]...), 0x18, 0x01),
			wantError: ErrNonCanonicalProtocolPayload.Error(),
		},
		{
			name:      "indefinite map",
			data:      append(append([]byte{0xbf}, canonical[1:]...), 0xff),
			wantError: "indefinite-length",
		},
		{
			name:      "tag",
			data:      append([]byte{0xc0}, canonical...),
			wantError: "tag",
		},
		{
			name:      "unknown field",
			data:      unknown,
			wantError: "unknown field",
		},
		{
			name: "duplicate field",
			data: append(append(append([]byte{0xa2}, canonical[1:len(canonical)-1]...), 0x01),
				append(append([]byte{}, canonical[1:len(canonical)-1]...), 0x01)...),
			wantError: "duplicate map key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var request DescribeRequest
			err := UnmarshalProtocol(test.data, &request)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("UnmarshalProtocol error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestProtocolPayloadLimit(t *testing.T) {
	var value []byte
	if err := UnmarshalProtocol(make([]byte, MaxProtocolPayloadBytes), &value); !errors.Is(err, arpc.ErrMessageTooLarge) {
		t.Fatalf("UnmarshalProtocol error = %v, want ErrMessageTooLarge", err)
	}
	if _, err := MarshalProtocol(make([]byte, MaxProtocolPayloadBytes)); !errors.Is(err, arpc.ErrMessageTooLarge) {
		t.Fatalf("MarshalProtocol error = %v, want ErrMessageTooLarge", err)
	}
}
