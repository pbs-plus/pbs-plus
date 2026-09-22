package targetplugin

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

// MaxProtocolPayloadBytes matches the local aRPC control-envelope limit.
const MaxProtocolPayloadBytes = arpc.DefaultLocalMessageLimit

// ErrNonCanonicalProtocolPayload reports CBOR that has more than one wire representation.
var ErrNonCanonicalProtocolPayload = errors.New("non-canonical plugin protocol payload")

var (
	protocolEncMode = newProtocolEncMode()
	protocolDecMode = newProtocolDecMode()
)

// MarshalProtocol encodes a canonical CBOR plugin method payload.
func MarshalProtocol(value any) ([]byte, error) {
	data, err := protocolEncMode.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode plugin protocol payload: %w", err)
	}
	if int64(len(data)) >= MaxProtocolPayloadBytes {
		return nil, arpc.ErrMessageTooLarge
	}
	return data, nil
}

// UnmarshalProtocol strictly decodes a canonical CBOR plugin method payload.
func UnmarshalProtocol(data []byte, value any) error {
	if int64(len(data)) >= MaxProtocolPayloadBytes {
		return arpc.ErrMessageTooLarge
	}
	if err := protocolDecMode.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode plugin protocol payload: %w", err)
	}
	canonical, err := protocolEncMode.Marshal(value)
	if err != nil {
		return fmt.Errorf("re-encode plugin protocol payload: %w", err)
	}
	if !bytes.Equal(data, canonical) {
		return ErrNonCanonicalProtocolPayload
	}
	return nil
}

func newProtocolEncMode() cbor.EncMode {
	options := cbor.CanonicalEncOptions()
	options.TagsMd = cbor.TagsForbidden
	mode, err := options.EncMode()
	if err != nil {
		panic(err)
	}
	return mode
}

func newProtocolDecMode() cbor.DecMode {
	mode, err := (cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		MaxNestedLevels:   16,
		MaxArrayElements:  1024,
		MaxMapPairs:       256,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
		FieldNameMatching: cbor.FieldNameMatchingCaseSensitive,
		NaN:               cbor.NaNDecodeForbidden,
		Inf:               cbor.InfDecodeForbidden,
	}).DecMode()
	if err != nil {
		panic(err)
	}
	return mode
}
