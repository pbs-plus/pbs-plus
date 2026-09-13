package targetplugin

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ScalarKind identifies a supported form value type.
type ScalarKind uint8

const (
	ScalarUnset ScalarKind = iota
	ScalarString
	ScalarInteger
	ScalarBoolean
)

// Scalar is a TOML and CBOR string, integer, or boolean value.
type Scalar struct {
	kind    ScalarKind
	text    string
	integer int64
	boolean bool
}

// NewStringScalar constructs a string form value.
func NewStringScalar(value string) Scalar {
	return Scalar{kind: ScalarString, text: value}
}

// NewIntegerScalar constructs an integer form value.
func NewIntegerScalar(value int64) Scalar {
	return Scalar{kind: ScalarInteger, integer: value}
}

// NewBooleanScalar constructs a boolean form value.
func NewBooleanScalar(value bool) Scalar {
	return Scalar{kind: ScalarBoolean, boolean: value}
}

// Kind returns the scalar's wire type.
func (scalar Scalar) Kind() ScalarKind {
	return scalar.kind
}

// StringValue returns the scalar string when its type matches.
func (scalar Scalar) StringValue() (string, bool) {
	return scalar.text, scalar.kind == ScalarString
}

// IntegerValue returns the scalar integer when its type matches.
func (scalar Scalar) IntegerValue() (int64, bool) {
	return scalar.integer, scalar.kind == ScalarInteger
}

// BooleanValue returns the scalar boolean when its type matches.
func (scalar Scalar) BooleanValue() (bool, bool) {
	return scalar.boolean, scalar.kind == ScalarBoolean
}

// Equal reports whether two scalars have the same type and value.
func (scalar Scalar) Equal(other Scalar) bool {
	if scalar.kind != other.kind {
		return false
	}
	switch scalar.kind {
	case ScalarString:
		return scalar.text == other.text
	case ScalarInteger:
		return scalar.integer == other.integer
	case ScalarBoolean:
		return scalar.boolean == other.boolean
	default:
		return false
	}
}

func (scalar Scalar) validate() error {
	switch scalar.kind {
	case ScalarString:
		return validateScalarString(scalar.text)
	case ScalarInteger, ScalarBoolean:
		return nil
	default:
		return errors.New("form scalar is unset")
	}
}

// MarshalTOML emits the scalar as its native TOML type.
func (scalar Scalar) MarshalTOML() ([]byte, error) {
	if err := scalar.validate(); err != nil {
		return nil, err
	}
	switch scalar.kind {
	case ScalarString:
		return []byte(strconv.Quote(scalar.text)), nil
	case ScalarInteger:
		return []byte(strconv.FormatInt(scalar.integer, 10)), nil
	case ScalarBoolean:
		return []byte(strconv.FormatBool(scalar.boolean)), nil
	default:
		return nil, errors.New("form scalar is unset")
	}
}

// UnmarshalTOML accepts only native TOML strings, integers, and booleans.
func (scalar *Scalar) UnmarshalTOML(value any) error {
	if scalar == nil {
		return errors.New("form scalar is nil")
	}
	return scalar.assign(value)
}

// MarshalCBOR emits the scalar as its native CBOR type.
func (scalar Scalar) MarshalCBOR() ([]byte, error) {
	if err := scalar.validate(); err != nil {
		return nil, err
	}
	switch scalar.kind {
	case ScalarString:
		return protocolEncMode.Marshal(scalar.text)
	case ScalarInteger:
		return protocolEncMode.Marshal(scalar.integer)
	case ScalarBoolean:
		return protocolEncMode.Marshal(scalar.boolean)
	default:
		return nil, errors.New("form scalar is unset")
	}
}

// UnmarshalCBOR accepts only native CBOR strings, integers, and booleans.
func (scalar *Scalar) UnmarshalCBOR(data []byte) error {
	if scalar == nil {
		return errors.New("form scalar is nil")
	}
	var value any
	if err := protocolDecMode.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode form scalar: %w", err)
	}
	return scalar.assign(value)
}

func (scalar *Scalar) assign(value any) error {
	switch value := value.(type) {
	case string:
		if err := validateScalarString(value); err != nil {
			return err
		}
		*scalar = NewStringScalar(value)
	case int64:
		*scalar = NewIntegerScalar(value)
	case uint64:
		if value > math.MaxInt64 {
			return errors.New("form scalar integer exceeds int64")
		}
		*scalar = NewIntegerScalar(int64(value))
	case bool:
		*scalar = NewBooleanScalar(value)
	default:
		return fmt.Errorf("unsupported form scalar type %T", value)
	}
	return nil
}

func validateScalarString(value string) error {
	if len(value) > maxFormValueBytes {
		return fmt.Errorf("form scalar exceeds %d bytes", maxFormValueBytes)
	}
	for _, char := range value {
		if char < ' ' || char == 0x7f {
			return errors.New("form scalar contains a control character")
		}
	}
	return nil
}
