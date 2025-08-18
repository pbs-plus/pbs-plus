package arpc

import (
	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
)

// StringMsg is a type alias for string
type StringMsg string

func (msg *StringMsg) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(*msg))
	if err := enc.WriteString(string(*msg)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (msg *StringMsg) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	str, err := dec.ReadString()
	if err != nil {
		return err
	}

	*msg = StringMsg(str)
	arpcdata.ReleaseDecoder(dec)
	return nil
}

// MapStringIntMsg is a type alias for map[string]int
type MapStringIntMsg map[string]int

func (msg *MapStringIntMsg) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(4)

	// nil flag
	if *msg == nil {
		if err := enc.WriteUint8(0); err != nil {
			return nil, err
		}
		return enc.Bytes(), nil
	}
	if err := enc.WriteUint8(1); err != nil {
		return nil, err
	}

	// length + entries
	if err := enc.WriteUint32(uint32(len(*msg))); err != nil {
		return nil, err
	}
	for key, value := range *msg {
		if err := enc.WriteString(key); err != nil {
			return nil, err
		}
		if err := enc.WriteUint32(uint32(value)); err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

func (msg *MapStringIntMsg) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	// nil flag
	flag, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	if flag == 0 {
		*msg = nil
		return nil
	}

	// length + entries
	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	*msg = make(MapStringIntMsg, length)
	for i := 0; i < int(length); i++ {
		key, err := dec.ReadString()
		if err != nil {
			return err
		}
		value, err := dec.ReadUint32()
		if err != nil {
			return err
		}
		(*msg)[key] = int(value)
	}
	return nil
}

// MapStringUint64Msg is a type alias for map[string]uint64
type MapStringUint64Msg map[string]uint64

func (msg *MapStringUint64Msg) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	// nil flag
	if *msg == nil {
		if err := enc.WriteUint8(0); err != nil {
			return nil, err
		}
		return enc.Bytes(), nil
	}
	if err := enc.WriteUint8(1); err != nil {
		return nil, err
	}

	// length + entries
	if err := enc.WriteUint32(uint32(len(*msg))); err != nil {
		return nil, err
	}
	for key, value := range *msg {
		if err := enc.WriteString(key); err != nil {
			return nil, err
		}
		if err := enc.WriteUint64(value); err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

func (msg *MapStringUint64Msg) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	// nil flag
	flag, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	if flag == 0 {
		*msg = nil
		return nil
	}

	// length + entries
	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	*msg = make(MapStringUint64Msg, length)
	for i := 0; i < int(length); i++ {
		key, err := dec.ReadString()
		if err != nil {
			return err
		}
		value, err := dec.ReadUint64()
		if err != nil {
			return err
		}
		(*msg)[key] = value
	}
	return nil
}

// MapStringStringMsg is a type alias for map[string]string
type MapStringStringMsg map[string]string

func (msg *MapStringStringMsg) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	// nil flag
	if *msg == nil {
		if err := enc.WriteUint8(0); err != nil {
			return nil, err
		}
		return enc.Bytes(), nil
	}
	if err := enc.WriteUint8(1); err != nil {
		return nil, err
	}

	// length + entries
	if err := enc.WriteUint32(uint32(len(*msg))); err != nil {
		return nil, err
	}
	for key, value := range *msg {
		if err := enc.WriteString(key); err != nil {
			return nil, err
		}
		if err := enc.WriteString(value); err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

func (msg *MapStringStringMsg) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	// nil flag
	flag, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	if flag == 0 {
		*msg = nil
		return nil
	}

	// length + entries
	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	*msg = make(MapStringStringMsg, length)
	for i := 0; i < int(length); i++ {
		key, err := dec.ReadString()
		if err != nil {
			return err
		}
		value, err := dec.ReadString()
		if err != nil {
			return err
		}
		(*msg)[key] = value
	}
	return nil
}

type MapStringBoolMsg map[string]bool

func (msg *MapStringBoolMsg) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	// Write a flag: 0 = nil map, 1 = non-nil map
	if *msg == nil {
		if err := enc.WriteUint8(0); err != nil {
			return nil, err
		}
		return enc.Bytes(), nil
	}
	if err := enc.WriteUint8(1); err != nil {
		return nil, err
	}

	// Write length and entries
	if err := enc.WriteUint32(uint32(len(*msg))); err != nil {
		return nil, err
	}
	for key, value := range *msg {
		if err := enc.WriteString(key); err != nil {
			return nil, err
		}
		if err := enc.WriteBool(value); err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

func (msg *MapStringBoolMsg) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	// Read nil/non-nil flag
	flag, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	if flag == 0 {
		*msg = nil
		return nil
	}

	// Read length and entries
	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	*msg = make(MapStringBoolMsg, length)
	for i := 0; i < int(length); i++ {
		key, err := dec.ReadString()
		if err != nil {
			return err
		}
		value, err := dec.ReadBool()
		if err != nil {
			return err
		}
		(*msg)[key] = value
	}
	return nil
}
