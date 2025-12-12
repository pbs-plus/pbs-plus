package arpc

type StringMsg string

func (msg *StringMsg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*string)(msg))
}

func (msg *StringMsg) Decode(buf []byte) error {
	var s string
	if err := cborDecMode.Unmarshal(buf, &s); err != nil {
		return err
	}
	*msg = StringMsg(s)
	return nil
}

type IntMsg int

func (msg *IntMsg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*int)(msg))
}

func (msg *IntMsg) Decode(buf []byte) error {
	var s int
	if err := cborDecMode.Unmarshal(buf, &s); err != nil {
		return err
	}
	*msg = IntMsg(s)
	return nil
}

type MapStringIntMsg map[string]int

func (msg *MapStringIntMsg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*map[string]int)(msg))
}

func (msg *MapStringIntMsg) Decode(buf []byte) error {
	var m map[string]int
	if err := cborDecMode.Unmarshal(buf, &m); err != nil {
		return err
	}
	*msg = MapStringIntMsg(m)
	return nil
}

type MapStringUint64Msg map[string]uint64

func (msg *MapStringUint64Msg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*map[string]uint64)(msg))
}

func (msg *MapStringUint64Msg) Decode(buf []byte) error {
	var m map[string]uint64
	if err := cborDecMode.Unmarshal(buf, &m); err != nil {
		return err
	}
	*msg = MapStringUint64Msg(m)
	return nil
}

type MapStringStringMsg map[string]string

func (msg *MapStringStringMsg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*map[string]string)(msg))
}

func (msg *MapStringStringMsg) Decode(buf []byte) error {
	var m map[string]string
	if err := cborDecMode.Unmarshal(buf, &m); err != nil {
		return err
	}
	*msg = MapStringStringMsg(m)
	return nil
}

type MapStringBoolMsg map[string]bool

func (msg *MapStringBoolMsg) Encode() ([]byte, error) {
	return cborEncMode.Marshal((*map[string]bool)(msg))
}

func (msg *MapStringBoolMsg) Decode(buf []byte) error {
	var m map[string]bool
	if err := cborDecMode.Unmarshal(buf, &m); err != nil {
		return err
	}
	*msg = MapStringBoolMsg(m)
	return nil
}
