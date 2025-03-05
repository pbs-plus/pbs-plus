package arpc

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc/arpcdata"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/sb"
	"github.com/xtaci/smux"
)

type Request struct {
	Method  string
	Payload []byte // Serialized data of one of the other structs
}

func (req *Request) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()
	if err := enc.WriteBytes(sb.ToBytes(req.Method)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(req.Payload); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *Request) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	method, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	req.Method = sb.ToString(method)
	payload, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	req.Payload = payload
	return nil
}

type Response struct {
	Status    int
	Message   string
	Data      []byte
	RawStream func(*smux.Stream)
}

func (resp *Response) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()
	if err := enc.WriteUint32(uint32(resp.Status)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(sb.ToBytes(resp.Message)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(resp.Data); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (resp *Response) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	status, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	resp.Status = int(status)
	message, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	resp.Message = sb.ToString(message)
	dataField, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	// Copy the data to avoid referencing the pooled buffer
	resp.Data = make([]byte, len(dataField))
	copy(resp.Data, dataField)
	// Note: RawStream is skipped
	return nil
}

// SerializableError represents a serializable error
type SerializableError struct {
	ErrorType     string
	Message       string
	Op            string
	Path          string
	OriginalError error // Skipped during encoding/decoding
}

func (errObj *SerializableError) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()
	if err := enc.WriteBytes(sb.ToBytes(errObj.ErrorType)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(sb.ToBytes(errObj.Message)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(sb.ToBytes(errObj.Op)); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(sb.ToBytes(errObj.Path)); err != nil {
		return nil, err
	}
	// Note: OriginalError is skipped
	return enc.Bytes(), nil
}

func (errObj *SerializableError) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	errorType, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	errObj.ErrorType = sb.ToString(errorType)
	message, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	errObj.Message = sb.ToString(message)
	op, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	errObj.Op = sb.ToString(op)
	path, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	errObj.Path = sb.ToString(path)
	// Note: OriginalError is skipped
	return nil
}
