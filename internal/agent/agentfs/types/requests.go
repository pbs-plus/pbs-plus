package types

import (
	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
)

// OpenFileReq represents a request to open a file
type OpenFileReq struct {
	Path string
	Flag int
	Perm int
}

func (req *OpenFileReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(req.Path) + 4 + 4)
	if err := enc.WriteString(req.Path); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(req.Flag)); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(req.Perm)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *OpenFileReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	path, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.Path = path

	flag, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.Flag = int(flag)

	perm, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.Perm = int(perm)

	return nil
}

// StatReq represents a request to get file stats
type StatReq struct {
	Path string
}

func (req *StatReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(req.Path))
	if err := enc.WriteString(req.Path); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *StatReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	path, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.Path = path

	return nil
}

// ReadDirReq represents a request to read a directory
type ReadDirReq struct {
	HandleID FileHandleId
}

func (req *ReadDirReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *ReadDirReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	handleId, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(handleId)

	return nil
}

// ReadReq represents a request to read from a file
type ReadReq struct {
	HandleID FileHandleId
	Length   int
}

func (req *ReadReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8 + 4)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(req.Length)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *ReadReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	handleID, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(handleID)

	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.Length = int(length)

	return nil
}

// ReadAtReq represents a request to read from a file at a specific offset
type ReadAtReq struct {
	HandleID FileHandleId
	Offset   int64
	Length   int
}

func (req *ReadAtReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8 + 8 + 4)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(req.Offset); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(req.Length)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *ReadAtReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	handleID, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(handleID)

	offset, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	req.Offset = offset

	length, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.Length = int(length)

	return nil
}

// CloseReq represents a request to close a file
type CloseReq struct {
	HandleID FileHandleId
}

func (req *CloseReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *CloseReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	handleID, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(handleID)

	return nil
}

// BackupReq represents a request to back up a file
type BackupReq struct {
	JobId      string
	Drive      string
	SourceMode string
	ReadMode   string
	Extras     string
}

func (req *BackupReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(req.JobId) + len(req.Drive) + len(req.SourceMode) + len(req.ReadMode) + len(req.Extras))
	if err := enc.WriteString(req.JobId); err != nil {
		return nil, err
	}
	if err := enc.WriteString(req.Drive); err != nil {
		return nil, err
	}
	if err := enc.WriteString(req.SourceMode); err != nil {
		return nil, err
	}
	if err := enc.WriteString(req.ReadMode); err != nil {
		return nil, err
	}
	if err := enc.WriteString(req.Extras); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *BackupReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	jobId, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.JobId = jobId

	drive, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.Drive = drive

	sourceMode, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.SourceMode = sourceMode

	readMode, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.ReadMode = readMode

	extras, err := dec.ReadString()
	if err != nil {
		return err
	}
	req.Extras = extras

	return nil
}

// LseekReq represents a request to seek within a file
type LseekReq struct {
	HandleID FileHandleId
	Offset   int64
	Whence   int
}

func (req *LseekReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8 + 8 + 4)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(req.Offset); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(req.Whence)); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *LseekReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	handleID, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(handleID)

	offset, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	req.Offset = offset

	whence, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.Whence = int(whence)

	return nil
}
