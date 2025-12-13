package types

// OpenFileReq represents a request to open a file
type OpenFileReq struct {
	Path string
	Flag int
	Perm int
}

func (req *OpenFileReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *OpenFileReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// StatReq represents a request to get file stats
type StatReq struct {
	Path    string
	AclOnly bool
}

func (req *StatReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *StatReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// FileHandleId is a type alias for uint64
type FileHandleId uint64

func (id *FileHandleId) Encode() ([]byte, error) {
	return cborEncMode.Marshal(uint64(*id))
}

func (id *FileHandleId) Decode(buf []byte) error {
	var value uint64
	if err := cborDecMode.Unmarshal(buf, &value); err != nil {
		return err
	}
	*id = FileHandleId(value)
	return nil
}

// ReadDirReq represents a request to read a directory
type ReadDirReq struct {
	HandleID FileHandleId
}

func (req *ReadDirReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *ReadDirReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// ReadReq represents a request to read from a file
type ReadReq struct {
	HandleID FileHandleId
	Length   int
}

func (req *ReadReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *ReadReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// ReadAtReq represents a request to read from a file at a specific offset
type ReadAtReq struct {
	HandleID FileHandleId
	Offset   int64
	Length   int
}

func (req *ReadAtReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *ReadAtReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// CloseReq represents a request to close a file
type CloseReq struct {
	HandleID FileHandleId
}

func (req *CloseReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *CloseReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// BackupReq represents a request to back up a file
type BackupReq struct {
	JobId      string
	Volume     string
	SourceMode string
	ReadMode   string
	Extras     string
}

func (req *BackupReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *BackupReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

// LseekReq represents a request to seek within a file
type LseekReq struct {
	HandleID FileHandleId
	Offset   int64
	Whence   int
}

func (req *LseekReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *LseekReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}

type TargetStatusReq struct {
	Drive   string
	Subpath string
}

func (req *TargetStatusReq) Encode() ([]byte, error) {
	return cborEncMode.Marshal(req)
}

func (req *TargetStatusReq) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, req)
}
