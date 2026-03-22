package native

const PxarHeaderSize uint64 = 16
const HeaderSize_ = PxarHeaderSize

const (
	PXARHashKey1 uint64 = 0x83ac3f1cfbb450db
	PXARHashKey2 uint64 = 0xaa4f1b6879369fbd
)

const (
	PXARFormatVersion uint64 = 0x730f6c75df16a40d
	PXAREntry         uint64 = 0xd5956474e588acef
	PXAREntryV1       uint64 = 0x11da850a1c1cceff
	PXARPrelude       uint64 = 0xe309d79d9f7b771b
	PXARFilename      uint64 = 0x16701121063917b3
	PXARSymlink       uint64 = 0x27f971e7dbf5dc5f
	PXARDevice        uint64 = 0x9fc9e906586d5ce9
	PXARXAttr         uint64 = 0x0dab0229b57dcd03
	PXARAclUser       uint64 = 0x2ce8540a457d55b8
	PXARAclGroup      uint64 = 0x136e3eceb04c03ab
	PXARFCaps         uint64 = 0x2da9dd9db5f7fb67
	PXARHardlink      uint64 = 0x51269c8422bd7275
	PXARPayload       uint64 = 0x28147a1b0b7c1a25
	PXARPayloadRef    uint64 = 0x419d3d6bc4ba977e
	PXARGoodbye       uint64 = 0x2fec4fa642d5731d
	PXARGoodbyeTail   uint64 = 0xef5eed5b753e1555
)

const (
	ModeIFMT   uint64 = 0o0170000
	ModeIFSOCK uint64 = 0o0140000
	ModeIFLNK  uint64 = 0o0120000
	ModeIFREG  uint64 = 0o0100000
	ModeIFBLK  uint64 = 0o0060000
	ModeIFDIR  uint64 = 0o0040000
	ModeIFCHR  uint64 = 0o0020000
	ModeIFIFO  uint64 = 0o0010000
	ModeISUID  uint64 = 0o0004000
	ModeISGID  uint64 = 0o0002000
	ModeISVTX  uint64 = 0o0001000
)

type Header struct {
	HType    uint64
	FullSize uint64
}

func (h *Header) ContentSize() uint64 {
	if h.FullSize < PxarHeaderSize {
		return 0
	}
	return h.FullSize - PxarHeaderSize
}

type Stat struct {
	Mode  uint64
	Flags uint64
	UID   uint32
	GID   uint32
	Mtime StatxTimestamp
}

type StatxTimestamp struct {
	Secs  int64
	Nanos uint32
}

func (s *Stat) FileType() uint64 {
	return s.Mode & ModeIFMT
}

func (s *Stat) FileMode() uint64 {
	return s.Mode &^ ModeIFMT
}

func (s *Stat) IsDir() bool {
	return s.FileType() == ModeIFDIR
}

func (s *Stat) IsSymlink() bool {
	return s.FileType() == ModeIFLNK
}

func (s *Stat) IsRegularFile() bool {
	return s.FileType() == ModeIFREG
}

func (s *Stat) IsDevice() bool {
	ft := s.FileType()
	return ft == ModeIFCHR || ft == ModeIFBLK
}

func (s *Stat) IsFifo() bool {
	return s.FileType() == ModeIFIFO
}

func (s *Stat) IsSocket() bool {
	return s.FileType() == ModeIFSOCK
}

type PayloadRef struct {
	Offset uint64
	Size   uint64
}

type Device struct {
	Major uint64
	Minor uint64
}

type XAttr struct {
	Data    []byte
	NameLen int
}

func NewXAttr(name, value []byte) XAttr {
	data := make([]byte, len(name)+1+len(value))
	copy(data, name)
	data[len(name)] = 0
	copy(data[len(name)+1:], value)
	return XAttr{Data: data, NameLen: len(name)}
}

func (x *XAttr) Name() []byte {
	return x.Data[:x.NameLen]
}

func (x *XAttr) Value() []byte {
	return x.Data[x.NameLen+1:]
}
