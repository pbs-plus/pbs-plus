package pxar

type Request struct {
	Data any
}

type Response map[string]any

type filesystemCapabilities struct {
	supportsACLs           bool
	supportsPersistentACLs bool
	supportsXAttrs         bool
	supportsChown          bool
	prefersSequentialOps   bool
}
