package pxar

type filesystemCapabilities struct {
	supportsACLs           bool
	supportsPersistentACLs bool
	supportsXAttrs         bool
	supportsChown          bool
	prefersSequentialOps   bool
}
