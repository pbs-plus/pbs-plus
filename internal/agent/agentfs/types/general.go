package types

import (
	"github.com/fxamacker/cbor/v2"
)

var (
	cborEncMode cbor.EncMode
	cborDecMode cbor.DecMode
)

func init() {
	var err error
	// Use canonical CBOR encoding to ensure consistent byte representation
	cborEncMode, err = cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	// Default CBOR decoding options
	cborDecMode, err = cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(err)
	}
}
