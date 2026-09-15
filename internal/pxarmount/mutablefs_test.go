package pxarmount

import "testing"

func TestMutableFSCloseIsIdempotent(t *testing.T) {
	fs := &MutableFS{}
	fs.Close()
	fs.Close()
}
