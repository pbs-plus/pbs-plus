package pxarmount

import (
	"io"
	"strings"
	"testing"

	pxar "github.com/pbs-plus/pxar"
)

func TestReadFileAtInlineAndSplitPayload(t *testing.T) {
	for _, test := range []struct {
		name   string
		inline bool
		header string
	}{
		{name: "inline", inline: true},
		{name: "split", header: strings.Repeat("h", 16)},
	} {
		t.Run(test.name, func(t *testing.T) {
			const contents = "archive contents"
			fs := &PxarFS{readerAt: strings.NewReader("prefix" + test.header + contents)}
			entry := &pxar.Entry{ContentOffset: 6, FileSize: uint64(len(contents))}
			if !test.inline {
				entry.PayloadOffset = 6
			}
			n := newNodeFromEntry(entry, 2, RootInode)
			for _, offset := range []int64{0, 3, int64(len(contents))} {
				buffer := make([]byte, len(contents)+10)
				count, err := fs.readFileAt(&n, offset, buffer)
				if err != nil && err != io.EOF {
					t.Fatal(err)
				}
				if got, want := string(buffer[:count]), contents[offset:]; got != want {
					t.Fatalf("offset %d: read %q, want %q", offset, got, want)
				}
			}
		})
	}
}
