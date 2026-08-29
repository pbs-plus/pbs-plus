//go:build linux

package snapshotmount

import "testing"

func TestIsWholeRootSelection(t *testing.T) {
	tests := []struct {
		name      string
		paths     []string
		stripRoot bool
		want      bool
	}{
		{name: "root", paths: []string{"/"}, want: true},
		{name: "unclean root", paths: []string{"/dir/.."}},
		{name: "flatten root", paths: []string{"/"}, stripRoot: true},
		{name: "directory", paths: []string{"/dir"}},
		{name: "multiple", paths: []string{"/", "/dir"}},
		{name: "empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isWholeRootSelection(test.paths, test.stripRoot); got != test.want {
				t.Fatalf("isWholeRootSelection(%v, %t) = %t, want %t", test.paths, test.stripRoot, got, test.want)
			}
		})
	}
}
