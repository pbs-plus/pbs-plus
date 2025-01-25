package pattern

import (
	"testing"
)

func TestMatcher(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		paths    []struct {
			path string
			want bool
		}
	}{
		{
			name:     "basic literals",
			patterns: []string{"foo/bar", "baz/qux"},
			paths: []struct {
				path string
				want bool
			}{
				{"foo/bar", true},
				{"baz/qux", true},
				{"foo/baz", false},
				{"foo/ba", false},
			},
		},
		{
			name:     "wildcards",
			patterns: []string{"foo/*/baz", "qux/*"},
			paths: []struct {
				path string
				want bool
			}{
				{"foo/bar/baz", true},
				{"foo/anything/baz", true},
				{"qux/anything", true},
				{"foo/bar", false},
				{"qux/too/many", false},
			},
		},
		{
			name:     "double wildcards",
			patterns: []string{"foo/**/baz", "a/**/b/**/c"},
			paths: []struct {
				path string
				want bool
			}{
				{"foo/baz", true},
				{"foo/x/baz", true},
				{"foo/x/y/baz", true},
				{"a/b/c", true},
				{"a/x/b/y/c", true},
				{"a/x/y/b/z/w/c", true},
				{"foo/bar", false},
				{"a/b/d", false},
			},
		},
		{
			name:     "negations",
			patterns: []string{"foo/**", "!foo/bar", "!foo/baz/**"},
			paths: []struct {
				path string
				want bool
			}{
				{"foo/qux", true},
				{"foo/qux/quux", true},
				{"foo/bar", false},
				{"foo/baz", false},
				{"foo/baz/qux", false},
			},
		},
		{
			name:     "case sensitivity",
			patterns: []string{"FOO/BAR", "baz/*/QUX"},
			paths: []struct {
				path string
				want bool
			}{
				{"foo/bar", true},
				{"FOO/BAR", true},
				{"baz/anything/qux", true},
				{"BAZ/anything/QUX", true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMatcher(tt.patterns)
			if err != nil {
				t.Fatalf("NewMatcher() error = %v", err)
			}

			for _, p := range tt.paths {
				got, _ := m.Match(p.path)
				if got != p.want {
					t.Errorf("Match(%q) = %v, want %v", p.path, got, p.want)
				}
			}
		})
	}
}
