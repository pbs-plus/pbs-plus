package pattern

import (
	"testing"
)

func TestPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		valid   bool
		paths   map[string]bool // path -> should match
	}{
		{
			name:    "empty pattern",
			pattern: "",
			valid:   false,
		},
		{
			name:    "simple exact match",
			pattern: "foo",
			valid:   true,
			paths: map[string]bool{
				"foo":     true,
				"FOO":     true,
				"foo/":    true,
				"food":    false,
				"fo":      false,
				"foo/bar": false,
			},
		},
		{
			name:    "negated pattern",
			pattern: "!foo",
			valid:   true,
			paths: map[string]bool{
				"foo":     false,
				"FOO":     false,
				"food":    true,
				"bar":     true,
				"foo/bar": true,
			},
		},
		{
			name:    "single wildcard",
			pattern: "*.txt",
			valid:   true,
			paths: map[string]bool{
				"file.txt":   true,
				"FILE.TXT":   true,
				".txt":       true,
				"file.doc":   false,
				"file.txt/a": false,
				"dir/a.txt":  false,
			},
		},
		{
			name:    "double wildcard",
			pattern: "**/*.txt",
			valid:   true,
			paths: map[string]bool{
				"file.txt":     true,
				"dir/a.txt":    true,
				"a/b/c/d.txt":  true,
				"file.doc":     false,
				"dir/file.doc": false,
			},
		},
		{
			name:    "question mark",
			pattern: "?.txt",
			valid:   true,
			paths: map[string]bool{
				"a.txt":  true,
				"A.TXT":  true,
				"ab.txt": false,
				".txt":   false,
				"dir/a":  false,
			},
		},
		{
			name:    "bracket expression",
			pattern: "[abc].txt",
			valid:   true,
			paths: map[string]bool{
				"a.txt":  true,
				"b.txt":  true,
				"c.txt":  true,
				"d.txt":  false,
				"ab.txt": false,
			},
		},
		{
			name:    "unclosed bracket",
			pattern: "[abc.txt",
			valid:   false,
		},
		{
			name:    "nested brackets",
			pattern: "[[abc]].txt",
			valid:   false,
		},
		{
			name:    "brace expansion",
			pattern: "{a,b}.txt",
			valid:   false,
		},
		{
			name:    "mixed path separators",
			pattern: "dir\\*.txt",
			valid:   true,
			paths: map[string]bool{
				"dir/file.txt":     true,
				"dir\\file.txt":    true,
				"dir/sub/file.txt": false,
			},
		},
		{
			name:    "complex pattern",
			pattern: "!**/temp/[0-9]*.log",
			valid:   true,
			paths: map[string]bool{
				"temp/1.log":     false,
				"a/temp/123.log": false,
				"temp/abc.log":   true,
				"temp/log":       true,
				"other/file.log": true,
			},
		},
		{
			name:    "multiple asterisks in segment",
			pattern: "***",
			valid:   true,
			paths: map[string]bool{
				"anyfile":           true,
				"any/directory":     false,
				"multiple/segments": false,
			},
		},
		{
			name:    "mixed asterisks and literals",
			pattern: "a***b",
			valid:   true,
			paths: map[string]bool{
				"aXXXb": true,
				"ab":    true,
				"aXb":   true,
				"aXbY":  false,
				"a/b":   false,
			},
		},
		{
			name:    "question mark and asterisk",
			pattern: "a?*b",
			valid:   true,
			paths: map[string]bool{
				"aXb":   true,
				"aXYb":  true,
				"ab":    false,
				"aXZYb": true,
				"a/b":   false,
			},
		},
		{
			name:    "bracket with wildcard",
			pattern: "[ab]*.txt",
			valid:   true,
			paths: map[string]bool{
				"a.txt":      true,
				"bfile.txt":  true,
				"cfile.txt":  false,
				"a/file.txt": false,
			},
		},
		{
			name:    "double wildcard in various positions",
			pattern: "**/test/**",
			valid:   true,
			paths: map[string]bool{
				"test":         true,
				"a/test/b":     true,
				"test/b/c":     true,
				"a/b/test/c/d": true,
				"atest/b":      false,
			},
		},
		{
			name:    "root directory match",
			pattern: "/",
			valid:   true,
			paths: map[string]bool{
				"":     true,
				"file": false,
				"dir/": false,
			},
		},
		{
			name:    "wildcard in directory and file",
			pattern: "src/*/*.js",
			valid:   true,
			paths: map[string]bool{
				"src/app/index.js":          true,
				"src/index.js":              false,
				"src/utils/helpers.js":      true,
				"src/utils/helpers/test.js": false,
			},
		},
		{
			name:    "hidden directories",
			pattern: ".config/**",
			valid:   true,
			paths: map[string]bool{
				".config/file":        true,
				".config/sub/file":    true,
				"config/file":         false,
				".config-hidden/file": false,
			},
		},
		{
			name:    "complex mixed patterns",
			pattern: "!docs/**/*.md",
			valid:   true,
			paths: map[string]bool{
				"docs/readme.md":      false,
				"src/readme.md":       true,
				"docs/sub/note.md":    false,
				"docs/images/img.png": true,
			},
		},
		{
			name:    "empty segment normalization",
			pattern: "a//b",
			valid:   true,
			paths: map[string]bool{
				"a/b":   true,
				"a//b":  true,
				"a/c/b": false,
				"a/b/c": false,
			},
		},
		{
			name:    "numeric range bracket",
			pattern: "report[0-9].txt",
			valid:   true,
			paths: map[string]bool{
				"report0.txt":  true,
				"report5.txt":  true,
				"reportA.txt":  false,
				"report10.txt": false,
			},
		},
		{
			name:    "case insensitive special chars",
			pattern: "FILE_#.TXT",
			valid:   true,
			paths: map[string]bool{
				"file_#.txt":  true,
				"FILE_#.TXT":  true,
				"file_1.txt":  false,
				"file_#.txtx": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test pattern validity
			if got := IsValidPattern(tt.pattern); got != tt.valid {
				t.Errorf("IsValidPattern(%q) = %v, want %v", tt.pattern, got, tt.valid)
			}

			if !tt.valid || tt.paths == nil {
				return
			}

			// Test pattern matching
			pattern, err := NewPattern(tt.pattern)
			if err != nil {
				t.Fatalf("NewPattern(%q) error = %v", tt.pattern, err)
			}

			for path, want := range tt.paths {
				if got := pattern.Match(path); got != want {
					t.Errorf("Pattern(%q).Match(%q) = %v, want %v", tt.pattern, path, got, want)
				}
			}
		})
	}
}

func TestPatternEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		path    string
		want    bool
	}{
		{"empty path", "*.txt", "", false},
		{"only slashes", "*.txt", "///", false},
		{"only backslashes", "*.txt", "\\\\\\", false},
		{"mixed slashes", "dir/*.txt", "dir\\/\\/file.txt", true},
		{"consecutive stars", "**/**/**.txt", "a/b/c.txt", true},
		{"all wildcards", "***", "anything", true},
		{"escaped special chars", "\\*.txt", "*.txt", true},
		{
			name:    "mixed wildcard types",
			pattern: "a?b*/file.*",
			path:    "aXb123/file.txt",
			want:    true,
		},
		{
			name:    "deeply nested",
			pattern: "a/b/c/d/e/f",
			path:    "a/b/c/d/e/f",
			want:    true,
		},
		{
			name:    "multiple dots",
			pattern: "*.*.txt",
			path:    "file.version.txt",
			want:    true,
		},
		{
			name:    "uppercase normalization",
			pattern: "mixedCase",
			path:    "MIXEDcase",
			want:    true,
		},
		{
			name:    "special characters",
			pattern: "file_*.@(txt|md)",
			path:    "file_123.@(txt|md)",
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern, err := NewPattern(tt.pattern)
			if err != nil {
				t.Fatalf("NewPattern(%q) error = %v", tt.pattern, err)
			}

			if got := pattern.Match(tt.path); got != tt.want {
				t.Errorf("Pattern(%q).Match(%q) = %v, want %v", tt.pattern, tt.path, got, tt.want)
			}
		})
	}
}
