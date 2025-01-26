package pattern

import "github.com/gobwas/glob"

type GlobPattern struct {
	glob.Glob
	RawString string
}

func IsValidPattern(pattern string) bool {
	if _, err := glob.Compile(pattern); err == nil {
		return true
	}

	return false
}
