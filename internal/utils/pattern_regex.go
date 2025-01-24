package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// GlobToRegex converts a glob pattern to a case-insensitive regex without start/end anchors
func GlobToRegex(glob string) (string, error) {
	negate := false
	if strings.HasPrefix(glob, "!") {
		negate = true
		glob = glob[1:]
	}

	glob = strings.TrimRight(glob, "/\\")
	var regexStr strings.Builder

	i := 0
	for i < len(glob) {
		c := glob[i]
		switch c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				regexStr.WriteString(".*")
				i += 2
				continue
			}
			regexStr.WriteString(`[^/\\]*`)
		case '?':
			regexStr.WriteString(`[^/\\]`)
		case '.':
			regexStr.WriteString(`\.`)
		case '\\', '/':
			regexStr.WriteString(`[/\\]`)
		case '[':
			end := strings.IndexByte(glob[i:], ']')
			if end == -1 {
				return "", fmt.Errorf("unclosed character class")
			}
			end += i
			regexStr.WriteString(regexp.QuoteMeta(glob[i : end+1]))
			i = end
		case '{':
			return "", fmt.Errorf("brace expansions not supported")
		default:
			if strings.ContainsRune(`+()|$.^`, rune(c)) {
				regexStr.WriteByte('\\')
			}
			regexStr.WriteByte(c)
		}
		i++
	}

	finalRegex := regexStr.String()
	if negate {
		// Case-insensitive negative lookahead
		finalRegex = fmt.Sprintf("^(?!.*(?i:%s)).*$", finalRegex)
	} else {
		// Prepend case-insensitive flag
		finalRegex = "(?i)" + finalRegex
	}

	return finalRegex, nil
}

func IsValidPattern(pattern string) bool {
	regexStr, err := GlobToRegex(pattern)
	if err != nil {
		return false
	}
	_, err = regexp.Compile(regexStr)
	return err == nil
}
