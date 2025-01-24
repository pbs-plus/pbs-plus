package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// GlobToRegex converts a glob pattern to a regex-compatible string
func GlobToRegex(glob string) (string, error) {
	negate := false
	if strings.HasPrefix(glob, "!") {
		negate = true
		glob = glob[1:]
	}

	glob = strings.TrimRight(glob, "/\\")
	var regexStr strings.Builder
	regexStr.WriteString("^") // Start anchor

	i := 0
	for i < len(glob) {
		c := glob[i]
		switch c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				// Double star - match across directories
				regexStr.WriteString(".*")
				i += 2
				continue
			}
			// Single star - non-separator characters
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

	regexStr.WriteString(`$`) // End anchor

	finalRegex := regexStr.String()
	if negate {
		finalRegex = fmt.Sprintf("^(?!%s).*", finalRegex[1:])
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
