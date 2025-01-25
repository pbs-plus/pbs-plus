package pattern

import (
	"fmt"
	"strings"
)

func parseSegment(seg string) ([]token, bool, string, error) {
	if seg == "" {
		return nil, false, "", fmt.Errorf("empty segment")
	}

	seg = strings.ToUpper(seg)
	tokens := tokenPool.Get().([]token)
	defer func() {
		tokens = tokens[:0]
		tokenPool.Put(tokens)
	}()

	isLiteral := true
	var literalBuilder strings.Builder

	for i := 0; i < len(seg); {
		c := seg[i]
		switch c {
		case '*':
			isLiteral = false
			// Collapse multiple stars to single anySequenceToken
			tokens = append(tokens, anySequenceToken{})
			// Skip all consecutive asterisks
			for i < len(seg) && seg[i] == '*' {
				i++
			}
		case '?':
			isLiteral = false
			tokens = append(tokens, anyCharToken{})
			i++
		case '[':
			end := strings.IndexByte(seg[i:], ']')
			if end == -1 {
				return nil, false, "", fmt.Errorf("unclosed bracket")
			}
			end += i
			content := seg[i+1 : end]

			var allowed [256]bool
			if err := parseBracketContent(content, &allowed); err != nil {
				return nil, false, "", err
			}

			isLiteral = false
			tokens = append(tokens, bracketToken{allowed: allowed})
			i = end + 1
		default:
			literalBuilder.WriteByte(c)
			tokens = append(tokens, literalToken{char: c})
			i++
		}
	}

	if isLiteral {
		return nil, true, literalBuilder.String(), nil
	}

	result := make([]token, len(tokens))
	copy(result, tokens)
	return result, false, "", nil
}

func matchWildcard(tokens []token, text string) bool {
	n := len(tokens)
	m := len(text)
	if m == 0 && n == 0 {
		return true
	}
	if n == 0 || m == 0 {
		return false
	}

	prev := make([]bool, m+1)
	current := make([]bool, m+1)
	prev[0] = true

	for i := 1; i <= n; i++ {
		token := tokens[i-1]
		current[0] = prev[0] && isAnySequence(token)

		for j := 1; j <= m; j++ {
			switch t := token.(type) {
			case anySequenceToken:
				current[j] = prev[j] || current[j-1]
			case anyCharToken:
				current[j] = prev[j-1]
			case literalToken:
				current[j] = prev[j-1] && text[j-1] == t.char
			case bracketToken:
				current[j] = prev[j-1] && t.allowed[text[j-1]]
			}
		}

		prev, current = current, make([]bool, m+1)
	}

	return prev[m]
}

func isAnySequence(t token) bool {
	_, ok := t.(anySequenceToken)
	return ok
}

func matchSegmentWithIndex(info segmentInfo, parts []string, idx int) bool {
	if info.isDoubleWildcard {
		return true
	}
	if info.isLiteral {
		return info.literal == parts[idx]
	}
	return matchWildcard(info.tokens, parts[idx])
}

func IsValidPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	if strings.HasPrefix(pattern, "!") {
		pattern = pattern[1:]
	}
	if strings.Contains(pattern, "{") {
		return false
	}

	inBracket := false
	for _, c := range pattern {
		switch c {
		case '[':
			if inBracket {
				return false
			}
			inBracket = true
		case ']':
			if !inBracket {
				return false
			}
			inBracket = false
		}
	}
	return !inBracket
}

func parseBracketContent(content string, allowed *[256]bool) error {
	for i := 0; i < len(content); {
		c := content[i]
		if i+2 < len(content) && content[i+1] == '-' {
			start := c
			end := content[i+2]
			if start > end {
				return fmt.Errorf("invalid range")
			}
			for char := start; char <= end; char++ {
				allowed[char] = true
			}
			i += 3
		} else {
			allowed[c] = true
			i++
		}
	}
	return nil
}

func PreprocessPath(path string) []string {
	normalized := normalizePath(path)
	if normalized == "" {
		return nil
	}
	parts := strings.Split(normalized, "/")
	upperParts := make([]string, len(parts))
	for i, p := range parts {
		upperParts[i] = strings.ToUpper(p)
	}
	return upperParts
}

func checkPrefix(path, literals []string) bool {
	if len(literals) == 0 {
		return true
	}
	if len(path) < len(literals) {
		return false
	}
	for i, lit := range literals {
		if path[i] != lit {
			return false
		}
	}
	return true
}

func checkSuffix(path, literals []string) bool {
	if len(literals) == 0 {
		return true
	}
	if len(path) < len(literals) {
		return false
	}
	offset := len(path) - len(literals)
	for i, lit := range literals {
		if path[offset+i] != lit {
			return false
		}
	}
	return true
}
