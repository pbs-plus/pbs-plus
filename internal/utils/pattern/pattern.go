package pattern

import (
	"fmt"
	"strings"
	"sync"
)

type Pattern struct {
	isNegative        bool
	segments          []segmentInfo
	hasDoubleWildcard bool
}

type segmentInfo struct {
	tokens           []token
	isLiteral        bool
	literal          string
	isDoubleWildcard bool
}

type token interface{ isToken() }

type anySequenceToken struct{}
type anyCharToken struct{}
type literalToken struct{ char byte }
type bracketToken struct{ allowed [256]bool }

func (anySequenceToken) isToken() {}
func (anyCharToken) isToken()     {}
func (literalToken) isToken()     {}
func (bracketToken) isToken()     {}

var tokenPool = sync.Pool{
	New: func() interface{} { return make([]token, 0, 8) },
}

func NewPattern(glob string) (*Pattern, error) {
	if glob == "" {
		return nil, fmt.Errorf("empty pattern")
	}

	p := &Pattern{}
	if glob[0] == '!' {
		p.isNegative = true
		glob = glob[1:]
	}

	glob = strings.NewReplacer("\\", "/", "//", "/").Replace(glob)
	glob = strings.Trim(glob, "/")

	if strings.Contains(glob, "{") {
		return nil, fmt.Errorf("brace expansions not supported")
	}

	rawSegments := strings.Split(glob, "/")
	p.segments = make([]segmentInfo, 0, len(rawSegments))
	hasDoubleWildcard := false

	prevWasDoubleWildcard := false
	for _, seg := range rawSegments {
		if seg == "" {
			continue
		}

		// Handle ** segments
		if seg == "**" {
			if prevWasDoubleWildcard {
				continue // Collapse consecutive **
			}
			prevWasDoubleWildcard = true
			hasDoubleWildcard = true
			p.segments = append(p.segments, segmentInfo{isDoubleWildcard: true})
			continue
		}

		prevWasDoubleWildcard = false

		// Remove the invalid '**' check here
		tokens, isLiteral, lit, err := parseSegment(seg)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, segmentInfo{
			tokens:    tokens,
			isLiteral: isLiteral,
			literal:   lit,
		})
	}

	p.hasDoubleWildcard = hasDoubleWildcard
	return p, nil
}

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
			tokens = append(tokens, anySequenceToken{})
			i++
			// Skip consecutive asterisks after first
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

func (p *Pattern) Match(path string) bool {
	normalized := normalizePath(path)
	if normalized == "" {
		return p.matchEmptyPath()
	}

	pathParts := strings.Split(normalized, "/")
	upperParts := make([]string, len(pathParts))
	for i, part := range pathParts {
		upperParts[i] = strings.ToUpper(part)
	}

	matched := p.matchSegments(upperParts)
	return matched != p.isNegative
}

func normalizePath(path string) string {
	var normalized strings.Builder
	prevSlash := false

	for _, c := range path {
		if c == '/' || c == '\\' {
			if !prevSlash {
				normalized.WriteByte('/')
				prevSlash = true
			}
		} else {
			normalized.WriteRune(c)
			prevSlash = false
		}
	}
	return strings.Trim(normalized.String(), "/")
}

func (p *Pattern) matchEmptyPath() bool {
	if len(p.segments) == 0 {
		return true
	}
	if len(p.segments) == 1 && p.segments[0].isDoubleWildcard {
		return true
	}
	return false
}

func (p *Pattern) matchSegments(pathParts []string) bool {
	if p.hasDoubleWildcard {
		return p.matchWithDoubleWildcard(pathParts)
	}
	if len(pathParts) != len(p.segments) {
		return false
	}
	for i, seg := range p.segments {
		if !p.matchSegment(seg, pathParts[i]) {
			return false
		}
	}
	return true
}

func (p *Pattern) matchSegment(info segmentInfo, part string) bool {
	if info.isDoubleWildcard {
		return true
	}
	if info.isLiteral {
		return info.literal == part
	}
	return matchWildcard(info.tokens, part)
}

func matchWildcard(tokens []token, text string) bool {
	n := len(tokens)
	m := len(text)
	if m == 0 && n == 0 {
		return true
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

func (p *Pattern) matchWithDoubleWildcard(pathParts []string) bool {
	dp := make([]bool, len(pathParts)+1)
	dp[0] = true

	for _, seg := range p.segments {
		if seg.isDoubleWildcard {
			for j := 1; j <= len(pathParts); j++ {
				dp[j] = dp[j] || dp[j-1]
			}
		} else {
			newDp := make([]bool, len(pathParts)+1)
			for j := 1; j <= len(pathParts); j++ {
				if matchSegmentWithIndex(seg, pathParts, j-1) {
					newDp[j] = dp[j-1]
				}
				if j > 0 {
					newDp[j] = newDp[j] || newDp[j-1]
				}
			}
			dp = newDp
		}
	}

	return dp[len(pathParts)]
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
