package pattern

import (
	"fmt"
	"strings"
	"sync"
)

type Pattern struct {
	rawString         string
	isNegative        bool
	segments          []segmentInfo
	hasDoubleWildcard bool
	literals          map[int]string // For fixed segments
	prefixLiterals    []string       // For ** patterns
	suffixLiterals    []string       // For ** patterns
	minSegments       int            // For ** patterns
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

	if glob == "/" {
		return &Pattern{
			segments:    []segmentInfo{},
			literals:    make(map[int]string),
			minSegments: 0,
			rawString:   glob,
		}, nil
	}

	p := &Pattern{rawString: glob}
	if glob[0] == '!' {
		p.isNegative = true
		glob = glob[1:]
	}

	glob = strings.ReplaceAll(glob, "**", "\x00") // Temporary marker
	glob = strings.ReplaceAll(glob, "*", "\x01")  // Single star marker
	glob = strings.ReplaceAll(glob, "\x00", "**") // Restore double stars
	glob = strings.ReplaceAll(glob, "\x01", "*")  // Restore single stars
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

	p.precomputeLiterals()
	return p, nil
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

func (p *Pattern) String() string {
	return p.rawString
}

func normalizePath(path string) string {
	if path == "/" {
		return ""
	}

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

func (p *Pattern) matchSegments(upperParts []string) bool {
	if p.hasDoubleWildcard {
		// Keep existing double wildcard handling but add safety checks
		if len(upperParts) < p.minSegments {
			return false
		}
		if !checkPrefix(upperParts, p.prefixLiterals) {
			return false
		}
		if !checkSuffix(upperParts, p.suffixLiterals) {
			return false
		}
		return p.matchWithDoubleWildcard(upperParts)
	}

	// Reinstate critical segment count check
	if len(upperParts) != len(p.segments) {
		return false
	}

	// Check literals first for fast failure
	for pos, lit := range p.literals {
		if upperParts[pos] != lit {
			return false
		}
	}

	// Verify remaining segments with wildcards
	for i, seg := range p.segments {
		if seg.isLiteral {
			continue // Already validated
		}
		if !p.matchSegment(seg, upperParts[i]) {
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

func (p *Pattern) matchWithDoubleWildcard(upperParts []string) bool {
	patternSegments := p.segments
	pathSegments := upperParts

	// Matrix for dynamic programming
	dp := make([][]bool, len(patternSegments)+1)
	for i := range dp {
		dp[i] = make([]bool, len(pathSegments)+1)
	}
	dp[0][0] = true

	for i := 1; i <= len(patternSegments); i++ {
		pattern := patternSegments[i-1]
		if pattern.isDoubleWildcard {
			dp[i][0] = dp[i-1][0]
			for j := 1; j <= len(pathSegments); j++ {
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			}
		} else {
			for j := 1; j <= len(pathSegments); j++ {
				if dp[i-1][j-1] && p.matchSegment(pattern, pathSegments[j-1]) {
					dp[i][j] = true
				}
			}
		}
	}

	return dp[len(patternSegments)][len(pathSegments)]
}

func (p *Pattern) matchPreprocessed(upperParts []string) bool {
	matched := p.matchSegments(upperParts)
	return matched != p.isNegative
}

// Add these to Pattern struct during initialization
func (p *Pattern) precomputeLiterals() {
	p.literals = make(map[int]string)
	for i, seg := range p.segments {
		if seg.isLiteral {
			p.literals[i] = seg.literal
		}
	}

	if p.hasDoubleWildcard {
		// Precompute prefix/suffix literals
		var firstWC, lastWC = -1, -1
		for i, seg := range p.segments {
			if seg.isDoubleWildcard {
				if firstWC == -1 {
					firstWC = i
				}
				lastWC = i
			}
		}

		// Prefix literals
		for i := 0; i < firstWC; i++ {
			if p.segments[i].isLiteral {
				p.prefixLiterals = append(p.prefixLiterals, p.segments[i].literal)
			} else {
				break
			}
		}

		// Suffix literals
		for i := len(p.segments) - 1; i > lastWC; i-- {
			if p.segments[i].isLiteral {
				p.suffixLiterals = append([]string{p.segments[i].literal}, p.suffixLiterals...)
			} else {
				break
			}
		}

		p.minSegments = len(p.prefixLiterals) + len(p.suffixLiterals)
	}
}
