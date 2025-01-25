package pattern

import (
	"strings"
)

type State struct {
	transitions    map[string]*State
	wildcardState  *State
	recursiveState *State
	isTerminal     bool
	pattern        *Pattern
}

type Matcher struct {
	rawPatterns []string
	root        *State
}

func NewMatcher(patterns []string) (*Matcher, error) {
	root := &State{transitions: make(map[string]*State)}

	for _, pat := range patterns {
		pattern, err := NewPattern(pat)
		if err != nil {
			return nil, err
		}
		if !pattern.isNegative {
			addPatternToState(root, pattern)
		}
	}

	for _, pat := range patterns {
		pattern, err := NewPattern(pat)
		if err != nil {
			return nil, err
		}
		if pattern.isNegative {
			addPatternToState(root, pattern)
		}
	}
	return &Matcher{root: root, rawPatterns: patterns}, nil
}

func addPatternToState(state *State, pattern *Pattern) {
	current := state
	for _, seg := range pattern.segments {
		if seg.isDoubleWildcard {
			if current.recursiveState == nil {
				current.recursiveState = &State{transitions: make(map[string]*State)}
			}
			current = current.recursiveState
		} else if !seg.isLiteral {
			if current.wildcardState == nil {
				current.wildcardState = &State{transitions: make(map[string]*State)}
			}
			current = current.wildcardState
		} else {
			if _, exists := current.transitions[seg.literal]; !exists {
				current.transitions[seg.literal] = &State{transitions: make(map[string]*State)}
			}
			current = current.transitions[seg.literal]
		}
	}
	current.isTerminal = true
	current.pattern = pattern
}

func (m *Matcher) Match(rawPath string) (bool, *Pattern) {
	path := strings.ReplaceAll(rawPath, "\\", "/")

	segments := strings.Split(strings.ToUpper(path), "/")

	matches := m.matchSegments(m.root, segments)

	// Check exact negation matches
	for _, state := range matches {
		if state.isTerminal && state.pattern.isNegative && !state.pattern.hasDoubleWildcard {
			exactMatch := true
			for i, seg := range state.pattern.segments {
				if i >= len(segments) || (seg.isLiteral && seg.literal != segments[i]) {
					exactMatch = false
					break
				}
			}
			if exactMatch {
				return false, state.pattern
			}
		}
	}

	// Check wildcard negations
	for _, state := range matches {
		if state.isTerminal && state.pattern.isNegative && state.pattern.hasDoubleWildcard {
			if state.pattern.prefixLiterals[0] == segments[0] {
				return false, state.pattern
			}
		}
	}

	// Check positive patterns
	for _, state := range matches {
		if state.isTerminal && !state.pattern.isNegative {
			return true, state.pattern
		}
	}

	return false, nil
}

func (m *Matcher) ToStringArray() []string {
	return m.rawPatterns
}

func (m *Matcher) matchSegments(state *State, segments []string) []*State {
	if len(segments) == 0 {
		var matches []*State
		matches = append(matches, state)
		if state.recursiveState != nil {
			recursiveMatches := m.matchSegments(state.recursiveState, segments)
			matches = append(matches, recursiveMatches...)
			matches = append(matches, state.recursiveState)
		}
		return uniqueStates(matches)
	}

	var matches []*State
	seg := segments[0]

	if state.recursiveState != nil {
		matches = append(matches, m.matchSegments(state.recursiveState, segments)...)
		matches = append(matches, m.matchSegments(state.recursiveState, segments[1:])...)
		matches = append(matches, m.matchSegments(state, segments[1:])...)
	}

	if next, ok := state.transitions[seg]; ok {
		matches = append(matches, m.matchSegments(next, segments[1:])...)
	}

	if state.wildcardState != nil {
		matches = append(matches, m.matchSegments(state.wildcardState, segments[1:])...)
	}

	return uniqueStates(matches)
}

func uniqueStates(states []*State) []*State {
	seen := make(map[*State]bool)
	unique := make([]*State, 0)

	for _, state := range states {
		if !seen[state] {
			seen[state] = true
			unique = append(unique, state)
		}
	}
	return unique
}
