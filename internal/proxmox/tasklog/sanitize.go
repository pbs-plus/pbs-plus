//go:build linux

package tasklog

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxMessageLen = 16 * 1024

func SanitizeMessage(msg string) string {
	if msg == "" {
		return msg
	}
	if utf8.ValidString(msg) && len(msg) <= maxMessageLen && !strings.ContainsFunc(msg, needsEscape) {
		return msg
	}

	var sb strings.Builder
	truncated := false
	for i := 0; i < len(msg); {
		if sb.Len() >= maxMessageLen {
			truncated = true
			break
		}
		c := msg[i]
		if c < utf8.RuneSelf {
			if c == '\t' || (c >= 0x20 && c < 0x7f) {
				sb.WriteByte(c)
			} else {
				fmt.Fprintf(&sb, `\x%02x`, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(msg[i:])
		if r == utf8.RuneError && size == 1 {
			fmt.Fprintf(&sb, `\x%02x`, c)
			i++
			continue
		}
		if sb.Len()+size > maxMessageLen {
			truncated = true
			break
		}
		sb.WriteString(msg[i : i+size])
		i += size
	}
	if truncated {
		sb.WriteString("...[truncated]")
	}
	return sb.String()
}

func needsEscape(r rune) bool {
	return (r < 0x20 && r != '\t') || r == 0x7f
}
