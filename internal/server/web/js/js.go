// Package js renders ExtJS class definitions from Go values.
//
// An ExtJS component is a nested config object, so the renderer only needs
// objects, arrays, primitives and verbatim JavaScript (Raw) for the
// imperative bits: handlers, controller methods and expressions.
package js

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Value is a node that can render itself as JavaScript source. indent is the
// current nesting depth, in two-space units.
type Value interface {
	AppendJS(dst []byte, indent int) []byte
}

// Raw is JavaScript source emitted verbatim: identifiers, expressions and
// function literals. It is never quoted or escaped.
type Raw string

// AppendJS implements Value.
func (r Raw) AppendJS(dst []byte, indent int) []byte {
	return append(dst, reindent(string(r), indent)...)
}

// Obj is a JavaScript object literal. Keys are emitted in sorted order so the
// generated file is byte-stable across builds; ExtJS config order is not
// significant.
type Obj map[string]any

// AppendJS implements Value.
func (o Obj) AppendJS(dst []byte, indent int) []byte {
	if len(o) == 0 {
		return append(dst, "{}"...)
	}
	dst = append(dst, "{\n"...)
	for _, k := range slices.Sorted(maps.Keys(o)) {
		dst = pad(dst, indent+1)
		dst = appendKey(dst, k)
		dst = append(dst, ": "...)
		dst = appendValue(dst, o[k], indent+1)
		dst = append(dst, ",\n"...)
	}
	dst = pad(dst, indent)
	return append(dst, '}')
}

// Arr is a JavaScript array literal. Arrays holding only primitives render on
// a single line.
type Arr []any

// AppendJS implements Value.
func (a Arr) AppendJS(dst []byte, indent int) []byte {
	if len(a) == 0 {
		return append(dst, "[]"...)
	}
	if a.flat() {
		dst = append(dst, '[')
		for i, v := range a {
			if i > 0 {
				dst = append(dst, ", "...)
			}
			dst = appendValue(dst, v, indent)
		}
		return append(dst, ']')
	}
	dst = append(dst, "[\n"...)
	for _, v := range a {
		dst = pad(dst, indent+1)
		dst = appendValue(dst, v, indent+1)
		dst = append(dst, ",\n"...)
	}
	dst = pad(dst, indent)
	return append(dst, ']')
}

func (a Arr) flat() bool {
	n := 0
	for _, v := range a {
		switch t := v.(type) {
		case Obj, Arr, map[string]any, []any:
			return false
		case Raw:
			if strings.ContainsAny(string(t), "\n{") {
				return false
			}
			n += len(t)
		case string:
			n += len(t) + 2
		default:
			n += 8
		}
	}
	return n <= 72
}

// Class is an Ext.define statement.
type Class struct {
	Name   string
	Config Obj
}

// Define builds an Ext.define statement for name.
func Define(name string, config Obj) Class {
	return Class{Name: name, Config: config}
}

// AppendJS implements Value.
func (c Class) AppendJS(dst []byte, indent int) []byte {
	dst = append(dst, "Ext.define("...)
	dst = appendValue(dst, c.Name, indent)
	dst = append(dst, ", "...)
	dst = c.Config.AppendJS(dst, indent)
	return append(dst, ");\n"...)
}

// Render concatenates statements into a single JavaScript source file.
func Render(items ...Value) []byte {
	var dst []byte
	for i, it := range items {
		if i > 0 {
			dst = append(dst, '\n')
		}
		dst = it.AppendJS(dst, 0)
	}
	return dst
}

// Func builds a function literal. params is the raw parameter list and body
// the raw statement list, both dedented and reindented on render.
func Func(params, body string) Raw {
	body = strings.TrimRight(dedent(body), "\n")
	return Raw(fmt.Sprintf("function (%s) {\n%s\n}", params, indentLines(body, 1)))
}

// T wraps s in a gettext call.
func T(s string) Raw {
	return Raw("gettext(" + string(quote(s)) + ")")
}

func appendValue(dst []byte, v any, indent int) []byte {
	switch t := v.(type) {
	case nil:
		return append(dst, "null"...)
	case Value:
		return t.AppendJS(dst, indent)
	case map[string]any:
		return Obj(t).AppendJS(dst, indent)
	case []any:
		return Arr(t).AppendJS(dst, indent)
	case string:
		return append(dst, quote(t)...)
	case bool:
		return strconvAppendBool(dst, t)
	case int:
		return fmt.Appendf(dst, "%d", t)
	case int64:
		return fmt.Appendf(dst, "%d", t)
	case float64:
		return fmt.Appendf(dst, "%v", t)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Appendf(dst, "null %s", strings.ReplaceAll(err.Error(), "*/", ""))
		}
		return append(dst, b...)
	}
}

func strconvAppendBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, "true"...)
	}
	return append(dst, "false"...)
}

func quote(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		return []byte(`""`)
	}
	return b
}

func appendKey(dst []byte, k string) []byte {
	if isIdent(k) {
		return append(dst, k...)
	}
	return append(dst, quote(k)...)
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		ok := r == '_' || r == '$' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(i > 0 && r >= '0' && r <= '9')
		if !ok {
			return false
		}
	}
	return true
}

func pad(dst []byte, indent int) []byte {
	return append(dst, strings.Repeat("  ", indent)...)
}

func reindent(src string, indent int) string {
	return indentLines(strings.TrimRight(dedent(src), "\n"), indent)[indent*2:]
}

// dedent strips the smallest common leading whitespace from every non-blank
// line, so Go raw string literals can stay indented in the source.
func dedent(src string) string {
	src = strings.TrimLeft(src, "\n")
	lines := strings.Split(src, "\n")
	prefix := -1
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		n := len(l) - len(strings.TrimLeft(l, " \t"))
		if prefix < 0 || n < prefix {
			prefix = n
		}
	}
	if prefix <= 0 {
		return src
	}
	for i, l := range lines {
		if len(l) >= prefix {
			lines[i] = l[prefix:]
		} else {
			lines[i] = strings.TrimSpace(l)
		}
	}
	return strings.Join(lines, "\n")
}

func indentLines(src string, indent int) string {
	if indent == 0 {
		return src
	}
	p := strings.Repeat("  ", indent)
	var b bytes.Buffer
	for i, l := range strings.Split(src, "\n") {
		if i > 0 {
			b.WriteByte('\n')
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		b.WriteString(p)
		b.WriteString(l)
	}
	return b.String()
}
