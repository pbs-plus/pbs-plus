package log_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// slogAllowed packages run before the shared logger exists or are imported by it.
var slogAllowed = []string{
	"internal/log",
	"internal/conf",
	"internal/proxmox/tasklog",
}

// enforced lists the trees held to the convention; the rest predate it and are converted package by package.
var enforced = []string{
	"internal/log",
	"internal/proxmox",
	"internal/server/backup",
	"internal/server/restore",
	"internal/server/jobs",
	"internal/server/vfs",
	"internal/server/rpc",
}

func isEnforced(path string) bool {
	for _, tree := range enforced {
		if strings.HasPrefix(path, tree+"/") {
			return true
		}
	}
	return false
}

// properNouns stay capitalized at the start of a message or error string.
var properNouns = []string{
	"ACL", "API", "CA", "CBOR", "CPU", "CSRF", "DN", "Dovecot", "FUSE", "GID", "HTTP",
	"IMAP", "JSON", "LDAP", "MTF", "MariaDB", "MySQL", "NFS", "PBS", "PEM", "PID",
	"PostgreSQL", "Proxmox", "QUIC", "RPC", "S3", "SMTP", "SOGo", "TLS", "UID", "UPID",
	"URL", "UTF", "VSS", "Windows", "XML", "ZIP",
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source file")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

func startsWithProperNoun(s string) bool {
	for _, noun := range properNouns {
		if strings.HasPrefix(s, noun) {
			return true
		}
	}
	return false
}

func literal(node ast.Expr) (string, bool) {
	lit, ok := node.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

func callName(call *ast.CallExpr) (recv, method string, ok bool) {
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel {
		return "", "", false
	}
	switch x := sel.X.(type) {
	case *ast.Ident:
		return x.Name, sel.Sel.Name, true
	case *ast.SelectorExpr:
		return x.Sel.Name, sel.Sel.Name, true
	}
	return "", "", false
}

type violation struct {
	pos  string
	rule string
	text string
}

func scanRepo(t *testing.T, visit func(path string, fset *token.FileSet, file *ast.File)) {
	t.Helper()
	root := repoRoot(t)
	internal := filepath.Join(root, "internal")

	err := filepath.WalkDir(internal, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "corequery" || d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		parsed, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)
		if !isEnforced(rel) {
			return nil
		}
		visit(rel, fset, parsed)
		return nil
	})
	if err != nil {
		t.Fatalf("walking internal: %v", err)
	}
}

func report(t *testing.T, found []violation) {
	t.Helper()
	if len(found) == 0 {
		return
	}
	var sb strings.Builder
	for _, v := range found {
		sb.WriteString("\n  " + v.pos + ": " + v.rule)
		if v.text != "" {
			sb.WriteString(" -> " + strconv.Quote(v.text))
		}
	}
	t.Errorf("%d violation(s):%s", len(found), sb.String())
}

func TestLogMessagesFollowTheHouseStyle(t *testing.T) {
	var found []violation

	scanRepo(t, func(path string, fset *token.FileSet, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			recv, method, ok := callName(call)
			if !ok || !strings.Contains(strings.ToLower(recv), "log") {
				return true
			}

			var msgIndex int
			switch method {
			case "Error":
				msgIndex = 1
			case "Info", "Warn", "Debug":
				msgIndex = 0
			default:
				return true
			}
			if len(call.Args) <= msgIndex {
				return true
			}
			msg, ok := literal(call.Args[msgIndex])
			if !ok {
				return true
			}
			pos := path + ":" + strconv.Itoa(fset.Position(call.Pos()).Line)

			if msg == "" {
				found = append(found, violation{pos, "log call has an empty message", ""})
				return true
			}
			if strings.HasSuffix(msg, ".") {
				found = append(found, violation{pos, "log message ends with a period", msg})
			}
			if first := []rune(msg)[0]; unicode.IsUpper(first) && !startsWithProperNoun(msg) {
				found = append(found, violation{pos, "log message starts with an uppercase word", msg})
			}
			if head, _, cut := strings.Cut(msg, ": "); cut && !strings.Contains(head, " ") && head != strings.ToLower(head) {
				found = append(found, violation{pos, "log message carries a Go identifier prefix", msg})
			}
			return true
		})
	})

	report(t, found)
}

func TestErrorStringsFollowGoStyle(t *testing.T) {
	var found []violation

	scanRepo(t, func(path string, fset *token.FileSet, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			pkg, fn, ok := callName(call)
			if !ok || pkg != "errors" && pkg != "fmt" {
				return true
			}
			if fn != "New" && fn != "Errorf" {
				return true
			}
			if len(call.Args) == 0 {
				return true
			}
			msg, ok := literal(call.Args[0])
			if !ok || msg == "" {
				return true
			}
			pos := path + ":" + strconv.Itoa(fset.Position(call.Pos()).Line)

			if strings.HasSuffix(msg, ".") {
				found = append(found, violation{pos, "error string ends with a period", msg})
			}
			if first := []rune(msg)[0]; unicode.IsUpper(first) && !startsWithProperNoun(msg) {
				found = append(found, violation{pos, "error string starts with an uppercase word", msg})
			}
			if fn == "Errorf" && strings.Contains(msg, "%v") {
				for _, arg := range call.Args[1:] {
					if ident, isIdent := arg.(*ast.Ident); isIdent && strings.Contains(strings.ToLower(ident.Name), "err") {
						found = append(found, violation{pos, "error wrapped with %v instead of %w", msg})
						break
					}
				}
			}
			return true
		})
	})

	report(t, found)
}

func TestSlogIsNotUsedOutsideBootstrapPackages(t *testing.T) {
	var found []violation

	scanRepo(t, func(path string, fset *token.FileSet, file *ast.File) {
		dir := filepath.ToSlash(filepath.Dir(path))
		if slices.Contains(slogAllowed, dir) {
			return
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			pkg, fn, ok := callName(call)
			if !ok || pkg != "slog" {
				return true
			}
			switch fn {
			case "Error", "Warn", "Info", "Debug":
				found = append(found, violation{
					path + ":" + strconv.Itoa(fset.Position(call.Pos()).Line),
					"use the shared log package instead of slog." + fn,
					"",
				})
			}
			return true
		})
	})

	report(t, found)
}
