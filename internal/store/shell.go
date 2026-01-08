//go:build linux

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"mvdan.cc/sh/v3/expand"
	"mvdan.cc/sh/v3/interp"
	"mvdan.cc/sh/v3/syntax"
)

func (s *Store) securityMiddleware(jailDir string, next interp.ExecHandlerFunc) interp.ExecHandlerFunc {
	return func(ctx context.Context, args []string) error {
		if len(args) == 0 {
			return next(ctx, args)
		}

		cmd := args[0]
		if !slices.Contains(s.GetAppConfig().Scripts.AllowedCommands, cmd) {
			return fmt.Errorf("security violation: command '%s' not allowed", cmd)
		}

		hc := interp.HandlerCtx(ctx)
		cwd := hc.Dir

		for _, arg := range args[1:] {
			if strings.HasPrefix(arg, "-") {
				continue
			}

			var targetPath string
			if filepath.IsAbs(arg) {
				targetPath = filepath.Clean(arg)
			} else {
				targetPath = filepath.Join(cwd, arg)
			}

			rel, err := filepath.Rel(jailDir, targetPath)
			if err != nil || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
				if !isWhitelistedSystemPath(targetPath) {
					return fmt.Errorf("security violation: path '%s' is outside working directory", arg)
				}
			}
		}

		return next(ctx, args)
	}
}

func isWhitelistedSystemPath(path string) bool {
	allowed := []string{"/dev/null", "/dev/urandom"}

	return slices.Contains(allowed, path)
}

func (s *Store) RunScript(
	ctx context.Context,
	scriptFilePath string,
	envVars []string,
) (string, map[string]string, error) {
	f, err := os.Open(scriptFilePath)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()

	jailDir, err := os.MkdirTemp("", "pbs-exec-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create jail: %w", err)
	}
	defer os.RemoveAll(jailDir)

	parser := syntax.NewParser()
	file, err := parser.Parse(f, scriptFilePath)
	if err != nil {
		return "", nil, err
	}

	var outBuf bytes.Buffer
	combinedOut := io.MultiWriter(&outBuf)

	runner, err := interp.New(
		interp.StdIO(nil, combinedOut, combinedOut),
		interp.Env(expand.ListEnviron(append(os.Environ(), envVars...)...)),
		interp.Dir(jailDir),
		interp.OpenHandler(func(ctx context.Context, path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
			cleanPath := filepath.Clean(path)
			if !strings.HasPrefix(cleanPath, jailDir) && !isWhitelistedSystemPath(cleanPath) {
				return nil, fmt.Errorf("security violation: opening path %s is forbidden", path)
			}
			return interp.DefaultOpenHandler()(ctx, path, flag, perm)
		}),
		interp.ExecHandlers(func(next interp.ExecHandlerFunc) interp.ExecHandlerFunc {
			return s.securityMiddleware(jailDir, next)
		}),
	)
	if err != nil {
		return "", nil, err
	}

	if err := runner.Run(ctx, file); err != nil {
		return outBuf.String(), nil, fmt.Errorf("execution error: %w", err)
	}

	resultEnvs := make(map[string]string)
	for name, vr := range runner.Vars {
		if vr.Exported {
			resultEnvs[name] = vr.String()
		}
	}

	return outBuf.String(), resultEnvs, nil
}

func (s *Store) ValidateScript(scriptContent string) error {
	parser := syntax.NewParser()
	file, err := parser.Parse(strings.NewReader(scriptContent), "")
	if err != nil {
		return fmt.Errorf("syntax error: %w", err)
	}

	var validationErr error
	syntax.Walk(file, func(node syntax.Node) bool {
		switch x := node.(type) {
		case *syntax.CallExpr:
			if len(x.Args) > 0 {
				cmdName := ""
				if len(x.Args[0].Parts) > 0 {
					if lit, ok := x.Args[0].Parts[0].(*syntax.Lit); ok {
						cmdName = lit.Value
					}
				}

				if cmdName != "" && !slices.Contains(s.GetAppConfig().Scripts.AllowedCommands, cmdName) {
					validationErr = fmt.Errorf("command '%s' is not whitelisted", cmdName)
					return false
				}

				if cmdName == "" {
					validationErr = fmt.Errorf("dynamic commands are forbidden")
					return false
				}
			}
		}
		return true
	})

	return validationErr
}

func (s *Store) SaveScript(scriptContent string) (string, error) {
	if err := os.MkdirAll(constants.ScriptsBasePath, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	tmpfile, err := os.CreateTemp(constants.ScriptsBasePath, "script-*.sh")
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer tmpfile.Close()

	if _, err := tmpfile.WriteString(scriptContent); err != nil {
		os.Remove(tmpfile.Name())
		return "", fmt.Errorf("failed to write script: %w", err)
	}

	_ = os.Chmod(tmpfile.Name(), 0755)
	return tmpfile.Name(), nil
}

func (s *Store) UpdateScript(filePath string, newScriptContent string) error {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	if !strings.HasPrefix(absPath, constants.ScriptsBasePath) {
		return fmt.Errorf("invalid file path: outside allowed directory")
	}

	if err := s.ValidateScript(newScriptContent); err != nil {
		return err
	}

	return os.WriteFile(absPath, []byte(newScriptContent), 0644)
}

func (s *Store) ReadScript(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return string(content), nil
}
