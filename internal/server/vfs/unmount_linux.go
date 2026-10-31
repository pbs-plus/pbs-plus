//go:build linux

package vfs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ForceUnmount detaches only an existing mount and bounds the external commands to ten seconds.
func ForceUnmount(ctx context.Context, mountpoint string) error {
	if mountpoint == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mounted, err := isMounted(mountpoint)
	if err != nil || !mounted {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return forceUnmount(ctx, mountpoint, func(ctx context.Context, name string, args ...string) error {
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.WaitDelay = time.Second
		return cmd.Run()
	})
}

func forceUnmount(ctx context.Context, mountpoint string, run func(context.Context, string, ...string) error) error {
	var errs []error
	for _, args := range [][]string{
		{"fusermount3", "-uz", mountpoint},
		{"fusermount", "-uz", mountpoint},
		{"umount", "-lf", mountpoint},
	} {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := run(ctx, args[0], args[1:]...); err == nil {
			return nil
		} else {
			errs = append(errs, fmt.Errorf("%s: %w", args[0], err))
		}
	}
	return fmt.Errorf("failed to detach %s: %w", mountpoint, errors.Join(errs...))
}

func isMounted(mountpoint string) (mounted bool, err error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	mountpoint = filepath.Clean(mountpoint)
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && unescape.Replace(fields[4]) == mountpoint {
			return true, nil
		}
	}
	return false, scanner.Err()
}
