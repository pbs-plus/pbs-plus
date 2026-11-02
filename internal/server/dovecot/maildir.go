//go:build linux

package dovecot

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const userDirNameLimit = 64

// UserDirName maps a mailbox username to its archive directory. The hash keeps
// the name unique after unsupported characters are folded away.
func UserDirName(username string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_', r == '@':
			return r
		default:
			return '_'
		}
	}, username)
	safe = strings.TrimLeft(safe, ".")
	if len(safe) > userDirNameLimit {
		safe = safe[:userDirNameLimit]
	}
	if safe == "" {
		safe = "user"
	}
	sum := sha256.Sum256([]byte(username))
	return safe + "-" + hex.EncodeToString(sum[:4])
}

func validateUserDir(dir string) error {
	if dir == "" {
		return nil
	}
	if dir != filepath.Base(dir) || dir == "." || dir == ".." || strings.HasPrefix(dir, ".") {
		return fmt.Errorf("user directory %q is unsupported", dir)
	}
	return nil
}

// mailboxFolder resolves a mailbox name to its Maildir++ folder: INBOX is the
// Maildir root itself, every other mailbox is a dot-prefixed sibling.
func mailboxFolder(root, mailbox string) string {
	if mailbox == "" || strings.EqualFold(mailbox, "INBOX") {
		return root
	}
	return filepath.Join(root, "."+strings.ReplaceAll(mailbox, "/", "."))
}

// buildSelection stages the selected messages as a throwaway Maildir holding
// only the destination mailbox, which dsync then merges into the destination
// user. It returns the staging root and the mailbox to transfer.
func buildSelection(mailDir, sourceMailbox, destinationMailbox string, messages []string) (_ string, _ string, err error) {
	mailbox := destinationMailbox
	if mailbox == "" {
		mailbox = sourceMailbox
	}
	sources, err := selectionSources(mailDir, sourceMailbox, messages)
	if err != nil {
		return "", "", err
	}
	if len(sources) == 0 {
		return "", "", errors.New("Dovecot message selection matched no messages")
	}

	root, err := os.MkdirTemp("", ".pbs-plus-dovecot-selection-")
	if err != nil {
		return "", "", fmt.Errorf("create Dovecot selection directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
		}
	}()
	if err = os.Chmod(root, 0o700); err != nil {
		return "", "", fmt.Errorf("secure Dovecot selection directory: %w", err)
	}
	selectionMailDir := filepath.Join(root, mailDirName)
	folder := mailboxFolder(selectionMailDir, mailbox)
	for _, dir := range []string{selectionMailDir, folder} {
		for _, sub := range []string{"cur", "new", "tmp"} {
			if err = os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
				return "", "", fmt.Errorf("create Dovecot selection mailbox: %w", err)
			}
		}
	}
	for _, source := range sources {
		sub := "cur"
		if filepath.Base(filepath.Dir(source)) == "new" {
			sub = "new"
		}
		if err = copyMessage(source, filepath.Join(folder, sub, filepath.Base(source))); err != nil {
			return "", "", err
		}
	}
	return root, mailbox, nil
}

func selectionSources(mailDir, sourceMailbox string, messages []string) ([]string, error) {
	if len(messages) == 0 {
		folder := mailboxFolder(mailDir, sourceMailbox)
		var sources []string
		for _, sub := range []string{"cur", "new"} {
			entries, err := os.ReadDir(filepath.Join(folder, sub))
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("read Dovecot backup mailbox: %w", err)
			}
			for _, entry := range entries {
				if entry.Type().IsRegular() {
					sources = append(sources, filepath.Join(folder, sub, entry.Name()))
				}
			}
		}
		return sources, nil
	}

	sources := make([]string, 0, len(messages))
	for _, message := range messages {
		path, err := messagePath(mailDir, message)
		if err != nil {
			return nil, err
		}
		sources = append(sources, path)
	}
	return sources, nil
}

// messagePath keeps a selected message inside the backed-up Maildir, so a
// crafted job cannot read files elsewhere on the server.
func messagePath(mailDir, message string) (string, error) {
	cleaned := filepath.Clean("/" + strings.TrimSpace(message))
	path := filepath.Join(mailDir, cleaned)
	relative, err := filepath.Rel(mailDir, path)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		return "", fmt.Errorf("Dovecot message %q is outside the backup", message)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("read Dovecot message %q: %w", message, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("Dovecot message %q is not a regular file", message)
	}
	return path, nil
}

// copyMessage preserves the modification time because Maildir derives the
// received date of a message from it.
func copyMessage(source, destination string) error {
	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("read Dovecot message: %w", err)
	}
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("read Dovecot message: %w", err)
	}
	defer func() {
		_ = in.Close()
	}()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write Dovecot message: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("write Dovecot message: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("write Dovecot message: %w", err)
	}
	if err := os.Chtimes(destination, info.ModTime(), info.ModTime()); err != nil {
		return fmt.Errorf("write Dovecot message: %w", err)
	}
	return nil
}
