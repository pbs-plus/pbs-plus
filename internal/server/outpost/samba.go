//go:build linux

package outpost

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// runSmbcontrol shells out (Samba has no stable reload API) and is faked in tests.
var runSmbcontrol = func(args ...string) error {
	if err := exec.Command("smbcontrol", args...).Run(); err != nil {
		return fmt.Errorf("smbcontrol %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

type sambaDriver struct{}

func (sambaDriver) Type() string { return TypeSamba }

func (sambaDriver) Validate(o Outpost) error { return nil }

func (sambaDriver) Start(ctx context.Context, o Outpost) (Instance, error) {
	if _, err := exec.LookPath("smbcontrol"); err != nil {
		return nil, fmt.Errorf("smbcontrol not found: install samba and add 'include = %s' to smb.conf [global]", sambaIncludePath(o.Name))
	}
	if err := runSmbcontrol("smbd", "ping"); err != nil {
		return nil, fmt.Errorf("smbd is not running: %w", err)
	}
	return &sambaInstance{name: o.Name, shares: map[string]Attachment{}}, nil
}

type sambaInstance struct {
	name string

	mu     sync.Mutex
	shares map[string]Attachment
}

func (s *sambaInstance) Attach(a Attachment) error {
	if a.Path == "" {
		return fmt.Errorf("samba attachments need a local path")
	}
	s.mu.Lock()
	s.shares[a.Name] = a
	s.mu.Unlock()
	if err := s.sync(); err != nil {
		s.mu.Lock()
		delete(s.shares, a.Name)
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *sambaInstance) Detach(name string) error {
	s.mu.Lock()
	_, ok := s.shares[name]
	delete(s.shares, name)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return s.sync()
}

func (s *sambaInstance) Attached() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.shares))
	for name := range s.shares {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *sambaInstance) Endpoint(share string) string {
	s.mu.Lock()
	_, ok := s.shares[share]
	s.mu.Unlock()
	if !ok {
		return ""
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("smb://%s/%s", host, share)
}

func (s *sambaInstance) Stop() error {
	s.mu.Lock()
	s.shares = map[string]Attachment{}
	s.mu.Unlock()
	return s.sync()
}

// sync rewrites the managed include file (admin smb.conf keeps one include line) and reloads smbd.
func (s *sambaInstance) sync() error {
	s.mu.Lock()
	names := make([]string, 0, len(s.shares))
	for name := range s.shares {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "# managed by pbs-plus; include from smb.conf [global]\n")
	for _, name := range names {
		a := s.shares[name]
		fmt.Fprintf(&b, "\n[%s]\n", name)
		fmt.Fprintf(&b, "\tpath = %s\n", a.Path)
		if a.ReadOnly {
			fmt.Fprintf(&b, "\tread only = yes\n")
		} else {
			fmt.Fprintf(&b, "\tread only = no\n")
		}
	}
	s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(sambaIncludePath(s.name)), 0o755); err != nil {
		return fmt.Errorf("create samba include dir: %w", err)
	}
	if err := os.WriteFile(sambaIncludePath(s.name), []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write samba include file: %w", err)
	}
	return runSmbcontrol("smbd", "reload-config")
}

func sambaIncludePath(name string) string {
	return filepath.Join(conf.StatePrefix, "outposts", "samba-"+name+".conf")
}
