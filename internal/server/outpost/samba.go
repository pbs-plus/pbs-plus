//go:build linux

package outpost

import (
	"context"
	"errors"
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
	path, err := exec.LookPath("smbcontrol")
	if err != nil {
		return fmt.Errorf("smbcontrol not found: %w", err)
	}
	if err := exec.Command(path, args...).Run(); err != nil {
		return fmt.Errorf("smbcontrol %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// runNet shells out to samba's net tool and is faked in tests.
var runNet = func(args ...string) error {
	path, err := exec.LookPath("net")
	if err != nil {
		return fmt.Errorf("net not found: %w", err)
	}
	out, err := exec.Command(path, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("net %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func sambaList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.ReplaceAll(strings.TrimSpace(part), "\\\\", "\\")
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// sambaNeedsDomain reports whether any entry names a domain principal
// (DOMAIN\user, user@REALM, @DOMAIN\group) rather than a local account.
func sambaNeedsDomain(users []string) bool {
	for _, u := range users {
		if strings.ContainsAny(strings.TrimLeft(u, "@+&"), `\@`) {
			return true
		}
	}
	return false
}

type sambaDriver struct{}

func (sambaDriver) Type() string { return TypeSamba }

func (sambaDriver) Validate(o Outpost) error {
	users := sambaList(o.ValidUsers)
	switch {
	case o.Guest && len(users) > 0:
		return fmt.Errorf("samba outpost: guest access and valid users are mutually exclusive")
	case !o.Guest && len(users) == 0:
		return fmt.Errorf("samba outpost: set valid users or enable guest access")
	}
	for _, f := range []struct{ name, value string }{
		{"valid users", o.ValidUsers},
		{"force user", o.ForceUser},
		{"hosts allow", o.HostsAllow},
	} {
		if strings.ContainsAny(f.value, "\n\r[]") {
			return fmt.Errorf("samba outpost: %s must not contain newlines or brackets", f.name)
		}
	}
	if sambaNeedsDomain(users) {
		if err := runNet("ads", "testjoin"); err != nil {
			return fmt.Errorf("samba outpost: valid users names a domain account but this host is not joined to a domain (join it with 'net ads join -U administrator'): %w", err)
		}
	}
	return nil
}

func (sambaDriver) Start(ctx context.Context, o Outpost) (Instance, error) {
	if err := runSmbcontrol("smbd", "ping"); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, fmt.Errorf("smbcontrol not found: install samba and add 'include = %s' to smb.conf [global]: %w", sambaIncludePath(), err)
		}
		return nil, fmt.Errorf("smbd is not running: %w", err)
	}
	inst := &sambaInstance{name: o.Name, cfg: o, shares: map[string]Attachment{}}
	sambaRegMu.Lock()
	sambaLive[o.Name] = inst
	sambaRegMu.Unlock()
	return inst, nil
}

type sambaInstance struct {
	name string
	cfg  Outpost

	mu     sync.Mutex
	shares map[string]Attachment
}

// sambaLive tracks every running samba outpost for the single include file.
var (
	sambaRegMu sync.Mutex
	sambaLive  = map[string]*sambaInstance{}
)

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

// AttachSub ensures the shared share exists at the outpost root path; the
// per-sub FUSE mounts underneath are managed by the caller.
func (s *sambaInstance) AttachSub(share, sub string, a Attachment) error {
	s.mu.Lock()
	existing, ok := s.shares[share]
	s.mu.Unlock()
	if ok {
		if existing.Path != a.Path {
			return fmt.Errorf("share %q on outpost %q is already attached to %s", share, s.name, existing.Path)
		}
		return nil
	}
	return s.Attach(a)
}

// DetachSub is a no-op: the share stays until the last mount detaches it.
func (s *sambaInstance) DetachSub(share, sub string) {}

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
	sambaRegMu.Lock()
	delete(sambaLive, s.name)
	sambaRegMu.Unlock()
	return s.sync()
}

// sync rewrites the one managed include with all samba outposts' shares and reloads smbd.
func (s *sambaInstance) sync() error {
	sambaRegMu.Lock()
	names := make([]string, 0, len(sambaLive))
	for name := range sambaLive {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "# managed by pbs-plus; one include line in smb.conf [global]: include = %s\n", sambaIncludePath())
	for _, name := range names {
		inst := sambaLive[name]
		inst.mu.Lock()
		shareNames := make([]string, 0, len(inst.shares))
		for sn := range inst.shares {
			shareNames = append(shareNames, sn)
		}
		sort.Strings(shareNames)
		for _, sn := range shareNames {
			b.WriteString(sambaShareStanza(inst.cfg, inst.shares[sn]))
		}
		inst.mu.Unlock()
	}
	sambaRegMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(sambaIncludePath()), 0o755); err != nil {
		return fmt.Errorf("create samba include dir: %w", err)
	}
	if err := os.WriteFile(sambaIncludePath(), []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write samba include file: %w", err)
	}
	if legacy, _ := filepath.Glob(filepath.Join(filepath.Dir(sambaIncludePath()), "samba-*.conf")); len(legacy) > 0 {
		for _, f := range legacy {
			_ = os.Remove(f)
		}
	}
	return runSmbcontrol("smbd", "reload-config")
}

func sambaIncludePath() string {
	return filepath.Join(conf.StatePrefix, "outposts", "samba.conf")
}

// sambaShareStanza renders one share, applying the outpost's access policy.
// Shares are hidden by default; Browseable only lists names, access is still
// gated by guest/valid users.
func sambaShareStanza(o Outpost, a Attachment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n[%s]\n", a.Name)
	fmt.Fprintf(&b, "\tpath = %s\n", a.Path)
	browseableVal := "no"
	if o.Browseable {
		browseableVal = "yes"
	}
	fmt.Fprintf(&b, "\tbrowseable = %s\n", browseableVal)
	if a.ReadOnly {
		fmt.Fprintf(&b, "\tread only = yes\n")
	} else {
		fmt.Fprintf(&b, "\tread only = no\n")
	}
	if o.Guest {
		fmt.Fprintf(&b, "\tguest ok = yes\n")
	} else {
		fmt.Fprintf(&b, "\tguest ok = no\n")
	}
	if users := sambaList(o.ValidUsers); len(users) > 0 {
		fmt.Fprintf(&b, "\tvalid users = %s\n", strings.Join(users, ", "))
	}
	if o.ForceUser != "" {
		fmt.Fprintf(&b, "\tforce user = %s\n", o.ForceUser)
	}
	if o.HostsAllow != "" {
		fmt.Fprintf(&b, "\thosts allow = %s\n", o.HostsAllow)
	}
	return b.String()
}
