//go:build linux

// Package outpost manages typed serving endpoints that attached mounts are
// exposed through as shares. Drivers are registered per type so further
// outpost kinds (e.g. an S3 endpoint) can be added without touching mount
// workflows.
package outpost

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	TypeNFS   = "nfs"
	TypeSamba = "samba"
)

// Outpost is the persisted configuration of one serving endpoint.
type Outpost struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	ListenAddr string `json:"listen_addr,omitempty"`
	Guest      bool   `json:"guest,omitempty"`
	ValidUsers string `json:"valid_users,omitempty"`
	ForceUser  string `json:"force_user,omitempty"`
	HostsAllow string `json:"hosts_allow,omitempty"`
	CreatedAt  int64  `json:"created_at"`
}

// Attachment is a share served by an outpost: FS serves in process (nfs driver), Path backs VFS drivers (samba).
type Attachment struct {
	Name     string
	ReadOnly bool
	FS       billy.Filesystem
	Path     string
	Release  func()
}

// Driver implements one outpost type.
type Driver interface {
	Type() string
	Validate(o Outpost) error
	Start(ctx context.Context, o Outpost) (Instance, error)
}

// Instance is a running outpost endpoint.
type Instance interface {
	Attach(a Attachment) error
	Detach(name string) error
	Attached() []string
	Endpoint(share string) string
	Stop() error
}

// drivers registers the available outpost types.
var drivers = map[string]Driver{
	TypeNFS:   nfsDriver{},
	TypeSamba: sambaDriver{},
}

func DriverTypes() []string {
	types := make([]string, 0, len(drivers))
	for t := range drivers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

func ValidateOutpost(o Outpost) error {
	if !nameRe.MatchString(o.Name) {
		return fmt.Errorf("invalid outpost name %q: lowercase alphanumerics and dashes, max 32 chars", o.Name)
	}
	driver, ok := drivers[o.Type]
	if !ok {
		return fmt.Errorf("unknown outpost type %q (available: %s)", o.Type, strings.Join(DriverTypes(), ", "))
	}
	return driver.Validate(o)
}

// IsValidName reports whether name is a valid outpost name.
func IsValidName(name string) bool { return nameRe.MatchString(name) }

func outpostsDir() string { return filepath.Join(conf.StatePrefix, "outposts") }

func SaveOutpost(o Outpost) error {
	if o.CreatedAt == 0 {
		o.CreatedAt = time.Now().Unix()
	}
	if err := ValidateOutpost(o); err != nil {
		return err
	}
	if err := os.MkdirAll(outpostsDir(), 0o700); err != nil {
		return fmt.Errorf("create outposts dir: %w", err)
	}
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outpostsDir(), o.Name+".json"), data, 0o600)
}

func LoadOutpost(name string) (Outpost, bool, error) {
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return Outpost{}, false, fmt.Errorf("invalid outpost name")
	}
	data, err := os.ReadFile(filepath.Join(outpostsDir(), name+".json"))
	if err != nil {
		if os.IsNotExist(err) {
			return Outpost{}, false, nil
		}
		return Outpost{}, false, err
	}
	var o Outpost
	if err := json.Unmarshal(data, &o); err != nil {
		return Outpost{}, false, fmt.Errorf("decoding outpost %s: %w", name, err)
	}
	return o, true, nil
}

func DeleteOutpost(name string) error {
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return fmt.Errorf("invalid outpost name")
	}
	return os.Remove(filepath.Join(outpostsDir(), name+".json"))
}

func ListOutposts() ([]Outpost, error) {
	entries, err := os.ReadDir(outpostsDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	outposts := make([]Outpost, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		o, ok, err := LoadOutpost(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		if ok {
			outposts = append(outposts, o)
		}
	}
	sort.Slice(outposts, func(i, j int) bool { return outposts[i].Name < outposts[j].Name })
	return outposts, nil
}

// Status reports an outpost's runtime state for the API and UI.
type Status struct {
	Outpost
	Running   bool     `json:"running"`
	Error     string   `json:"error,omitempty"`
	Attached  []string `json:"attached,omitempty"`
	Endpoints []string `json:"endpoints,omitempty"`
}

type manager struct {
	mu        sync.RWMutex
	instances map[string]Instance
	attached  map[string]map[string]Attachment
	errs      map[string]error
}

var mgr = &manager{
	instances: map[string]Instance{},
	attached:  map[string]map[string]Attachment{},
	errs:      map[string]error{},
}

// StartAll starts an instance for every persisted outpost. Outposts that
// fail to start are reported in Status instead of aborting the rest.
func StartAll(ctx context.Context) {
	outposts, err := ListOutposts()
	if err != nil {
		log.Error(err, "listing outposts")
		return
	}
	for _, o := range outposts {
		if err := startOne(ctx, o); err != nil {
			log.Error(err, "starting outpost "+o.Name)
			mgr.mu.Lock()
			mgr.errs[o.Name] = err
			mgr.mu.Unlock()
		}
	}
}

func startOne(ctx context.Context, o Outpost) error {
	driver, ok := drivers[o.Type]
	if !ok {
		return fmt.Errorf("unknown outpost type %q", o.Type)
	}
	inst, err := driver.Start(ctx, o)
	if err != nil {
		return err
	}
	mgr.mu.Lock()
	mgr.instances[o.Name] = inst
	delete(mgr.errs, o.Name)
	mgr.mu.Unlock()
	return nil
}

// ApplyConfig persists o and restarts its instance if it was running.
func ApplyConfig(ctx context.Context, o Outpost) error {
	if err := SaveOutpost(o); err != nil {
		return err
	}
	mgr.mu.Lock()
	_, running := mgr.instances[o.Name]
	mgr.mu.Unlock()
	if running {
		StopOutpost(o.Name)
	}
	return startOne(ctx, o)
}

// StopOutpost stops the instance and releases all its attachments.
func StopOutpost(name string) {
	mgr.mu.Lock()
	inst := mgr.instances[name]
	delete(mgr.instances, name)
	atts := mgr.attached[name]
	delete(mgr.attached, name)
	mgr.mu.Unlock()

	if inst != nil {
		if err := inst.Stop(); err != nil {
			log.Error(err, "stopping outpost "+name)
		}
	}
	for _, a := range atts {
		if a.Release != nil {
			a.Release()
		}
	}
}

// StopAll stops every running outpost; used on server shutdown.
func StopAll() {
	mgr.mu.Lock()
	names := make([]string, 0, len(mgr.instances))
	for name := range mgr.instances {
		names = append(names, name)
	}
	mgr.mu.Unlock()
	for _, name := range names {
		StopOutpost(name)
	}
}

// EndpointOf returns the client-facing locator of a share on a running
// outpost, or "" when the outpost is not running.
func EndpointOf(outpostName, share string) string {
	mgr.mu.RLock()
	inst := mgr.instances[outpostName]
	mgr.mu.RUnlock()
	if inst == nil {
		return ""
	}
	return inst.Endpoint(share)
}

func Attach(outpostName string, a Attachment) error {
	if a.Name == "" || (a.FS == nil && a.Path == "") {
		return fmt.Errorf("attachment needs a name and a filesystem or path")
	}
	mgr.mu.RLock()
	inst := mgr.instances[outpostName]
	mgr.mu.RUnlock()
	if inst == nil {
		return fmt.Errorf("outpost %q is not running", outpostName)
	}
	Detach(outpostName, a.Name)
	if err := inst.Attach(a); err != nil {
		return err
	}
	mgr.mu.Lock()
	if mgr.attached[outpostName] == nil {
		mgr.attached[outpostName] = map[string]Attachment{}
	}
	mgr.attached[outpostName][a.Name] = a
	mgr.mu.Unlock()
	return nil
}

// Detach removes the named share and releases its underlying stack.
func Detach(outpostName, name string) {
	mgr.mu.RLock()
	inst := mgr.instances[outpostName]
	mgr.mu.RUnlock()

	mgr.mu.Lock()
	att, ok := mgr.attached[outpostName][name]
	delete(mgr.attached[outpostName], name)
	mgr.mu.Unlock()

	if inst != nil {
		if err := inst.Detach(name); err != nil {
			log.Error(err, "detaching "+name+" from outpost "+outpostName)
		}
	}
	if ok && att.Release != nil {
		att.Release()
	}
}

// StatusAll reports the runtime state of every configured outpost.
func StatusAll() []Status {
	outposts, err := ListOutposts()
	if err != nil {
		log.Error(err, "listing outposts")
		return nil
	}
	statuses := make([]Status, 0, len(outposts))
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for _, o := range outposts {
		st := Status{Outpost: o}
		if err := mgr.errs[o.Name]; err != nil {
			st.Error = err.Error()
		}
		if inst := mgr.instances[o.Name]; inst != nil {
			st.Running = true
			st.Attached = inst.Attached()
			for _, share := range st.Attached {
				st.Endpoints = append(st.Endpoints, inst.Endpoint(share))
			}
		}
		statuses = append(statuses, st)
	}
	return statuses
}
