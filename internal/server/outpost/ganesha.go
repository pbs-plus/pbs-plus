//go:build linux

package outpost

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	godbus "github.com/godbus/dbus/v5"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
)

const (
	ganeshaBusName    = "org.ganesha.nfsd"
	ganeshaObjectPath = "/org/ganesha/nfsd/ExportMgr"
	ganeshaInterface  = "org.ganesha.nfsd.exportmgr"
	ganeshaTimeout    = 15 * time.Second
)

// ganeshaExportMgr is the exportmgr D-Bus surface the driver drives, fakeable in tests.
type ganeshaExportMgr interface {
	Ping() error
	AddExport(confPath, expr string) error
	RemoveExport(id uint16) error
}

type ganeshaBus struct {
	obj godbus.BusObject
}

func (b ganeshaBus) call(method string, args ...any) error {
	ctx, cancel := context.WithTimeout(context.Background(), ganeshaTimeout)
	defer cancel()
	return b.obj.CallWithContext(ctx, method, 0, args...).Err
}

func (b ganeshaBus) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), ganeshaTimeout)
	defer cancel()
	return b.obj.CallWithContext(ctx, "org.freedesktop.DBus.Peer.Ping", 0).Err
}

func (b ganeshaBus) AddExport(confPath, expr string) error {
	return b.call(ganeshaInterface+".AddExport", confPath, expr)
}

func (b ganeshaBus) RemoveExport(id uint16) error {
	return b.call(ganeshaInterface+".RemoveExport", id)
}

var dialGanesha = func() (ganeshaExportMgr, error) {
	conn, err := godbus.SystemBus()
	if err != nil {
		return nil, err
	}
	return ganeshaBus{obj: conn.Object(ganeshaBusName, ganeshaObjectPath)}, nil
}

type ganeshaDriver struct{}

func (ganeshaDriver) Type() string { return TypeGanesha }

func (ganeshaDriver) Validate(o Outpost) error {
	for _, st := range ganeshaSectypes(o.Sectype) {
		switch st {
		case "sys", "krb5", "krb5i", "krb5p":
		default:
			return fmt.Errorf("invalid sectype %q (allowed: sys, krb5, krb5i, krb5p)", st)
		}
	}
	return nil
}

// ganeshaSectypes defaults to krb5i, the minimum safe flavor for a Windows-domain NFS frontend.
func ganeshaSectypes(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{"krb5i"}
	}
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func (ganeshaDriver) Start(ctx context.Context, o Outpost) (Instance, error) {
	bus, err := dialGanesha()
	if err != nil {
		return nil, fmt.Errorf("connect to system dbus: %w", err)
	}
	if err := bus.Ping(); err != nil {
		return nil, fmt.Errorf("nfs-ganesha is not reachable on dbus as %s (install nfs-ganesha and start it with dbus): %w", ganeshaBusName, err)
	}
	inst := &ganeshaInstance{
		name:    o.Name,
		sectype: strings.Join(ganeshaSectypes(o.Sectype), ","),
		bus:     bus,
		exports: map[string]ganeshaExport{},
	}
	if err := inst.clearFragments(); err != nil {
		return nil, err
	}
	return inst, nil
}

type ganeshaExport struct {
	id   uint16
	path string
}

type ganeshaInstance struct {
	name    string
	sectype string
	bus     ganeshaExportMgr

	mu      sync.Mutex
	exports map[string]ganeshaExport
}

func (g *ganeshaInstance) Attach(a Attachment) error {
	if a.Path == "" {
		return fmt.Errorf("ganesha attachments need a local path")
	}
	id := ganeshaExportID(g.name, a.Name)
	frag := ganeshaFragmentPath(g.name, a.Name)
	access := "RW"
	if a.ReadOnly {
		access = "RO"
	}
	if err := os.MkdirAll(filepath.Dir(frag), 0o700); err != nil {
		return fmt.Errorf("create ganesha export dir: %w", err)
	}
	if err := os.WriteFile(frag, []byte(ganeshaFragment(id, a.Path, a.Name, access, g.sectype)), 0o600); err != nil {
		return fmt.Errorf("write ganesha export fragment: %w", err)
	}
	if err := g.bus.AddExport(frag, ""); err != nil {
		_ = os.Remove(frag)
		return fmt.Errorf("ganesha AddExport %s: %w", frag, err)
	}
	g.mu.Lock()
	g.exports[a.Name] = ganeshaExport{id: id, path: a.Path}
	g.mu.Unlock()
	return nil
}

func (g *ganeshaInstance) Detach(name string) error {
	g.mu.Lock()
	exp, ok := g.exports[name]
	delete(g.exports, name)
	g.mu.Unlock()
	if !ok {
		return nil
	}
	err := g.bus.RemoveExport(exp.id)
	_ = os.Remove(ganeshaFragmentPath(g.name, name))
	return err
}

func (g *ganeshaInstance) Attached() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	names := make([]string, 0, len(g.exports))
	for name := range g.exports {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g *ganeshaInstance) Endpoint(share string) string {
	g.mu.Lock()
	exp, ok := g.exports[share]
	g.mu.Unlock()
	if !ok {
		return ""
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("nfs://%s%s", host, exp.path)
}

func (g *ganeshaInstance) Stop() error {
	g.mu.Lock()
	exports := make([]ganeshaExport, 0, len(g.exports))
	for _, exp := range g.exports {
		exports = append(exports, exp)
	}
	g.exports = map[string]ganeshaExport{}
	g.mu.Unlock()
	var errs []error
	for _, exp := range exports {
		if err := g.bus.RemoveExport(exp.id); err != nil {
			errs = append(errs, err)
		}
	}
	if err := g.clearFragments(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// clearFragments drops stale fragment files; exports from a crashed server
// stay registered in ganesha until restart (deterministic ids let re-Attach replace them).
// ponytail: no ShowExports reconcile; add one if stale exports after crashes bite.
func (g *ganeshaInstance) clearFragments() error {
	return os.RemoveAll(ganeshaExportsDir(g.name))
}

func ganeshaExportsDir(name string) string {
	return filepath.Join(conf.StatePrefix, "outposts", "ganesha-exports", name)
}

func ganeshaFragmentPath(outpostName, share string) string {
	return filepath.Join(ganeshaExportsDir(outpostName), share+".conf")
}

// ganeshaExportID derives a stable id in 4096..65535 so restarts address the
// same export without persisted state; collisions with other exports fail AddExport.
func ganeshaExportID(outpostName, share string) uint16 {
	raw, err := hex.DecodeString(crypto.SHA256Hex([]byte(outpostName + "/" + share)))
	if err != nil || len(raw) < 4 {
		return 4096
	}
	return 4096 + uint16(binary.BigEndian.Uint32(raw[:4])%60000)
}

func ganeshaFragment(id uint16, path, share, access, sectype string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "EXPORT {\n")
	fmt.Fprintf(&b, "\tExport_Id = %d;\n", id)
	fmt.Fprintf(&b, "\tPath = %s;\n", path)
	fmt.Fprintf(&b, "\tPseudo = /%s;\n", share)
	fmt.Fprintf(&b, "\tAccess_Type = %s;\n", access)
	fmt.Fprintf(&b, "\tSquash = No_Root_Squash;\n")
	fmt.Fprintf(&b, "\tSecType = %s;\n", sectype)
	fmt.Fprintf(&b, "\tProtocols = 3;\n")
	fmt.Fprintf(&b, "\tTransports = TCP;\n")
	fmt.Fprintf(&b, "\tFSAL {\n\t\tName = VFS;\n\t}\n}\n")
	return b.String()
}
