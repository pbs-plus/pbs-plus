//go:build linux

package outpost

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pbs-plus/pbs-plus/internal/log"
	nfs "github.com/willscott/go-nfs"
)

// nfsHandleLimit caps the file handles kept alive for one outpost; handles
// are the NFS equivalent of FUSE inodes.
const nfsHandleLimit = 1 << 20

// NFSv3 with null auth has no authentication: any client that can reach the
// listen address can mount any share. Restrict access at the network layer.
type nfsDriver struct{}

func (nfsDriver) Type() string { return TypeNFS }

func (nfsDriver) Validate(o Outpost) error {
	if o.ListenAddr == "" {
		return errors.New("listen_addr is required for nfs outposts")
	}
	if _, port, err := net.SplitHostPort(o.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen_addr: %w", err)
	} else if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid listen_addr port %q", port)
	}
	return nil
}

func (nfsDriver) Start(ctx context.Context, o Outpost) (Instance, error) {
	ln, err := net.Listen("tcp", o.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("nfs outpost listen: %w", err)
	}
	inst := &nfsInstance{
		outpost: o,
		ln:      ln,
		shares:  make(map[string]billy.Filesystem),
		done:    make(chan error, 1),
	}
	handles, _ := lru.New[uuid.UUID, handleEntry](nfsHandleLimit)
	verifiers, _ := lru.New[uint64, verifierEntry](nfsHandleLimit)
	inst.mux = &muxHandler{
		inst:      inst,
		handles:   handles,
		reverse:   make(map[string][]uuid.UUID),
		verifiers: verifiers,
	}

	go func() {
		err := nfs.Serve(ln, inst.mux)
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		if err != nil {
			log.Error(err, "nfs outpost "+o.Name)
		}
		inst.done <- err
	}()
	return inst, nil
}

// nfsInstance is one running NFSv3 server serving every attached mount as a
// separate export path.
type nfsInstance struct {
	outpost Outpost
	ln      net.Listener
	mux     *muxHandler
	done    chan error

	mu     sync.RWMutex
	shares map[string]billy.Filesystem
}

// MaxShareName bounds share names so share-prefixed file handles stay under
// the NFSv3 64-byte handle limit (2-byte prefix + name + 16-byte uuid).
const MaxShareName = 46

func (i *nfsInstance) Attach(a Attachment) error {
	if strings.ContainsAny(a.Name, "/\\") || a.Name == "." || a.Name == ".." || len(a.Name) > MaxShareName {
		return fmt.Errorf("invalid share name %q", a.Name)
	}
	i.mu.Lock()
	i.shares[a.Name] = a.FS
	i.mu.Unlock()
	return nil
}

// Detach drops the share; handles issued for it resolve to ESTALE on next
// use, the correct NFS semantic for a removed export.
func (i *nfsInstance) Detach(name string) error {
	i.mu.Lock()
	delete(i.shares, name)
	i.mu.Unlock()
	return nil
}

func (i *nfsInstance) Attached() []string {
	i.mu.RLock()
	names := make([]string, 0, len(i.shares))
	for name := range i.shares {
		names = append(names, name)
	}
	i.mu.RUnlock()
	sort.Strings(names)
	return names
}

func (i *nfsInstance) Endpoint(share string) string {
	return "nfs://" + i.ln.Addr().String() + "/" + share
}

func (i *nfsInstance) Stop() error {
	return i.ln.Close()
}

func (i *nfsInstance) shareOf(fs billy.Filesystem) (string, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for name, candidate := range i.shares {
		if candidate == fs {
			return name, true
		}
	}
	return "", false
}

type handleEntry struct {
	fs    billy.Filesystem
	share string
	p     []string
}

type verifierEntry struct {
	path     string
	contents []fs.FileInfo
}

// muxHandler routes each NFS export path to the attached share it names and
// owns a share-scoped handle cache: handle bytes carry the share name, so
// handles can never cross shares even when two shares' filesystems would
// compare equal, and handles for detached or re-attached shares go stale.
type muxHandler struct {
	inst      *nfsInstance
	handles   *lru.Cache[uuid.UUID, handleEntry]
	revMu     sync.Mutex
	reverse   map[string][]uuid.UUID
	verifiers *lru.Cache[uint64, verifierEntry]
}

func (h *muxHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	share := strings.Trim(path.Clean("/"+strings.Trim(string(req.Dirpath), "/")), "/")
	if share == "" || strings.Contains(share, "/") {
		return nfs.MountStatusErrNoEnt, nil, nil
	}
	h.inst.mu.RLock()
	fs, ok := h.inst.shares[share]
	h.inst.mu.RUnlock()
	if !ok {
		return nfs.MountStatusErrNoEnt, nil, nil
	}
	return nfs.MountStatusOk, fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *muxHandler) Change(fs billy.Filesystem) billy.Change {
	if c, ok := fs.(billy.Change); ok {
		return c
	}
	return nil
}

func (h *muxHandler) FSStat(ctx context.Context, f billy.Filesystem, s *nfs.FSStat) error {
	return nil
}

func (h *muxHandler) HandleLimit() int { return nfsHandleLimit }

func (h *muxHandler) ToHandle(f billy.Filesystem, p []string) []byte {
	share, ok := h.inst.shareOf(f)
	if !ok {
		share = ""
	}
	joined := f.Join(p...)
	key := share + "\x00" + joined

	h.revMu.Lock()
	for _, id := range h.reverse[key] {
		if entry, ok := h.handles.Get(id); ok && entry.fs == f {
			h.revMu.Unlock()
			return encodeHandle(share, id)
		}
	}
	h.revMu.Unlock()

	id := uuid.New()
	entry := handleEntry{fs: f, share: share, p: p}
	evictedKey, evictedEntry, hasOldest := h.handles.GetOldest()
	if h.handles.Add(id, entry) && hasOldest {
		h.evictReverse(evictedEntry.share, evictedEntry.fs.Join(evictedEntry.p...), evictedKey)
	}
	h.revMu.Lock()
	h.reverse[key] = append(h.reverse[key], id)
	h.revMu.Unlock()

	return encodeHandle(share, id)
}

func (h *muxHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	share, idBytes, err := decodeHandle(fh)
	if err != nil {
		return nil, []string{}, err
	}
	id, err := uuid.FromBytes(idBytes)
	if err != nil {
		return nil, []string{}, err
	}
	entry, ok := h.handles.Get(id)
	if !ok || entry.share != share {
		return nil, []string{}, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}
	h.inst.mu.RLock()
	current, attached := h.inst.shares[share]
	h.inst.mu.RUnlock()
	if !attached || current != entry.fs {
		return nil, []string{}, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	for _, k := range h.handles.Keys() {
		if candidate, ok := h.handles.Peek(k); ok && candidate.share == share && hasPrefix(entry.p, candidate.p) {
			_, _ = h.handles.Get(k)
		}
	}

	p := make([]string, len(entry.p))
	copy(p, entry.p)
	return entry.fs, p, nil
}

func (h *muxHandler) InvalidateHandle(f billy.Filesystem, fh []byte) error {
	_, idBytes, err := decodeHandle(fh)
	if err != nil {
		return nil
	}
	id, err := uuid.FromBytes(idBytes)
	if err != nil {
		return nil
	}
	if entry, ok := h.handles.Get(id); ok {
		h.evictReverse(entry.share, entry.fs.Join(entry.p...), id)
	}
	h.handles.Remove(id)
	return nil
}

func (h *muxHandler) evictReverse(share, joined string, id uuid.UUID) {
	key := share + "\x00" + joined
	h.revMu.Lock()
	defer h.revMu.Unlock()
	list := h.reverse[key]
	for i, v := range list {
		if v == id {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(h.reverse, key)
		return
	}
	h.reverse[key] = list
}

func hasPrefix(path, prefix []string) bool {
	if len(prefix) > len(path) {
		return false
	}
	for i, e := range prefix {
		if path[i] != e {
			return false
		}
	}
	return true
}

func encodeHandle(share string, id uuid.UUID) []byte {
	out := make([]byte, 0, 2+len(share)+16)
	out = binary.BigEndian.AppendUint16(out, uint16(len(share)))
	out = append(out, share...)
	b, _ := id.MarshalBinary()
	return append(out, b...)
}

func decodeHandle(fh []byte) (string, []byte, error) {
	if len(fh) < 2+16 {
		return "", nil, fmt.Errorf("short handle")
	}
	nameLen := int(binary.BigEndian.Uint16(fh[:2]))
	if nameLen > 0 && len(fh) < 2+nameLen+16 {
		return "", nil, fmt.Errorf("short handle")
	}
	return string(fh[2 : 2+nameLen]), fh[2+nameLen:], nil
}

// VerifierFor hashes the full listing (names, sizes, modes, mtimes), not just
// names, so listings that differ in any attribute get different cookies.
func (h *muxHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	vHash := sha256.New()
	vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(len(path))))
	vHash.Write([]byte(path))
	for _, c := range contents {
		vHash.Write([]byte(c.Name()))
		vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(c.Size())))
		vHash.Write(binary.BigEndian.AppendUint32([]byte{}, uint32(c.Mode())))
		vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(c.ModTime().UnixNano())))
	}
	id := binary.BigEndian.Uint64(vHash.Sum(nil)[0:8])
	h.verifiers.Add(id, verifierEntry{path: path, contents: contents})
	return id
}

func (h *muxHandler) DataForVerifier(path string, id uint64) []fs.FileInfo {
	if entry, ok := h.verifiers.Get(id); ok && entry.path == path {
		return entry.contents
	}
	return nil
}
