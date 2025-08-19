package arpcfs

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	cacheTTLSeconds = 60 // tune this; short TTL as a safety valve
)

func canonicalDir(p string) string {
	if p == "" {
		return "/"
	}
	cp := filepath.Clean(p)
	if !strings.HasPrefix(cp, "/") {
		cp = "/" + cp
	}
	if len(cp) > 1 && strings.HasSuffix(cp, "/") {
		cp = strings.TrimRight(cp, "/")
	}
	return cp
}

func canonicalName(name string) string {
	// names from readdir should be single components; ensure no slashes
	// we leave the name as-is but you can add validation if needed
	return name
}

func splitParentBase(full string) (dir, base string) {
	full = filepath.Clean(full)
	if !strings.HasPrefix(full, "/") {
		full = "/" + full
	}
	dir = filepath.Dir(full)
	base = filepath.Base(full)
	return canonicalDir(dir), base
}

func dirGenKey(dir string) string {
	return "dirgen:" + canonicalDir(dir)
}

func attrKey(dir string, gen uint64, name string) string {
	return "attr:" + canonicalDir(dir) + ":" + strconv.FormatUint(gen, 10) + ":" + canonicalName(name)
}

func xattrKey(dir string, gen uint64, name string) string {
	return "xattr:" + canonicalDir(dir) + ":" + strconv.FormatUint(gen, 10) + ":" + canonicalName(name)
}

func (fs *ARPCFS) getOrInitDirGen(dir string) (uint64, error) {
	key := dirGenKey(dir)
	it, err := fs.memcache.Get(key)
	if err == nil {
		g, perr := strconv.ParseUint(string(it.Value), 10, 64)
		if perr == nil && g > 0 {
			return g, nil
		}
		// fallthrough to init on parse error
	}
	// initialize to 1
	err = fs.memcache.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("1"),
		Expiration: cacheTTLSeconds,
	})
	if err != nil {
		return 1, err
	}
	return 1, nil
}

func (fs *ARPCFS) bumpDirGen(dir string) error {
	key := dirGenKey(dir)
	// Try increment; if not found, set to "2"
	if _, err := fs.memcache.Increment(key, 1); err != nil {
		// memcache.Increment returns error on not found
		_ = fs.memcache.Set(&memcache.Item{
			Key:        key,
			Value:      []byte("2"),
			Expiration: cacheTTLSeconds,
		})
	}
	return nil
}
