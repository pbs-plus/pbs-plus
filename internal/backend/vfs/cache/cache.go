package cache

import (
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

type Cache struct {
	attrs  map[string]types.AgentFileInfo
	xattrs map[string]map[string]any
	mu     sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{
		attrs:  make(map[string]types.AgentFileInfo),
		xattrs: make(map[string]map[string]any),
	}
}

func (c *Cache) GetAttr(key string) (types.AgentFileInfo, bool) {
	c.mu.RLock()
	val, ok := c.attrs[key]
	c.mu.RUnlock()
	return val, ok
}

func (c *Cache) SetAttr(key string, val types.AgentFileInfo) {
	c.mu.Lock()
	c.attrs[key] = val
	c.mu.Unlock()
}

func (c *Cache) DeleteAttr(key string) {
	c.mu.Lock()
	delete(c.attrs, key)
	c.mu.Unlock()
}

func (c *Cache) GetListXattr(key string) ([]string, bool) {
	c.mu.RLock()
	val, ok := c.xattrs[key]
	c.mu.RUnlock()

	list := make([]string, len(val))
	for k := range val {
		list = append(list, k)
	}
	return list, ok
}

func (c *Cache) GetXattr(key string, attrKey string) (any, bool) {
	c.mu.RLock()
	val, ok := c.xattrs[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	xattr, hasxattr := val[attrKey]
	return xattr, hasxattr
}

func (c *Cache) SetListXattr(key string, val types.AgentFileInfo) {
	c.mu.Lock()
	c.xattrs[key] = make(map[string]any)
	c.xattrs[key]["user.creationtime"] = val.CreationTime
	c.xattrs[key]["user.lastaccesstime"] = val.LastAccessTime
	c.xattrs[key]["user.lastwritetime"] = val.LastWriteTime
	c.xattrs[key]["user.owner"] = val.Owner
	c.xattrs[key]["user.group"] = val.Group
	c.xattrs[key]["user.fileattributes"] = val.FileAttributes
	if len(val.WinACLs) != 0 {
		c.xattrs[key]["user.acls"] = val.WinACLs
	}
	if len(val.PosixACLs) != 0 {
		c.xattrs[key]["user.acls"] = val.PosixACLs
	}
	c.mu.Unlock()
}

func (c *Cache) Destroy() {
	c.mu.Lock()
	c.attrs = nil
	c.xattrs = nil
	c.mu.Unlock()
}
