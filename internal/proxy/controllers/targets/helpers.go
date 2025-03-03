//go:build linux

package targets

import (
	"strings"
	"sync"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

// CacheEntry represents a single cache entry with a value and a timestamp
type CacheEntry struct {
	Value     string
	Timestamp time.Time
}

// Cache is a thread-safe cache with TTL
type Cache struct {
	data *haxmap.Map[string, *CacheEntry]
	ttl  time.Duration
}

// NewCache creates a new cache with the specified TTL
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		data: hashmap.New[*CacheEntry](),
		ttl:  ttl,
	}
}

// Get retrieves a value from the cache if it exists and is not expired
func (c *Cache) Get(key string) (string, bool) {
	entry, exists := c.data.Get(key)
	if !exists || time.Since(entry.Timestamp) > c.ttl {
		// Entry does not exist or is expired
		if exists {
			c.data.Del(key)
		}
		return "", false
	}
	return entry.Value, true
}

// Set adds a value to the cache
func (c *Cache) Set(key string, value string) {
	c.data.Set(key, &CacheEntry{
		Value:     value,
		Timestamp: time.Now(),
	})
}

func processTargets(all []types.Target, storeInstance *store.Store, workerCount int) {
	var wg sync.WaitGroup
	tasks := make(chan int, len(all)) // Channel to distribute tasks to workers

	cache := NewCache(10 * time.Second)

	// Start worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				if all[i].IsAgent {
					targetSplit := strings.Split(all[i].Name, " - ")
					cacheKey := targetSplit[0]

					// Check the cache first
					if cachedResp, found := cache.Get(cacheKey); found {
						// Use cached response
						all[i].ConnectionStatus = true
						all[i].AgentVersion = cachedResp
						continue
					}

					// If not in cache, make the ARPC call
					arpcSess := storeInstance.GetARPC(cacheKey)
					if arpcSess != nil {
						var respBody arpc.MapStringStringMsg
						raw, err := arpcSess.CallMsgWithTimeout(1*time.Second, "ping", nil)
						if err != nil {
							continue
						}
						_, err = respBody.UnmarshalMsg(raw)
						if err == nil {
							// Update the target
							all[i].ConnectionStatus = true
							all[i].AgentVersion = respBody["version"]

							// Store the response in the cache
							cache.Set(cacheKey, respBody["version"])
						}
					}
				}
			}
		}()
	}

	// Send tasks to the workers
	for i := range all {
		tasks <- i
	}
	close(tasks) // Close the task channel to signal workers to stop

	wg.Wait() // Wait for all workers to finish
}
