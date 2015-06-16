package v2

import (
"sync"

)

type sourceEnum int;

const (
	INVALID sourceEnum = iota
	REMOTE
	LOCAL
)

type cacheEntry struct {
	source sourceEnum
	resource *Resource
}

type cacheDB interface {
	// All methods are threadsafe
	Get(key *Key) *cacheEntry;
	Put(key *Key, *cacheEntry);
}

type ChunkCache struct {
	remote ChunkService;
	local cacheDB;
	inProgress map[*Key] *Key; // a "set" of keys which are currently being fetched from remote

	lock sync.Mutex;
	cond sync.Cond;
}

func NewChunkCache(remote ChunkService, local cacheDB) *ChunkCache {
	c := &ChunkCache{remote: remote, local: local, inProgress: make(map[*Key]*Key)};
	c.cond = sync.NewCond(c.lock)
	return c;
}

func (c *ChunkCache) Put(key *Key, resource *Resource) {
	c.local.Put(key, &cacheEntry{source: LOCAL, resource});
}

func (c *ChunkCache) isKeyBeingFetched(key *Key) bool {
	_, keyInProgress := c.inProgress;
	return keyInProgress;
}

func (c *ChunkCache) Get(key *Key) *Resource {
	c.lock.Lock()
	defer c.lock.Unlock()

	resource := c.local.Get(key)
	if resource == nil {
		if c.isKeyBeingFetched(key) {
			for c.isKeyBeingFetched(key) {
				c.cond.Wait();
			}
			resource = c.local.Get(key)
		} else {
			c.inProgress[key] = key;
			resource = c.remote.Get(key)
			c.local.Put(key, &cacheEntry{source: REMOTE, resource})
			delete(c.inProgress[key])
			c.cond.Broadcast()
		}
	}
	return resource;
}

type MemChunkService struct {
	lock sync.Mutex
	chunks map[*Key] *Resource
}

func (c *MemChunkService) Get(key *Key) *Resource {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.chunks[key];
}

func (c *MemChunkService) Put(key *Key, resource *Resource) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.chunks[key] = resource
}

type memcacheDB struct {
	lock sync.Mutex
	entries map[*Key] *cacheEntry
}

func (c *MemChunkService) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.chunks[key];
}

func (c *MemChunkService) Put(key *Key, entry *cacheEntry) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.entries[key] = entry
}

func NewMemCacheDB() *memcacheDB {
	return &memcacheDB{entries: make(map[*Key] *cacheEntry)};
}
