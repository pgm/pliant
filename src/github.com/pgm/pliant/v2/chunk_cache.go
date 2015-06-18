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
	resource Resource
}

type cacheDB interface {
	// All methods are threadsafe
	Get(key *Key) *cacheEntry;
	Put(key *Key, entry *cacheEntry);
}

type ChunkCache struct {
	remote ChunkService;
	local cacheDB;
	inProgress map[*Key] *Key; // a "set" of keys which are currently being fetched from remote

	lock sync.Mutex;
	cond *sync.Cond;
}

func NewChunkCache(remote ChunkService, local cacheDB) *ChunkCache {
	c := &ChunkCache{remote: remote, local: local, inProgress: make(map[*Key]*Key)};
	c.cond = sync.NewCond(&c.lock)
	return c;
}

func (c *ChunkCache) Put(key *Key, resource Resource) {
	c.local.Put(key, &cacheEntry{source: LOCAL, resource: resource});
}

func (c *ChunkCache) isKeyBeingFetched(key *Key) bool {
	_, keyInProgress := c.inProgress[key];
	return keyInProgress;
}

func (c *ChunkCache) Get(key *Key) Resource {
	c.lock.Lock()
	defer c.lock.Unlock()

	var resource Resource
	entry := c.local.Get(key)
	if entry == nil {
		if c.isKeyBeingFetched(key) {
			for c.isKeyBeingFetched(key) {
				c.cond.Wait();
			}
			resource = c.local.Get(key).resource
		} else {
			c.inProgress[key] = key;
			resource = c.remote.Get(key)
			c.local.Put(key, &cacheEntry{source: REMOTE, resource: resource})
			delete(c.inProgress, key)
			c.cond.Broadcast()
		}
	} else {
		resource = entry.resource;
	}
	return resource;
}

type memcacheDB struct {
	lock sync.Mutex
	entries map[*Key] *cacheEntry
}

func (c *memcacheDB) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.entries[key];
}

func (c *memcacheDB) Put(key *Key, entry *cacheEntry) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.entries[key] = entry
}

func NewMemCacheDB() *memcacheDB {
	return &memcacheDB{entries: make(map[*Key] *cacheEntry)};
}
