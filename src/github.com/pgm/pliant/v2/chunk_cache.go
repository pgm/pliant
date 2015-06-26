package v2

import (
	"sync"
//	"github.com/boltdb/bolt"
	"io"
	"os"
	"io/ioutil"
	"strings"
	"fmt"
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
	inProgress map[Key] *Key; // a "set" of keys which are currently being fetched from remote

	lock sync.Mutex;
	cond *sync.Cond;
}

func NewChunkCache(remote ChunkService, local cacheDB) *ChunkCache {
	c := &ChunkCache{remote: remote, local: local, inProgress: make(map[Key]*Key)};
	c.cond = sync.NewCond(&c.lock)
	return c;
}

func (c *ChunkCache) Put(key *Key, resource Resource) {
	c.local.Put(key, &cacheEntry{source: LOCAL, resource: resource});
}

func (c *ChunkCache) isKeyBeingFetched(key *Key) bool {
	_, keyInProgress := c.inProgress[*key];
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
			c.inProgress[*key] = key;
			resource = c.remote.Get(key)
			c.local.Put(key, &cacheEntry{source: REMOTE, resource: resource})
			delete(c.inProgress, *key)
			c.cond.Broadcast()
		}
	} else {
		resource = entry.resource;
	}
	return resource;
}

type memcacheDB struct {
	lock sync.Mutex
	entries map[Key] *cacheEntry
}

func (c *memcacheDB) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.entries[*key];
}

func (c *memcacheDB) Put(key *Key, entry *cacheEntry) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.entries[*key] = entry
}

func NewMemCacheDB() *memcacheDB {
	return &memcacheDB{entries: make(map[Key] *cacheEntry)};
}

type filesystemCacheDB struct {
	root string;
//	db *bolt.DB;
	lock sync.Mutex
	entries map[Key] *cacheEntry
}

func NewFilesystemCacheDB(root string) (*filesystemCacheDB, error) {
//	db, err := bolt.Open("my.db", 0600, nil)
//	if err != nil {
//		return nil, err
//	}
	_, err := os.Stat(root)
	if os.IsNotExist(err) {
		os.MkdirAll(root, 0770)
	}

	return &filesystemCacheDB{root: root, entries: make(map[Key] *cacheEntry)}, nil;
}

type FilesystemResource struct {
	filename string
	length int64
}

func NewFileResource(filename string) (*FilesystemResource, error) {
	s, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &FilesystemResource{filename, s.Size()}, nil
}

func (r *FilesystemResource) AsBytes() []byte {
	buffer := make([]byte, r.length)
	f, err := os.Open(r.filename)
	if err != nil {
		panic(err.Error())
	}
	f.Read(buffer)
	f.Close()
	return buffer
}

func (r *FilesystemResource) GetReader() io.Reader {
	f, err := os.Open(r.filename)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func (c *filesystemCacheDB) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.entries[*key];
}

func (c *filesystemCacheDB) Dump() {
	fmt.Printf("Dumping cache\n")
	for k, v := range(c.entries) {
		fmt.Printf("  %s -> %s\n", k, v)
	}
}

func (c *filesystemCacheDB) MakeFSResource(resource Resource) (*FilesystemResource, error) {
	reader := resource.GetReader()
	dst, err := ioutil.TempFile(c.root, "import");
	if err != nil {
		return nil, err;
	}

	written, err := io.Copy(dst, reader)
	if err != nil {
		return nil, err;
	}

	readerCloser, hasClose := reader.(io.Closer)
	if hasClose {
		readerCloser.Close()
	}

	err = dst.Close()
	if err != nil {
		return nil, err
	}

	return &FilesystemResource{filename: dst.Name(), length: written}, nil
}

func (c *filesystemCacheDB) Put(key *Key, entry *cacheEntry) {
	fsentry := entry

	// make sure it is a filesystem resource and it's under the right tree
	fsResource, ok := entry.resource.(*FilesystemResource)
	if ok {
		ok = strings.HasPrefix(fsResource.filename, c.root)
	}

	if !ok {
		fsResource, err := c.MakeFSResource(entry.resource)
		if err != nil {
			panic(err.Error())
		}
		fsentry = &cacheEntry{source: entry.source, resource: fsResource}
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.entries[*key] = fsentry
}
