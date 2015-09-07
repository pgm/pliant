package v2

import (
	"sync"
	//	"github.com/boltdb/bolt"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"github.com/boltdb/bolt"
	"time"
	"github.com/golang/protobuf/proto"
	"errors"
)

type sourceEnum int

const (
	INVALID sourceEnum = iota
	REMOTE
	LOCAL
)

type cacheEntry struct {
	source   sourceEnum
	resource Resource
}

type cacheDB interface {
	// All methods are threadsafe
	Get(key *Key) *cacheEntry
	Put(key *Key, entry *cacheEntry)
}

type ChunkCache struct {
	remote     ChunkService
	local      cacheDB
	inProgress map[Key]*Key // a "set" of keys which are currently being fetched from remote

	lock sync.Mutex
	cond *sync.Cond
}

func (c *ChunkCache) Dump() {

}

func NewChunkCache(remote ChunkService, local cacheDB) *ChunkCache {
	c := &ChunkCache{remote: remote, local: local, inProgress: make(map[Key]*Key)}
	c.cond = sync.NewCond(&c.lock)
	return c
}

func (c *ChunkCache) PushToRemote(key *Key) error {
	fmt.Printf("**** PushToRemote %s\n", key.String())
	resource := c.Get(key)
	c.remote.Put(key, resource)
	//	r := c.remote.Get(KeyFromBytes(key.AsBytes()))
	//	if r == nil {
	//		panic("failed put")
	//	}
	return nil
}

func (c *ChunkCache) Put(key *Key, resource Resource) {
	c.local.Put(key, &cacheEntry{source: LOCAL, resource: resource})
}

func (c *ChunkCache) isKeyBeingFetched(key *Key) bool {
	_, keyInProgress := c.inProgress[*key]
	return keyInProgress
}

func (c *ChunkCache) Get(key *Key) Resource {
	c.lock.Lock()
	defer c.lock.Unlock()

	var resource Resource

	entry := c.local.Get(key)
	if entry == nil {
		if c.isKeyBeingFetched(key) {
			for c.isKeyBeingFetched(key) {
				c.cond.Wait()
			}
			resource = c.local.Get(key).resource
		} else {
			c.inProgress[*key] = key
			resource = c.remote.Get(key)
			c.local.Put(key, &cacheEntry{source: REMOTE, resource: resource})
			delete(c.inProgress, *key)
			c.cond.Broadcast()
		}
	} else {
		resource = entry.resource
	}
	return resource
}

type memcacheDB struct {
	lock    sync.Mutex
	entries map[Key]*cacheEntry
}

func (c *memcacheDB) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.entries[*key]
}

func (c *memcacheDB) Put(key *Key, entry *cacheEntry) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.entries[*key] = entry
}

func NewMemCacheDB() *memcacheDB {
	return &memcacheDB{entries: make(map[Key]*cacheEntry)}
}

type filesystemCacheDB struct {
	root string
	db *bolt.DB;
	lock    sync.Mutex // TODO: Can this be eliminated now that we're no longer using a map
}


func (f *filesystemCacheDB) AllocateTempFilename() string {
	fp, err := ioutil.TempFile(f.root, "temp")
	if err != nil {
		panic(err.Error())
	}
	fp.Close()
	return fp.Name()
}

func InitDb(filename string) (*bolt.DB, error) {
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(KEY_TO_FILENAME)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(ROOT_TO_KEY)
		return err
	})

	if err != nil {
		return nil, err
	}

	return db, nil
}

func NewFilesystemCacheDB(root string, db *bolt.DB) (*filesystemCacheDB, error) {
	return &filesystemCacheDB{root: root, db: db}, nil
}

type FilesystemResource struct {
	filename string
	length   int64
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

var KEY_TO_FILENAME  []byte = []byte("keyToFilename")
var ROOT_TO_KEY []byte = []byte("rootToKey")

func unpackCacheEntry(src []byte, entry *cacheEntry) {
	//fmt.Printf("unpackCacheEntry(%s, entry)\n", src);
	dest := &CacheEntry{}
	err := proto.Unmarshal(src, dest)
	if err != nil {
		panic(fmt.Sprintf("Couldn't unmarshal cacheentry object: %s", err))
	}
	entry.source = sourceEnum(dest.GetSource())
	entry.resource, err = NewFileResource(dest.GetFilename())
	if err != nil {
		panic(err.Error())
	}
}

func packCacheEntry(entry *cacheEntry) [] byte {
	filename := entry.resource.(*FilesystemResource).filename
	source := CacheEntry_SourceType(entry.source)
	data, err := proto.Marshal(&CacheEntry{Filename: proto.String(filename), Source: &source})
	if err != nil {
		panic(fmt.Sprintf("Couldn't marshal cacheentry object: %s", err))
	}
	return data
}

var NO_SUCH_KEY = errors.New("No such key")

func (c *filesystemCacheDB) Get(key *Key) *cacheEntry {
	c.lock.Lock()
	defer c.lock.Unlock()

	var entry cacheEntry

	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(KEY_TO_FILENAME)
		entryBuffer := b.Get(key.AsBytes())
		if entryBuffer == nil {
//			panic(fmt.Sprintf("Key %s did not exist in %s", key, c.db))
			return NO_SUCH_KEY
		} else {
			fmt.Printf("len(entryBuffer)=%d\n", len(entryBuffer))
		}
		unpackCacheEntry(entryBuffer, &entry)

		return nil
	})

	if err == NO_SUCH_KEY {
		return nil
	} else if err != nil {
		panic(err.Error())
	}

	return &entry
}

func (c *filesystemCacheDB) Dump() {
	fmt.Printf("-------------\n")
	fmt.Printf("Dumping cache\n")

	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(KEY_TO_FILENAME)
		b.ForEach(func(k, v []byte) error {
			key := KeyFromBytes(k)
			var entry cacheEntry;
			unpackCacheEntry(v, &entry)
			fmt.Printf("  %s -> %s\n", key, entry)
			return nil
		})

		return nil
	})

	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("-------------\n")
}

func (c *filesystemCacheDB) MakeFSResource(resource Resource) (*FilesystemResource, error) {
	reader := resource.GetReader()
	dst, err := ioutil.TempFile(c.root, "import")
	if err != nil {
		return nil, err
	}

	written, err := io.Copy(dst, reader)
	if err != nil {
		return nil, err
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

	c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(KEY_TO_FILENAME)
		fsentryBuffer := packCacheEntry(fsentry)
		fmt.Printf("Put(%s, len(entry)=%d (%s)\n", key, len(fsentryBuffer), c.db)
		err := b.Put(key.AsBytes(), fsentryBuffer)
		return err
	})
}
