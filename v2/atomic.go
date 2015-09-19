package v2

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
)

var NO_SUCH_PATH = errors.New("No such path")

type Atomic interface {
	// This interface connects paths which appear mutable with
	// the copy-on-write directory APIs below it

	GetDirectoryIterator(path *Path) (Iterator, error)
	GetMetadata(path *Path) (*FileMetadata, error)

	Put(destination *Path, resource Resource) (*Key, error)
	GetResource(key *Key) Resource

	Link(key *Key, path *Path, isDir bool) error
	Unlink(path *Path) error

	CreateResourceForLocalFile(localFile string) (Resource, error)

	Pull(tag string, lease *Lease) (*Key, error)
	Push(key *Key, new_tag string, lease *Lease) error

	ForEachRoot(prefix string, callback func(name string, key *Key)) error
}

// A wrapper around Atomic which uses simple types for its parameters.
type AtomicClient struct {
	atomic Atomic
}

type PushArgs struct {
	Source string
	Tag    string
}

func (ac *AtomicClient) Push(args *PushArgs, result *string) error {
	parsedPath := NewPath(args.Source)
	metadata, err := ac.atomic.GetMetadata(parsedPath)
	if err != nil {
		return err
	}

	key := KeyFromBytes(metadata.GetKey())

	return ac.atomic.Push(key, args.Tag, &Lease{})
}

type PullArgs struct {
	Tag         string
	Destination string
}

func (ac *AtomicClient) Pull(args *PullArgs, result *string) error {
	key, err := ac.atomic.Pull(args.Tag, &Lease{})
	if err != nil {
		return err
	}

	parsedPath := NewPath(args.Destination)
	return ac.atomic.Link(key, parsedPath, true)
}

type ListRootsRecord struct {
	Name string
	Key  *Key
}

func (ac *AtomicClient) ListRoots(prefix string, resultPtr *[]ListRootsRecord) error {
	result := make([]ListRootsRecord, 0, 100)
	err := ac.atomic.ForEachRoot(prefix, func(name string, key *Key) {
		result = append(result, ListRootsRecord{name, key})
	})
	if err != nil {
		return err
	}

	*resultPtr = result
	return nil
}

type ListFilesRecord struct {
	Name         string
	IsDir        bool
	Size         int64
	TotalSize    int64
	CreationTime int64
}

func (ac *AtomicClient) ListFiles(path string, result *[]ListFilesRecord) error {
	parsedPath := NewPath(path)
	it, err := ac.atomic.GetDirectoryIterator(parsedPath)
	if err != nil {
		return err
	}

	records := make([]ListFilesRecord, 0, 100)

	for it.HasNext() {
		name, metadata := it.Next()
		records = append(records, ListFilesRecord{Name: name, IsDir: metadata.GetIsDir(), TotalSize: metadata.GetTotalSize(), Size: metadata.GetSize(), CreationTime: metadata.GetCreationTime()})
	}

	*result = records
	return nil
}

func (ac *AtomicClient) MakeDir(path string, result *string) error {
	parsedPath := NewPath(path)
	return ac.atomic.Link(EMPTY_DIR_KEY, parsedPath, true)
}

func (ac *AtomicClient) GetKey(path string, key *string) error {
	parsedPath := NewPath(path)
	metadata, err := ac.atomic.GetMetadata(parsedPath)
	if err != nil {
		return err
	}
	*key = KeyFromBytes(metadata.GetKey()).String()
	return nil
}

const STAT_ERROR_MISSING = "missing"
const STAT_ERROR_NONE = ""

type StatResponse struct {
	Size         int64
	Key          []byte
	CreationTime int64
	IsDir        bool
	TotalSize    int64
	Error        string
}

func (ac *AtomicClient) Stat(path string, result *StatResponse) error {
	parsedPath := NewPath(path)
	metadata, err := ac.atomic.GetMetadata(parsedPath)
	fmt.Printf("Stat() = %s, err=%s\n", metadata, err)
	if err != nil {
		return err
	}

	if metadata == nil {
		result.Error = STAT_ERROR_MISSING
	} else {
		result.Size = metadata.GetSize()
		result.Key = metadata.GetKey()
		result.CreationTime = metadata.GetCreationTime()
		result.IsDir = metadata.GetIsDir()
		result.TotalSize = metadata.GetTotalSize()
		result.Error = STAT_ERROR_NONE
	}

	return nil
}

func (ac *AtomicClient) GetLocalPath(path string, localPath *string) error {
	parsedPath := NewPath(path)
	metadata, err := ac.atomic.GetMetadata(parsedPath)
	if err != nil {
		return err
	}
	key := KeyFromBytes(metadata.GetKey())
	resource := ac.atomic.GetResource(key)
	if resource == nil {
		return errors.New(fmt.Sprintf("Resource missing: %s", key.String()))
	}
	*localPath, err = filepath.Abs((resource.(*FilesystemResource)).filename)
	if err != nil {
		return err
	}
	return nil
}

type PutLocalPathArgs struct {
	LocalPath string
	DestPath  string
}

func (ac *AtomicClient) PutLocalPath(args *PutLocalPathArgs, result *string) error {
	parsedPath := NewPath(args.DestPath)
	resource, err := ac.atomic.CreateResourceForLocalFile(args.LocalPath)
	if err != nil {
		return err
	}
	fmt.Printf("Created resource: %s\n", resource)
	_, err = ac.atomic.Put(parsedPath, resource)
	return err
}

type LinkArgs struct {
	Key   string
	Path  string
	IsDir bool
}

func (ac *AtomicClient) Link(args *LinkArgs, result *string) error {
	parsedPath := NewPath(args.Path)
	parsedKey := NewKey(args.Key)
	return ac.atomic.Link(parsedKey, parsedPath, args.IsDir)
}

func (ac *AtomicClient) Unlink(path string, result *string) error {
	parsedPath := NewPath(path)
	return ac.atomic.Unlink(parsedPath)
}

type AtomicState struct {
	lock sync.Mutex // protects access to roots

	roots RootMap

	dirService DirectoryService
	cache      *filesystemCacheDB
	chunks     *ChunkCache
	tags       TagService

	// list of leases which need to be periodically renewed
	leases []string
}

type RootMap interface {
	Get(name string) (*FileMetadata, bool)
	Set(name string, value *FileMetadata)
	ForEach(func(name string, x *FileMetadata))
	//map[string]*FileMetadata
}

type DbRootMap struct {
	db *bolt.DB
}

func NewDbRootMap(db *bolt.DB) RootMap {
	return &DbRootMap{db: db}
}

func (self *DbRootMap) Get(name string) (*FileMetadata, bool) {
	var nilResult *FileMetadata = nil
	var result **FileMetadata = &nilResult
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(ROOT_TO_KEY)
		buffer := b.Get([]byte(name))
		if buffer != nil {
			*result = UnpackFileMetadata(bytes.NewBuffer(buffer))
		}
		return nil
	})
	if err != nil {
		panic(err.Error())
	}
	return *result, (*result != nil)
}

func (self *DbRootMap) Set(name string, value *FileMetadata) {
	err := self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ROOT_TO_KEY)
		if value == nil {
			b.Delete([]byte(name))
		} else {
			buffer := PackFileMetadata(value)
			fmt.Printf("setting %s to len(buffer)=%d\n", name, len(buffer))
			b.Put([]byte(name), buffer)
		}
		return nil
	})

	if err != nil {
		panic(err.Error())
	}
}

func (self *DbRootMap) ForEach(callback func(name string, x *FileMetadata)) {
	err := self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(ROOT_TO_KEY)
		b.ForEach(func(k, v []byte) error {
			metadata := UnpackFileMetadata(bytes.NewBuffer(v))
			callback(string(k), metadata)
			return nil
		})
		return nil
	})
	if err != nil {
		panic(err.Error())
	}
}

type MemRootMap struct {
	roots map[string]*FileMetadata
}

func (self *MemRootMap) Get(name string) (*FileMetadata, bool) {
	a, b := self.roots[name]
	return a, b
}

func (self *MemRootMap) Set(name string, value *FileMetadata) {
	if value == nil {
		delete(self.roots, name)
	} else {
		self.roots[name] = value
	}
}

func (self *MemRootMap) ForEach(callback func(name string, x *FileMetadata)) {
	for k, v := range self.roots {
		callback(k, v)
	}
}

func NewMemRootMap() RootMap {
	return &MemRootMap{roots: make(map[string]*FileMetadata)}
}

func NewAtomicState(dirService DirectoryService, chunks *ChunkCache, cache *filesystemCacheDB, tags TagService, roots RootMap) *AtomicState {
	return &AtomicState{dirService: dirService, roots: roots, cache: cache, chunks: chunks, tags: tags, leases: make([]string, 0, 10)}
}

var LEASE_TIMEOUT uint64 = 60 * 60 * 24

type typedKey struct {
	key   *Key
	isDir bool
}

func walk(roots []*Key, readDir func(*Key) *LeafDir, emitEdge func(*Key, *Key) bool) map[Key]bool {
	seen := make(map[Key]bool)

	for len(roots) > 0 {
		nextKey := roots[len(roots)-1]
		roots = roots[:len(roots)-1]

		var leaf *LeafDir
		leaf = readDir(nextKey)

		it := leaf.Iterate()
		for it.HasNext() {
			_, meta := it.Next()

			key := KeyFromBytes(meta.GetKey())
			shouldExplore := emitEdge(nextKey, key)

			if meta.GetIsDir() {
				if _, seenKey := seen[*key]; !seenKey {
					if shouldExplore {
						roots = append(roots, key)
						seen[*key] = true
					}
				}
			} else {
				seen[*key] = true
			}
		}
	}

	return seen
}

// roots are assumed to be dir pointers
func walkReachable(cache *filesystemCacheDB, roots []*Key) ([]*Key, []*FileMetadata) {
	panic("unimp")
	//
	//	seen := walk()
	//
	//	it := cache.Iterate()
	//	if !this in seen && cache.get(this).source == LOCAL {
	//		drop
	//	}
}

func (self *AtomicState) GC() {
	/*
		// find referenced remote keys
		self.lock.Lock()
		defer self.lock.Unlock()

		roots := make([]*Key, 0, len(self.roots))
		for _, meta := range(self.roots) {
			roots = append(roots, KeyFromBytes(meta.GetKey()))
		}

		unreachableKeys, referencedRemoteObjs := walkReachable(self.cache, roots)

		// create dummy leaf with all referenced remote keys
		refKey := CreateAnonymousRefLeaf(self.chunks.remote, referencedRemoteObjs)

		// create a new single lease
		newLease := self.tags.AddLease(LEASE_TIMEOUT, refKey)

		// revoke existing leases
		for _, leaseId := range(self.leases) {
			self.tags.RevokeLease(leaseId)
		}

		self.leases = [...]string{newLease};

		self.cache.delete(unreachableKeys)

		self.cache.ExpireRemoteObjects()
	*/
	panic("unimp")
}

func (self *AtomicState) Pull(tag string, lease *Lease) (*Key, error) {
	key, err := self.tags.Get(tag)
	return key, err
}

func (self *AtomicState) DumpDebug() {
	//	self.lock.Lock()
	//	defer self.lock.Unlock()
	//
	//	fmt.Printf("Atomic state has %d entries\n", len(self.roots))
	//	for k, v := range self.roots {
	//		fmt.Printf("  %s -> %s\n", k, KeyFromBytes(v.GetKey()).String())
	//	}
}

func (self *AtomicState) Push(key *Key, tag string, lease *Lease) error {
	seen := make(map[Key]*Key)
	pending := make([]typedKey, 0, 1000)

	// add empty dir as seen so we always skip it
	seen[*EMPTY_DIR_KEY] = EMPTY_DIR_KEY

	pending = append(pending, typedKey{key, true})

	for len(pending) > 0 {
		next := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		_, wasSeen := seen[*next.key]
		if wasSeen {
			continue
		}

		// remember we've handled this block
		seen[*next.key] = next.key

		entry := self.cache.Get(next.key)
		if entry == nil {
			panic("Could not find cache entry for " + next.key.String())
		}
		if entry.source == REMOTE {
			continue
		}

		// copy chunk to remote
		self.chunks.PushToRemote(next.key)
		// remember it's now available on the remote, to prevent pushing it again in the future
		// this may alleviate the need for tracking blocks with 'seen'
		self.cache.Put(next.key, &cacheEntry{source: REMOTE, resource: entry.resource})

		if !next.isDir {
			continue
		}

		// now record all the keys that this references
		dir := self.dirService.GetDirectory(next.key)
		it := dir.Iterate()
		for it.HasNext() {
			_, meta := it.Next()
			pending = append(pending, typedKey{KeyFromBytes(meta.GetKey()), meta.GetIsDir()})
		}
	}

	self.tags.Put(tag, key)

	return nil
}

func (self *AtomicState) CreateResourceForLocalFile(localFile string) (Resource, error) {
	resource, err := NewFileResource(localFile)
	if err != nil {
		return nil, err
	}
	fsResource, err := self.cache.MakeFSResource(resource)
	return fsResource, err
}

func (self *AtomicState) unsafeGetDirsFromPath(path *Path) ([]Directory, error) {
	// otherwise we need to descend in until we find the parent
	parentDirs := make([]Directory, 0, len(path.path))
	dirMetadata, ok := self.roots.Get(path.path[0])
	if !ok {
		//		fmt.Printf("Root keys:\n")
		//		for k, v := range self.roots {
		//			fmt.Printf("  %s: %s\n", k, v)
		//		}
		return nil, NO_SUCH_PATH
	}
	i := 0
	dirKey := KeyFromBytes(dirMetadata.GetKey())
	for {
		dir := self.dirService.GetDirectory(dirKey)
		parentDirs = append(parentDirs, dir)
		i++
		if i >= len(path.path) {
			break
		}
		metadata, err := dir.Get(path.path[i])
		if err != nil {
			return nil, err
		}
		if metadata == nil || !metadata.GetIsDir() {
			return nil, NO_SUCH_PATH
		}
		dirKey = KeyFromBytes(metadata.GetKey())
	}
	return parentDirs, nil
}

func (self *AtomicState) unsafeGetDirFromPath(path *Path) (Directory, error) {
	dirs, err := self.unsafeGetDirsFromPath(path)
	if err != nil {
		return nil, err
	}
	return dirs[len(dirs)-1], nil
}

//type MemDir struct {
//	names []string
//	metadatas []*FileMetadata
//}

type MemDirIterator struct {
	//	MemDir
	names     []string
	metadatas []*FileMetadata
	index     int
}

func (m *MemDirIterator) Len() int {
	return len(m.names)
}

func (m *MemDirIterator) Less(i, j int) bool {
	return m.names[i] < m.names[j]
}

func (m *MemDirIterator) Swap(i, j int) {
	tname := m.names[i]
	tmetadatas := m.metadatas[i]

	m.names[i] = m.names[j]
	m.metadatas[i] = m.metadatas[j]

	m.names[j] = tname
	m.metadatas[j] = tmetadatas
}

func (m *MemDirIterator) HasNext() bool {
	return m.index < len(m.names)
}

func (m *MemDirIterator) Next() (string, *FileMetadata) {
	i := m.index
	m.index += 1
	return m.names[i], m.metadatas[i]
}

func NewMemDirIterator(names []string, metadatas []*FileMetadata) Iterator {
	d := &MemDirIterator{index: 0, names: names, metadatas: metadatas}
	sort.Sort(d)
	return d
}

func (self *AtomicState) ForEachRoot(prefix string, callback func(name string, key *Key)) error {
	self.tags.ForEach(callback)
	return nil
}

func (self *AtomicState) GetDirectoryIterator(path *Path) (Iterator, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if len(path.path) == 0 {
		// make a snapshot of the directory and return an iterator over it
		names := make([]string, 0, 20)
		metadatas := make([]*FileMetadata, 0, 20)
		self.roots.ForEach(func(k string, v *FileMetadata) {
			names = append(names, k)
			metadatas = append(metadatas, v)
		})

		d := NewMemDirIterator(names, metadatas)
		return d, nil
	}

	finalDir, err := self.unsafeGetDirFromPath(path)
	if err != nil {
		return nil, err
	}
	return finalDir.Iterate(), nil
}

// returns nil if no file with that path exists
func (self *AtomicState) GetMetadata(path *Path) (*FileMetadata, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if path.IsRoot() {
		var key Key
		return &FileMetadata{TotalSize: proto.Int64(0), Size: proto.Int64(0), Key: key.AsBytes(), IsDir: proto.Bool(true), CreationTime: proto.Int64(time.Now().Unix())}, nil
	}

	parentPath, filename := path.Split()

	if parentPath.IsRoot() {

		meta, ok := self.roots.Get(path.path[0])
		if !ok {
			return nil, NO_SUCH_PATH
		}
		return meta, nil
		//return &FileMetadata{Key: key.AsBytes(), IsDir: proto.Bool(true)}, nil
	} else {
		parentDir, error := self.unsafeGetDirFromPath(parentPath)
		if error != nil {
			return nil, error
		}

		metadata, err := parentDir.Get(filename)
		fmt.Printf("parentDir.Get(%s)=%s\n", filename, metadata)
		if err != nil {
			return nil, err
		}
		return metadata, nil
	}
}

func (self *AtomicState) GetResource(key *Key) Resource {
	entry, err := self.chunks.Get(key)
	// TODO: maybe GetResource needs to return an error too?
	if err != nil {
		panic(err.Error())
	}
	if entry == nil {
		return nil
	}
	return entry
}

func (self *AtomicState) Put(destination *Path, resource Resource) (*Key, error) {
	buffer := resource.AsBytes()
	key := computeContentKey(buffer)

	self.cache.Put(key, &cacheEntry{source: LOCAL, resource: resource})

	err := self.Link(key, destination, false)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func (self *AtomicState) unsafeLink(key *Key, path *Path, isDir bool) error {
	var length int64
	var childrenSize int64
	if *key == *EMPTY_DIR_KEY {
		length = 0
		childrenSize = 0
	} else {
		resource, err := self.chunks.Get(key)
		if err != nil {
			return err
		}
		length = resource.GetLength()
		if isDir {
			dir := self.dirService.GetDirectory(key)
			childrenSize, err = dir.GetTotalSize()
		} else {
			childrenSize = 0
		}
	}

	// TODO: check for len(path) == 0 (error)
	var newParentKey *Key
	if len(path.path) == 0 {
		panic("invalid path")
	} else if len(path.path) == 1 {
		newParentKey = key
	} else {
		parentPath, filename := path.Split()

		parentDirs, err := self.unsafeGetDirsFromPath(parentPath)
		if err != nil {
			return err
		}

		var metadata *FileMetadata = &FileMetadata{TotalSize: proto.Int64(childrenSize + length), Size: proto.Int64(length), Key: key.AsBytes(), IsDir: proto.Bool(isDir), CreationTime: proto.Int64(time.Now().Unix())}

		i := len(parentDirs) - 1
		for i >= 0 {
			newParentKey, childrenSize, err = parentDirs[i].Put(filename, metadata)
			if err != nil {
				return err
			}
			length = self.cache.Get(newParentKey).resource.GetLength()
			// update metadata to point to new metadata which points to newParentKey
			metadata = &FileMetadata{TotalSize: proto.Int64(childrenSize + length), Size: proto.Int64(length), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true), CreationTime: proto.Int64(time.Now().Unix())}
			filename = path.path[i]
			i -= 1
		}
	}

	if *newParentKey == *EMPTY_DIR_KEY {
		length = 0
	} else {
		length = self.cache.Get(newParentKey).resource.GetLength()
	}
	newParentMetadata := &FileMetadata{TotalSize: proto.Int64(childrenSize + length), Size: proto.Int64(length), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true), CreationTime: proto.Int64(time.Now().Unix())}

	self.roots.Set(path.path[0], newParentMetadata)
	return nil
}

func (self *AtomicState) Link(key *Key, path *Path, isDir bool) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.unsafeLink(key, path, isDir)
}

func (self *AtomicState) Unlink(path *Path) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if len(path.path) == 1 {
		self.roots.Set(path.path[0], nil)
	} else {
		parentPath, filename := path.Split()

		parentDir, err := self.unsafeGetDirFromPath(parentPath)
		if err != nil {
			return err
		}

		newParentDirKey, _, err := parentDir.Remove(filename)
		if err != nil {
			return err
		}
		return self.unsafeLink(newParentDirKey, parentPath, true)
	}

	return nil
}
