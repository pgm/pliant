package v2

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"sort"
	"sync"
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

	Pull(tag string, lease *Lease) *Key
	Push(key *Key, new_tag string, lease *Lease) error
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
	key := ac.atomic.Pull(args.Tag, &Lease{})

	parsedPath := NewPath(args.Destination)
	return ac.atomic.Link(key, parsedPath, true)
}

type ListFilesRecord struct {
	Name   string
	IsDir  bool
	Length int64
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
		records = append(records, ListFilesRecord{Name: name, IsDir: metadata.GetIsDir(), Length: metadata.GetLength()})
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
	*localPath = (resource.(*FilesystemResource)).filename
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

	roots map[string]*FileMetadata

	dirService DirectoryService
	cache      *filesystemCacheDB
	chunks     *ChunkCache
	tags       TagService

	// list of leases which need to be periodically renewed
	leases     []string
}

func NewAtomicState(dirService DirectoryService, chunks *ChunkCache, cache *filesystemCacheDB, tags TagService) *AtomicState {
	return &AtomicState{dirService: dirService, roots: make(map[string]*FileMetadata), cache: cache, chunks: chunks, tags: tags, leases: make([]string, 0, 10)}
}

var LEASE_TIMEOUT uint64 = 60*60*24

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
				if _, seenKey := seen[*key] ; !seenKey {
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

func (self *AtomicState) Pull(tag string, lease *Lease) *Key {
	key := self.tags.Get(tag)

	return key
}

func (self *AtomicState) DumpDebug() {
	self.lock.Lock()
	defer self.lock.Unlock()

	fmt.Printf("Atomic state has %d entries\n", len(self.roots))
	for k, v := range self.roots {
		fmt.Printf("  %s -> %s\n", k, KeyFromBytes(v.GetKey()).String())
	}
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
	dirMetadata, ok := self.roots[path.path[0]]
	if !ok {
		fmt.Printf("Root keys:\n")
		for k, v := range self.roots {
			fmt.Printf("  %s: %s\n", k, v)
		}
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
		metadata := dir.Get(path.path[i])
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

func (self *AtomicState) GetDirectoryIterator(path *Path) (Iterator, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if len(path.path) == 0 {

		// make a snapshot of the directory and return an iterator over it
		names := make([]string, len(self.roots))
		metadatas := make([]*FileMetadata, len(self.roots))
		i := 0
		for k, v := range self.roots {
			names[i] = k
			metadatas[i] = v
			i += 1
		}

		d := NewMemDirIterator(names, metadatas)
		return d, nil
	}

	finalDir, err := self.unsafeGetDirFromPath(path)
	if err != nil {
		return nil, err
	}
	return finalDir.Iterate(), nil
}

func (self *AtomicState) GetMetadata(path *Path) (*FileMetadata, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	parentPath, filename := path.Split()

	if parentPath.IsRoot() {

		meta, ok := self.roots[path.path[0]]
		if !ok {
			return nil, errors.New("No such path")
		}
		return meta, nil
		//return &FileMetadata{Key: key.AsBytes(), IsDir: proto.Bool(true)}, nil
	} else {
		parentDir, error := self.unsafeGetDirFromPath(parentPath)
		if error != nil {
			return nil, error
		}

		return parentDir.Get(filename), nil
	}
}

func (self *AtomicState) GetResource(key *Key) Resource {
	entry := self.chunks.Get(key)
	if entry == nil {
		fmt.Printf("GetRes Could not find %s\n", key.String())
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

		var metadata *FileMetadata = &FileMetadata{Length: proto.Int64(0), Key: key.AsBytes(), IsDir: proto.Bool(isDir)}

		i := len(parentDirs) - 1
		for i >= 0 {
			newParentKey = parentDirs[i].Put(filename, metadata)
			// update metadata to point to new metadata which points to newParentKey
			metadata = &FileMetadata{Length: proto.Int64(0), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true)}
			filename = path.path[i]
			i -= 1
		}
	}

	newParentMetadata := &FileMetadata{Length: proto.Int64(0), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true)}

	self.roots[path.path[0]] = newParentMetadata
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
		delete(self.roots, path.path[0])
	} else {
		parentPath, filename := path.Split()

		parentDir, err := self.unsafeGetDirFromPath(parentPath)
		if err != nil {
			return err
		}

		newParentDirKey := parentDir.Remove(filename)
		return self.unsafeLink(newParentDirKey, parentPath, true)
	}

	return nil
}
