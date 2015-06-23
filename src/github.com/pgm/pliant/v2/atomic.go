package v2

import (
	"github.com/golang/protobuf/proto"
	"errors"
	"fmt"
)

var NO_SUCH_PATH = errors.New("No such path")

type Atomic interface {
	// This interface connects paths which appear mutable with
	// the copy-on-write directory APIs below it

	GetDirectoryIterator(path *Path) (Iterator, error);
	GetMetadata(path *Path) (*FileMetadata, error);

	Put(destination *Path, resource Resource) (*Key, error);
//	Get(destination *Path) *Resource;

	Link(key *Key, path *Path, isDir bool) error;
	Unlink(path *Path) error;

	//TODO
	//Pull(tag string, lease *Lease) *Key;
	//Push(key *Key, new_tag string, lease *Lease);
}


// A wrapper around Atomic which uses simple types for its parameters.
type AtomicClient struct {
	atomic Atomic
}

func (ac *AtomicClient) GetKey(path string, key *string) error {
	parsedPath := NewPath(path);
	metadata, err := ac.atomic.GetMetadata(parsedPath)
	if err != nil {
		return err
	}
	*key = KeyFromBytes(metadata.GetKey()).String()
	return nil;
}

func (ac *AtomicClient) GetLocalPath(path string, localPath *string) error {
	panic("unimp");
}

type PutLocalPathArgs struct {
	localPath string;
	destPath string
}

func (ac *AtomicClient) PutLocalPath(args *PutLocalPathArgs, result *string) error {
	panic("unimp")
//	parsedPath := NewPath(destPath);
//	cachedPath = CacheFile(localPath)
//	resource := NewFileResource(cachedPath);
//	ac.atomic.Put()
}

type LinkArgs struct {
	Key string
	Path string
	IsDir bool
}

func (ac *AtomicClient) Link(args *LinkArgs, result *string) error {
	parsedPath := NewPath(args.Path);
	parsedKey := NewKey(args.Key)
	return ac.atomic.Link(parsedKey, parsedPath, args.IsDir);
}

func (ac *AtomicClient) Unlink(path string, result *string) error {
	parsedPath := NewPath(path);
	return ac.atomic.Unlink(parsedPath);
}


type AtomicState struct {
	// TODO: Add lock to protect access to roots
	dirService DirectoryService;
	roots map[string] *FileMetadata;
}

func NewAtomicState(dirService DirectoryService) *AtomicState {
	return &AtomicState{dirService: dirService, roots: make(map[string]*FileMetadata)}
}

func (self *AtomicState) getDirsFromPath(path *Path) ([]Directory, error) {
	// otherwise we need to decend in until we find the parent
	parentDirs := make([]Directory,0,len(path.path));
	dirMetadata, ok := self.roots[path.path[0]];
	if ! ok {
		fmt.Printf("Root keys:\n")
		for k, v := range(self.roots) {
			fmt.Printf("  %s: %s\n", k, v)
		}
		panic(fmt.Sprintf("Could not find \"%s\"", path.path[0]))
		return nil, NO_SUCH_PATH;
	}
	i := 0
	dirKey := KeyFromBytes(dirMetadata.GetKey())
	for {
		dir := self.dirService.GetDirectory(dirKey)
		parentDirs = append(parentDirs, dir);
		i++;
		if i >= len(path.path) {
			break
		}
		metadata := dir.Get(path.path[i])
		if metadata == nil || !metadata.GetIsDir() {
			return nil, NO_SUCH_PATH;
		}
		dirKey = KeyFromBytes(metadata.GetKey());
	}
	return parentDirs, nil;
}

func (self *AtomicState) getDirFromPath(path *Path) (Directory, error) {
	dirs, err := self.getDirsFromPath(path)
	if err != nil {
		return nil, err
	}
	return dirs[len(dirs)-1], nil;
}

type MemDirIterator struct {
	index int
	names []string
	metadatas []*FileMetadata
}

func (m *MemDirIterator) HasNext() bool {
	return m.index < len(m.names);
}

func (m *MemDirIterator) Next() (string, *FileMetadata) {
	i := m.index
	m.index += 1
	return m.names[i], m.metadatas[i]
}

func NewMemDirIterator(names []string, metadatas []*FileMetadata) Iterator {
	return &MemDirIterator{index: 0, names: names, metadatas: metadatas}
}

func (self *AtomicState) GetDirectoryIterator(path *Path) (Iterator, error) {
	if len(path.path) == 0 {
		// make a snapshot of the directory and return an iterator over it
		names := make([]string, len(self.roots))
		metadatas := make([]*FileMetadata, len(self.roots))
		i := 0
		for k, v := range(self.roots) {
			names[i] = k
			metadatas[i] = v
			i+=1
		}
		return NewMemDirIterator(names, metadatas), nil
	}

	finalDir, err := self.getDirFromPath(path);
	if err != nil {
		return nil, err
	}
	return finalDir.Iterate(), nil;
}

func (self *AtomicState) GetMetadata(path *Path) (*FileMetadata, error) {
	parentPath, filename := path.Split();

	parentDir, error := self.getDirFromPath(parentPath);
	if error != nil {
		return nil, error;
	}

	return parentDir.Get(filename), nil;
}

func (self *AtomicState) Put(destination *Path, resource Resource) (*Key, error) {
	buffer := resource.AsBytes()
	key := computeContentKey(buffer)
	parentPath, filename := destination.Split();

	parentDir, error := self.getDirFromPath(parentPath);
	if error != nil {
		return nil, error;
	}

	metadata := &FileMetadata{Length: proto.Int64(int64(len(buffer))),
		Key: key[:],
		IsDir: proto.Bool(false),
		CreationTime: proto.Int64(1)}
	return parentDir.Put(filename, metadata), nil;
}

func (self *AtomicState) Link(key *Key, path *Path, isDir bool) error {
	// TODO: check for len(path) == 0 (error)
	var newParentKey *Key;
	if len(path.path) == 0 {
		panic("invalid path")
	} else if len(path.path) == 1 {
		newParentKey = key;
	} else {
		parentPath, filename := path.Split();

		parentDirs, err := self.getDirsFromPath(parentPath);
		if err != nil {
			return err;
		}

		var metadata *FileMetadata = &FileMetadata{Length: proto.Int64(0), Key: key.AsBytes(), IsDir: proto.Bool(isDir)};

		i := len(parentDirs)-1
		for i >= 0 {
			newParentKey = parentDirs[i].Put(filename, metadata);
			// update metadata to point to new metadata which points to newParentKey
			metadata = &FileMetadata{Length: proto.Int64(0), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true)};;
			filename = path.path[i]
			i -= 1
		}
	}

	newParentMetadata := &FileMetadata{Length: proto.Int64(0), Key: newParentKey.AsBytes(), IsDir: proto.Bool(true)}

	self.roots[path.path[0]] = newParentMetadata;
	return nil
}

func (self *AtomicState) Unlink(path *Path) error {
	if len(path.path) == 1 {
		delete(self.roots, path.path[0])
	} else {
		parentPath, filename := path.Split();

		parentDir, err := self.getDirFromPath(parentPath);
		if err != nil {
			return err
		}

		parentDir.Remove(filename);
	}

	return nil;
}
