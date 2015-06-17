package v2

type Atomic interface {
	// This interface connects paths which appear mutable with
	// the copy-on-write directory APIs below it

	GetDirectoryIterator(path *Path) *Iterator;
	GetMetadata(path *Path) *FileMetadata;

	Put(destination *Path, resource *Resource) *Key;
	Get(destination *Path) *Resource;

	Link(key *Key, path *Path);
	Unlink(path *Path);

	//TODO
	//Pull(tag string, lease *Lease) *Key;
	//Push(key *Key, new_tag string, lease *Lease);
}


// A wrapper around Atomic which uses simple types for its parameters.
type AtomicClient struct {
	atomic Atomic
}

func (ac *AtomicClient) GetKey(path string) string {
	parsedPath := NewPath(path);
	metadata := ac.atomic.GetMetadata(parsedPath)
	return KeyFromBytes(metadata.GetKey()).String();
}

func (ac *AtomicClient) GetLocalPath(path string) string {
	panic("unimp");
}

func (ac *AtomicClient) Link(key string, path string) {
	parsedPath := NewPath(path);
	parsedKey := NewKey(key)
	ac.atomic.Link(parsedKey, parsedPath);
}

func (ac *AtomicClient) Unlink(path string) {
	parsedPath := NewPath(path);
	ac.atomic.Unlink(parsedPath);
}

type AtomicState struct {
	dirService DirectoryService;
	roots map[string] *Key;
}

func (self *AtomicState) getDirsFromPath(path *Path) []Directory {
	// otherwise we need to decend in until we find the parent
	parentDirs := make([]Directory,0,0);
	dirKey, ok := self.roots[path.path[0]];
	if ! ok {
		return nil;
	}
	i := 0
	for {
		dir := self.dirService.GetDirectory(dirKey)
		parentDirs = append(parentDirs, dir);
		i++;
		if i >= len(path.path) {
			break
		}
		metadata := dir.Get(path.path[i])
		if metadata == nil || !metadata.GetIsDir() {
			return nil;
		}
		dirKey = KeyFromBytes(metadata.GetKey());
	}
	return parentDirs;
}

func (self *AtomicState) getDirFromPath(path *Path) Directory {
	dirs := self.getDirsFromPath(path)
	return dirs[len(dirs)-1];
}

func (self *AtomicState) GetDirectoryIterator(path *Path) Iterator {
	if len(path.path) == 0 {
		// Create a fake Leaf node in memory with these files and then return NamespaceIterator over this
		panic("unimp");
	}

	finalDir := self.getDirFromPath(path);
	if finalDir == nil {
		return nil;
	}
	return finalDir.Iterate();
}

func (self *AtomicState) GetMetadata(path *Path) *FileMetadata {
	parentPath, filename := path.Split();

	parentDir := self.getDirFromPath(parentPath);
	if parentDir == nil {
		return nil;
	}

	return parentDir.Get(filename);
}

func (self *AtomicState) Link(key *Key, path *Path) {
	// TODO: check for len(path) == 0 (error)

	var newParentKey *Key;
	if len(path.path) == 1 {
		newParentKey = key;
	} else {
		parentPath, filename := path.Split();

		parentDirs := self.getDirsFromPath(parentPath);
		if parentDirs == nil {
			panic("Error");
		}

		var metadata *FileMetadata;
		// todo: make metadata from key
		i := len(parentDirs)
		for i >= 0 {
			newParentKey = parentDirs[i].Put(filename, metadata);
			// update metadata to point to new metadata which points to newParentKey
			metadata = nil;
		}
	}

	self.roots[path.path[0]] = newParentKey;
}

func (self *AtomicState) Unlink(path *Path) {
	if len(path.path) == 1 {
		delete(self.roots, path.path[0])
	} else {
		parentPath, filename := path.Split();

		parentDir := self.getDirFromPath(parentPath);
		if parentDir == nil {
			panic("unimp");
		}

		parentDir.Remove(filename);
	}
}
