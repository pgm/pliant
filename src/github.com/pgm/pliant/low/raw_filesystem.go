package low

import (
	"fmt"
	"errors"
	"strings"
	"github.com/golang/protobuf/proto"
	"bytes"
	"io"
)

type RawFilesystem struct {
	chunks   ChunkService
}

func (self *Dir) cloneWithReplacement(name string, newDirEntry *DirEntry, replaceExisting bool) (*Dir, error) {
	if(newDirEntry != nil && newDirEntry.GetName() != name) {
		panic(fmt.Sprintf("name mismatches direntry: %s != %s", newDirEntry.GetName(), name))
	}

	newEntries := make([]*DirEntry, 0, len(self.Entries))
	found := false
	entries := self.GetEntries()
	for i := range (entries) {
		if entries[i].GetName() == name {
			if replaceExisting {
				if newDirEntry != nil {
					newEntries = append(newEntries, newDirEntry)
				}
				found = true
			} else {
				return nil, errors.New("already exists")
			}
		} else {
			newEntries = append(newEntries, entries[i])
		}
	}

	if !found {
		if newDirEntry != nil {
			newEntries = append(newEntries, newDirEntry)
		}
	}

	return &Dir{Entries: newEntries}, nil
}

func (self *Dir) Get(name string) *DirEntry {
	for i := range (self.Entries) {
		if self.Entries[i].GetName() == name {
			return self.Entries[i]
		}
	}

	return nil
}

func (self *RawFilesystem) cloneDirWithReplacement(dirId ChunkID, name string, newDirEntry *DirEntry, replaceExisting bool) (ChunkID, error) {
	dir, readDirErr := self.ReadDir(dirId)
	if readDirErr != nil {
		return INVALID_ID, readDirErr
	}

	newDir, cloneError := dir.cloneWithReplacement(name, newDirEntry, replaceExisting)
	if cloneError != nil {
		return INVALID_ID, cloneError
	}

	newDirId, newDirErr := self.NewDir(newDir)
	if newDirErr != nil {
		return INVALID_ID, newDirErr
	}

	return newDirId, nil
}

func splitPath(path string) []string {
	if path[0] == '/' {
		panic(fmt.Sprintf("invalid path: %s\n", path))
	}
	return strings.Split(path, "/")
}

func splitPathTo(path string) (string, string) {
	i := strings.LastIndex(path, "/")
	return path[:i], path[i+1:]
}

// finddirectory("x", ["a"]) -> ["a_id"]
// finddirectory("x", ["a", "b"]) -> ["a_id", "b_id"]

func (self *RawFilesystem) FindDirectories(rootId ChunkID, pathComponents []string) []ChunkID {
	if len(pathComponents) < 1 {
		panic("pathComponents must be >= 1")
	}

	parentId := rootId
	pathComponentIds := make([]ChunkID, len(pathComponents))
	for i := 0; i < len(pathComponents); i++ {
		fmt.Printf("dirId='%s', pathComponents[i]='%s' i=%d\n", string(parentId), pathComponents[i], i)

		dir, readDirErr := self.ReadDir(parentId)
		if readDirErr != nil {
			panic(fmt.Sprintf("readdir failed: %s", readDirErr.Error()))
		}

		entry := dir.Get(pathComponents[i])
		if entry == nil {
			return nil
		}

		if ChunkType(entry.GetType()) != DIR_TYPE {
			return nil
		}

		parentId = ChunkID(entry.GetChunk())
		pathComponentIds[i] = parentId
	}

	return pathComponentIds
}

func (self *RawFilesystem) GetFileId(rootId ChunkID, path string) (*DirEntry, error) {
	var parentDirId ChunkID
	var filename string

	if strings.Contains(path, "/") {
		var parentDir string
		parentDir, filename = splitPathTo(path)
		parentDirIds := self.FindDirectories(rootId, splitPath(parentDir))
		parentDirId = parentDirIds[len(parentDirIds)-1]
	} else {
		parentDirId = rootId
		filename = path
	}
	fmt.Printf("rootId=%s parentDirId = %s, filename=%s\n", string(rootId), string(parentDirId), filename)

	dir, readDirErr := self.ReadDir(parentDirId)
	if readDirErr != nil {
		panic(fmt.Sprintf("readdir failed: %s", readDirErr.Error()))
	}

	entry := dir.Get(filename)
	return entry, nil
}

func (self *RawFilesystem) FileExists(rootId ChunkID, path string) bool {
	entry, err := self.GetFileId(rootId, path)
	if err != nil {
		panic(err.Error())
	}
	return entry != nil
}

func (self *RawFilesystem) recursiveCloneDirWithReplacement(rootId ChunkID, parentDir string, name string, newDirEntry *DirEntry, replaceExisting bool) (ChunkID, error) {
	var parentDirIds [] ChunkID
	var parentDirNames [] string

	if parentDir == "." {
		parentDirIds = make([]ChunkID, 1)
		parentDirIds[0] = rootId
		parentDirNames = nil
	} else {
		parentDirNames = splitPath(parentDir)
		parentDirIds = make([]ChunkID, 1, 1+len(parentDirNames))
		parentDirIds[0] = rootId
		parentDirIds = append(parentDirIds, self.FindDirectories(rootId, parentDirNames)...)
	}

	newParentIds := make([]ChunkID, len(parentDirIds))
	nextName := name

	var cloneErr error
	for i := len(parentDirIds)-1 ; i >= 0 ; i -- {
		newParentIds[i], cloneErr = self.cloneDirWithReplacement(parentDirIds[i], nextName, newDirEntry, replaceExisting)
		if cloneErr != nil {
			return INVALID_ID, cloneErr
		}

		replaceExisting = true
		if i > 0 {
			newDirEntry = &DirEntry{Name: proto.String(string(parentDirNames[i-1])), Type: proto.Int32(int32(DIR_TYPE)), Chunk: proto.String(string(newParentIds[i]))}
			nextName = parentDirNames[i-1]
		} else {
			newDirEntry = nil
			nextName = ""
		}
		// Length uint64, 	MD5 [] byte CreationTime uint64
	}

	return newParentIds[0], nil
}

func NewRawFilesystem(chunks ChunkService) *RawFilesystem {
	return &RawFilesystem{chunks: chunks}
}

func (self * RawFilesystem) GetFileMetadata(id ChunkID) (*FileMetadata, error) {
	_, metadata, err := self.chunks.Read(id,0,0)
	return metadata, err
}

func (self * RawFilesystem) ReadFile(id ChunkID, offset int64, size int64, buffer []byte) error {
	reader, _, err := self.chunks.Read(id, offset, size)
	if err != nil {
		return err
	}

	_, read_err := reader.Read(buffer)
	return read_err
}

func (self * RawFilesystem) NewDir(dir *Dir) (ChunkID, error) {
	var chunk []byte = PackDirEntries(dir)
	id := NewChunkId()
	_, err := self.chunks.Create(id, bytes.NewBuffer(chunk))
	if err != nil {
		return INVALID_ID, err
	}
	return id, nil
}

func (self * RawFilesystem) NewFile(content io.Reader) (ChunkID, *FileMetadata, error) {
	id := NewChunkId()

	metadata, createErr := self.chunks.Create(id, content)
	if createErr != nil {
		return INVALID_ID, nil, createErr
	}

	return id, metadata, nil
}

func (self *RawFilesystem) VisitReachable(id ChunkID, visitor IdVisitor) {
	panic("unimp")
}
