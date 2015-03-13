package fffs_go

import (
	"fmt"
	"io"
	"bytes"
	"sync"
	"errors"
	"code.google.com/p/go-uuid/uuid"
	"strings"
	"github.com/golang/protobuf/proto"
	"crypto/md5"
	"time"
)

type ChunkID string
type ChunkType int32

type IdVisitor func(Chunk ChunkID)
type LabelVisitor func(Label string, Chunk ChunkID)

const (
	DIR_TYPE ChunkType = iota
	FILE_TYPE
)

/*
type DirEntry struct {
	Name  string
	Type  ChunkType
	Chunk ChunkID
	// file metadata is duplicated here because:
	// 1. it is immutable so duplication only costs space
	// 2. it avoids a round trip if we need it
	Length       uint64
	MD5          [] byte
	CreationTime uint64
}

type Dir struct {
	Entries [] *DirEntry
}

// should there be metadata for DirEntry lists as well as files?
type FileMetadata struct {
	Length       uint64
	MD5          []byte
	CreationTime uint64
}
*/

type Transient interface {
	GetReader() io.Reader
}

// all methods must be threadsafe
// Responsible for writing an entire chunk at a time.  Allows for reading pieces of a chunk if desired.
// Essentially interface for a KV store
// open questions:  What is the max size of a chunk?
type ChunkService interface {
	HasChunk(id ChunkID) (bool, error)
	Read(id ChunkID, offset int64, size int64) (io.Reader, error)
	Create(id ChunkID, data io.Reader) (int64, []byte, error)
	Free(id ChunkID) error
}

type IterableChunkService interface {
ChunkService
	// what does it mean for VisitEach to be threadsafe?  Are additions/removals allowed while this is iterating?
	// I think the answer must be yes, and there in lies the uncertanity
	VisitEach(visitor IdVisitor) error
}

type MemChunkService struct {
	table map [ChunkID] []byte
	lock sync.Mutex
}

func NewMemChunkService() *MemChunkService {
	return &MemChunkService{table: make(map[ChunkID] []byte)}
}

func (self *MemChunkService) HasChunk(id ChunkID) (bool, error) {
	self.lock.Lock()
	_, hasKey := self.table[id]
	self.lock.Unlock()

	return hasKey, nil
}

func (self *MemChunkService) Read(id ChunkID, offset int64, size int64) (io.Reader, error) {
	self.lock.Lock()
	buffer, ok := self.table[id]
	self.lock.Unlock()

	if ok {
		if size < 0 {
			size = int64(len(buffer))-offset
		}

		if offset+size > int64(len(buffer)) {
			return nil, errors.New("Attempted read which would exceed bounds")
		} else {
			return bytes.NewReader(buffer[offset:offset+size]), nil
		}
	} else {
		return nil, errors.New(fmt.Sprintf("No such ID: '%s'", string(id)))
	}
}

func (self *MemChunkService) Create(id ChunkID, data io.Reader) (int64, []byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	buffer.ReadFrom(data)

	b := buffer.Bytes()

	self.lock.Lock()
	self.table[id] = b
	self.lock.Unlock()

	hash := md5.Sum(b)
	return int64(len(b)), hash[:], nil
}

func (self *MemChunkService) Free(id ChunkID) error {
	self.lock.Lock()
	delete(self.table, id)
	self.lock.Unlock()

	return nil
}

func (self *MemChunkService) VisitEach(visitor IdVisitor) error {
	// simulate behavior of stable iteration even though list may mutate during iteration
	self.lock.Lock()
	keys := make([]ChunkID, 0, len(self.table))
	for k := range self.table {
		keys = append(keys, k)
	}
	self.lock.Unlock()
	for _, sk := range keys {
		visitor(sk)
	}

	return nil
}

type CachingChunkService struct {
	local ChunkService
	remote ChunkService
	is_local map[ChunkID] bool
}

func copyChunk(id ChunkID, src ChunkService, dst ChunkService) error {
	panic("unimp")
}

func (self *CachingChunkService) IsChunkLocalOnly(id ChunkID) bool {
//	_, ok := self.is_local[id]
//	return ok
	panic("unimp")
}

func (self *CachingChunkService) UpdateChunkStatus(id ChunkID, isLocal bool) {
	panic("unimp")
}

func (self *CachingChunkService) Read(id ChunkID, offset int64, size int64) (io.Reader, error) {
	has_chunk, err := self.local.HasChunk(id)
	if err != nil {
		panic(err.Error())
	}

	if(has_chunk) {
		return self.local.Read(id, offset, size)
	}

	// if we don't have it locally, then pull it from the remote
	copyErr := copyChunk(id, self.remote, self.local)
	if copyErr == nil {
		return nil, copyErr
	}

	// Then serve it from local
	return self.local.Read(id, offset, size)
}

func (self *CachingChunkService) Create(id ChunkID, data io.Reader) (int64, []byte, error) {
	length, md5, err := self.local.Create(id, data)
	if err == nil {
		self.UpdateChunkStatus(id, true)
	}

	return length, md5, err
}

func (self *CachingChunkService) Free(id ChunkID) error {
	return self.local.Free(id)
}

// Interface for a map from string -> a chunk id
type LabelService interface {
	HasLabel(label string) (bool, error)
	GetRoot(label string) (ChunkID, error)
	RemoveLabel(label string) error
	UpdateLabel(label string, root_dir_id ChunkID) error
	VisitEach(visitor LabelVisitor) error
}

/////////////////////////////////////////////////

type MemLabelService struct {
	labels map[string] ChunkID
	lock sync.Mutex
}

func NewMemLabelService() *MemLabelService {
	return &MemLabelService{labels: make(map[string] ChunkID)}
}

func (self * MemLabelService) HasLabel(label string) (bool, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	_, hasKey := self.labels[label]

	return hasKey, nil
}

func (self * MemLabelService) RemoveLabel(label string) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.labels, label)

	return nil
}

func (self * MemLabelService) UpdateLabel(label string, root_dir_id ChunkID) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.labels[label] = root_dir_id

	return nil
}

func (self *MemLabelService) GetRoot(label string) (ChunkID, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	value, hasKey := self.labels[label]

	if hasKey {
		return value, nil
	} else {
		return INVALID_ID, errors.New("No such label")
	}
}

type LabelAndChunkId struct {
	label string
	id    ChunkID
}

func (self * MemLabelService) VisitEach(visitor LabelVisitor) error {
	// simulate behavior of stable iteration even though list may mutate during iteration
	self.lock.Lock()
	keys := make([]LabelAndChunkId, 0, len(self.labels))
	for k, v := range self.labels {
		keys = append(keys, LabelAndChunkId{label: k, id: v})
	}
	self.lock.Unlock()
	for _, sk := range keys {
		visitor(sk.label, sk.id)
	}

	return nil
}

/////////////////////////////////////////////////

const EMPTY_DIR_ID = ChunkID("empty")
const INVALID_ID = ChunkID("invalid")

// Low level filesystem interface.  Minimal operations for creating and reading files/directories
// all operations based chunk id, or a root chunk id and a path under that dir
//type RawFilesystem interface {
//	ReadDir(id ChunkID) (*Dir, error)
//	GetFileMetadata(id ChunkID) (*FileMetadata, error)
//	ReadFile(id ChunkID, offset int, size int, buffer []byte) error
//	NewDir(entries *Dir) (ChunkID, error)
//	NewFile(content io.Reader) (ChunkID, error)
//
//	CloneWithReplacement(rootId ChunkID, path string, newId ChunkID, allowReplacement bool) (ChunkID, error)
//	GetDirEntry(rootId ChunkID, path string) *DirEntry
//}

func NewChunkId() ChunkID {
	return ChunkID(uuid.NewRandom().String())
}

type RawFilesystem struct {
	chunks   ChunkService
	metadata MetadataService
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

func NewRawFilesystem(chunks ChunkService, metadata MetadataService) *RawFilesystem {
	return &RawFilesystem{chunks: chunks, metadata: metadata}
}

func PackDirEntries(dir *Dir) []byte {
	data, err := proto.Marshal(dir)
	if err != nil {
		panic("Couldn't marshal Dir object")
	}
	return data
}

func UnpackDirEntries(r io.Reader) *Dir {
	dest := &Dir{}
	buffer := bytes.Buffer{}
	_, readErr := buffer.ReadFrom(r)
	if readErr != nil {
		panic("Could not read")
	}
	err := proto.Unmarshal(buffer.Bytes(), dest)
	if err != nil {
		panic(fmt.Sprintf("Could not unmarshal dir: %s", err.Error()))
	}

	return dest
}


func (self * RawFilesystem) ReadDir(id ChunkID) (*Dir, error) {
	if id == EMPTY_DIR_ID {
		return &Dir{}, nil;
	}

	chunk, err := self.chunks.Read(id, 0, -1)
	if err != nil {
		return nil, err
	}
	return UnpackDirEntries(chunk), nil
}

func (self * RawFilesystem) GetFileMetadata(id ChunkID) (*FileMetadata, error) {
	return self.metadata.GetFileMetadata(id)
}

func (self * RawFilesystem) ReadFile(id ChunkID, offset int64, size int64, buffer []byte) error {
	reader, err := self.chunks.Read(id, offset, size)
	if err != nil {
		return err
	}

	_, read_err := reader.Read(buffer)
	return read_err
}

func (self * RawFilesystem) NewDir(dir *Dir) (ChunkID, error) {
	var chunk []byte = PackDirEntries(dir)
	id := NewChunkId()
	_, _, err := self.chunks.Create(id, bytes.NewBuffer(chunk))
	if err != nil {
		return INVALID_ID, err
	}
	return id, nil
}

func (self * RawFilesystem) NewFile(content io.Reader) (ChunkID, error) {
	id := NewChunkId()

	length, md5, createErr := self.chunks.Create(id, content)
	if createErr != nil {
		return INVALID_ID, createErr
	}

	metadata := FileMetadata{Length: proto.Int64(length), Md5: md5, CreationTime: proto.Int64(time.Now().Unix())}
	err := self.metadata.SetFileMetadata(id, &metadata)
	if err != nil {
		return INVALID_ID, err
	}

	return id, nil
}


// TODO: Put cross cutting logic for GC somewhere
// TODO: Put cross-cutting logic for push/pull somewhere
// TODO: Write tests for unlink operation
// TODO: Write tests for higher level filesystem operations

func main() {
	fmt.Printf("Start")
}

