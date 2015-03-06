package main

import (
	"fmt"
	"io"
	"bytes"
	"sync"
	"errors"
	"code.google.com/p/go-uuid/uuid"
	"strings"
)

type ChunkID string
type ChunkType byte

type IdVisitor func(Chunk ChunkID)
type LabelVisitor func(Label string, Chunk ChunkID)

const (
	DIR_TYPE ChunkType = iota
	FILE_TYPE
)

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

type Transient interface {
	GetReader() io.Reader
}

// all methods must be threadsafe
// Responsible for writing an entire chunk at a time.  Allows for reading pieces of a chunk if desired.
// Essentially interface for a KV store
// open questions:  What is the max size of a chunk?
type ChunkService interface {
	HasChunk(id ChunkID) (bool, error)
	Read(id ChunkID, offset int, size int) (io.Reader, error)
	Create(id ChunkID, data io.Reader) error
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

func (self *MemChunkService) Read(id ChunkID, offset int, size int) (io.Reader, error) {
	self.lock.Lock()
	buffer, ok := self.table[id]
	self.lock.Unlock()

	if ok {
		if offset+size > len(buffer) {
			return nil, errors.New("Attempted read which would exceed bounds")
		} else {
			return bytes.NewReader(buffer[offset:offset+size]), nil
		}
	} else {
		return nil, errors.New("No such ID")
	}
}

func (self *MemChunkService) Create(id ChunkID, data io.Reader) error {
	buffer := bytes.NewBuffer(make([]byte, 1000))
	buffer.ReadFrom(data)

	self.lock.Lock()
	self.table[id] = buffer.Bytes()
	self.lock.Unlock()

	return nil
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
	metadata ChunkService
}

func (self *Dir) cloneWithReplacement(newDirEntry *DirEntry, replaceExisting bool) (*Dir, error) {
	newEntries := make([]*DirEntry, 0, len(self.Entries))
	found := false
	for i := range (self.Entries) {
		if self.Entries[i].Name == newDirEntry.Name {
			if replaceExisting {
				newEntries = append(newEntries, newDirEntry)
				found = true
			} else {
				return nil, errors.New("already exists")
			}
		} else {
			newEntries = append(newEntries, self.Entries[i])
		}
	}

	if !found {
		newEntries = append(newEntries, newDirEntry)
	}

	return &Dir{Entries: newEntries}, nil
}

func (self *Dir) Get(name string) *DirEntry {
	for i := range (self.Entries) {
		if self.Entries[i].Name == name {
			return self.Entries[i]
		}
	}

	return nil
}

func (self *RawFilesystem) cloneDirWithReplacement(dirId ChunkID, newDirEntry *DirEntry, replaceExisting bool) (ChunkID, error) {
	dir, readDirErr := self.ReadDir(dirId)
	if readDirErr != nil {
		return INVALID_ID, readDirErr
	}

	newDir, cloneError := dir.cloneWithReplacement(newDirEntry, replaceExisting)
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
	return strings.Split(path, "/")
}

func splitPathTo(path string) (string, string) {
	i := strings.LastIndex(path, "/")
	return path[:i], path[i+1:]
}

func (self *RawFilesystem) FileExists(rootId ChunkID, path string) bool {
	parentDir, filename := splitPathTo(path)
	var parentDirNames []string = splitPath(parentDir);
	dirId := rootId
	for i:=0; i<len(parentDirNames);i++ {
		dir, readDirErr := self.ReadDir(dirId)
		if readDirErr != nil {
			panic("readdir failed")
		}
		dirId = dir.Get(parentDirNames[i]).Chunk
	}
	dir, readDirErr := self.ReadDir(dirId)
	if readDirErr != nil {
		panic("readdir failed")
	}
	entry := dir.Get(filename)
	return entry != nil
}

func (self *RawFilesystem) recursiveCloneDirWithReplacement(rootId ChunkID, parentDir string, newDirEntry *DirEntry, replaceExisting bool) (ChunkID, error) {
	root, readDirErr := self.ReadDir(rootId)
	if readDirErr != nil {
		return INVALID_ID, readDirErr
	}

	var parentDirNames []string = splitPath(parentDir);
	parents := make([]ChunkID, 0, len(parentDirNames))
	newParents := make([]ChunkID, 0, len(parentDirNames))

	// resolve each dir that ultimately needs to be replaced
	curDir := root
	parents = append(parents, rootId)
	for _, name := range (parentDirNames) {
		entry := curDir.Get(name)
		if entry == nil {
			return INVALID_ID, errors.New("Path did not exist")
		}
		if entry.Type != DIR_TYPE {
			return INVALID_ID, errors.New("Path was not a directory")
		}
		curDir, readDirErr = self.ReadDir(entry.Chunk)
		if readDirErr != nil {
			return INVALID_ID, errors.New("Could not read dir")
		}
		parents = append(parents, entry.Chunk)
	}

	var cloneErr error
	for i := len(parents) ; i >= 0 ; i -- {
		newParents[i], cloneErr = self.cloneDirWithReplacement(parents[i], newDirEntry, true)
		newDirEntry = &DirEntry{Name: parentDirNames[i], Type: DIR_TYPE, Chunk: newParents[i]}
		// Length uint64, 	MD5 [] byte CreationTime uint64
		if cloneErr != nil {
			return INVALID_ID, cloneErr
		}
	}
	replaceExisting = true

	return newParents[0], nil
}

func NewRawFilesystem(chunks ChunkService, metadata ChunkService) *RawFilesystem {
	return &RawFilesystem{chunks: chunks, metadata: metadata}
}

func PackDirEntries(dir *Dir) []byte {
	panic("unimp")
}

func UnpackDirEntries(r io.Reader) *Dir {
	panic("unimp")
}

func PackFileMetadata(metadata *FileMetadata) []byte {
	panic("unimp")
}

func UnpackFileMetadata(r io.Reader) *FileMetadata {
	panic("unimp")
}

func (self * RawFilesystem) ReadDir(id ChunkID) (*Dir, error) {
	chunk, err := self.chunks.Read(id, 0, -1)
	if err != nil {
		return nil, err
	}
	return UnpackDirEntries(chunk), nil
}

func (self * RawFilesystem) GetFileMetadata(id ChunkID) (*FileMetadata, error) {
	chunk, err := self.metadata.Read(id, 0, -1)
	if err != nil {
		return nil, err
	}
	return UnpackFileMetadata(chunk), nil
}

func (self * RawFilesystem) ReadFile(id ChunkID, offset int, size int, buffer []byte) error {
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
	err := self.chunks.Create(id, bytes.NewBuffer(chunk))
	if err != nil {
		return INVALID_ID, err
	}
	return id, nil
}

func (self * RawFilesystem) NewFile(content io.Reader) (ChunkID, error) {
	var metadata FileMetadata

	id := NewChunkId()
	self.chunks.Create(id, content)
	err := self.metadata.Create(id, bytes.NewBuffer(PackFileMetadata(&metadata)))
	if err != nil {
		return INVALID_ID, err
	}
	return id, nil
}

// higher level filesystem interface.  Incorporates concept of label for identifying root dir, and paths within that tree.
// all operations based on label and path
type Filesystem interface {
	MakeDir(label string, vpath string) error
	Label(new_label string, existing_label string, vpath string) error
	Rename(label string, existing_vpath string, new_vpath string) error
	ReadFile(label string, vpath string, offset int, size int, buffer []byte) error
	WriteFile(label string, vpath string, content io.Reader) error
	Unlink(label string, vpath string) error
	ReadDir(label string, vpath string) ([]DirEntry, error)
	FileExists(label string, vpath string) (bool, error)
}

type FilesystemImp struct {
	labels  LabelService
	fs      RawFilesystem
	mapLock sync.Mutex
	labelLocks map[string] *sync.RWMutex
}

func (self *FilesystemImp) getLabelLock(label string) *sync.RWMutex {
	self.mapLock.Lock()
	defer self.mapLock.Unlock()

	lock, exists := self.labelLocks[label]

	if !exists {
		lock = new(sync.RWMutex)
		self.labelLocks[label] = lock
	}

	return lock
}

func (self *FilesystemImp) MakeDir(label string, vpath string) error {
	wlock := self.getLabelLock(label)
	wlock.Lock()
	defer wlock.Unlock()

	origRootId, getRootErr := self.labels.GetRoot(label)
	if getRootErr != nil {
		return getRootErr
	}

	parentPath, name := splitPathTo(vpath)
	newRootId, cloneErr := self.fs.recursiveCloneDirWithReplacement(origRootId, parentPath, &DirEntry{Name: name, Chunk: EMPTY_DIR_ID, Type: DIR_TYPE}, false)
	if cloneErr != nil {
		return cloneErr
	}

	updateLabelErr := self.labels.UpdateLabel(label, newRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}

/*
func (self *FilesystemImp) Label(label string, existing_label string, vpath string) error {
	rlock := self.getLabelLock(existing_label)
	rlock.RLock()
	rootId := self.labels.GetRoot(existing_label)
	rlock.RUnlock()

	dirId := self.getDirId(rootId, vpath)

	wlock := self.getLabelLock(label)
	wlock.Lock()
	self.labels.UpdateLabel(label, dirId)
	wlock.Unlock()
}
*/

// TODO: Put cross cutting logic for GC somewhere
// TODO: Put cross-cutting logic for push/pull somewhere
// TODO: find protobuffer support for GO.  (or write manual packing?)


func main() {
	fmt.Printf("Start")
}

