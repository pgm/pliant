package low

import (
	"fmt"
	"io"
	"bytes"
	"sync"
	"errors"
	"code.google.com/p/go-uuid/uuid"
	"github.com/golang/protobuf/proto"
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

	chunk, _, err := self.chunks.Read(id, 0, -1)
	if err != nil {
		return nil, err
	}
	return UnpackDirEntries(chunk), nil
}



// TODO: Put cross cutting logic for GC somewhere
// TODO: Put cross-cutting logic for push/pull somewhere
// TODO: Write tests for unlink operation
// TODO: Write tests for higher level filesystem operations

func main() {
	fmt.Printf("Start")
}

