package low

import (
	"fmt"
	"io"
	"bytes"
	"sync"
	"errors"
	"crypto/md5"
	"github.com/golang/protobuf/proto"
	"time"
//	"log"
	"log"
//	"github.com/boltdb/bolt"

)


// all methods must be threadsafe
// Responsible for writing an entire chunk at a time.  Allows for reading pieces of a chunk if desired.
// Essentially interface for a KV store
// open questions:  What is the max size of a chunk?
type ChunkService interface {
	HasChunk(id ChunkID) (bool, error)
	Read(id ChunkID, offset int64, size int64) (io.Reader, *FileMetadata, error)
	Create(id ChunkID, data io.Reader) (*FileMetadata, error)
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
	metadata map [ChunkID] *FileMetadata
	lock sync.Mutex
}

func NewMemChunkService() *MemChunkService {
	return &MemChunkService{
		table: make(map[ChunkID] []byte),
		metadata: make(map[ChunkID] *FileMetadata)}
}

func (self *MemChunkService) PrintDebug() {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("%d chunks in %p\n", len(self.metadata), self)
	for id, _ := range(self.metadata) {
		log.Printf("chunk %s\n", string(id))
	}
}

func (self *MemChunkService) HasChunk(id ChunkID) (bool, error) {
	self.lock.Lock()
	_, hasKey := self.table[id]
	self.lock.Unlock()

	return hasKey, nil
}

func (self *MemChunkService) Read(id ChunkID, offset int64, size int64) (io.Reader, *FileMetadata, error) {
	self.lock.Lock()
	buffer, ok := self.table[id]
	metadata, _ := self.metadata[id]
	self.lock.Unlock()

	if ok {
		if size <0  || offset+size > int64(len(buffer)) {
			size = int64(len(buffer))-offset
		}
		return bytes.NewReader(buffer[offset:offset+size]), metadata, nil
	} else {
		return nil, nil, errors.New(fmt.Sprintf("No such ID: '%s'", string(id)))
	}
}

func (self *MemChunkService) Create(id ChunkID, data io.Reader) (*FileMetadata, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	buffer.ReadFrom(data)
	log.Printf("MemChunkService: Create(%s) len=%d", string(id), buffer.Len())

	b := buffer.Bytes()

	hash := md5.Sum(b)
	metadata := &FileMetadata{Length: proto.Int64(int64(len(b))), Md5: hash[:], CreationTime: proto.Int64(time.Now().Unix())}

	self.lock.Lock()
	self.table[id] = b
	self.metadata[id] = metadata
	self.lock.Unlock()

	return metadata, nil
}

func (self *MemChunkService) Free(id ChunkID) error {
	self.lock.Lock()
	delete(self.table, id)
	delete(self.metadata, id)
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

	lock sync.Mutex
	is_local map[ChunkID] bool
}


func NewCachingChunkService(local ChunkService,
	remote ChunkService) * CachingChunkService {
	return &CachingChunkService{local: local, remote: remote, is_local: make(map[ChunkID] bool)}
}

func copyChunk(id ChunkID, src ChunkService, dst ChunkService) error {
	log.Printf("Warning: hardcoded max chunk size")
	reader, _, err := src.Read(id, 0, 1000000)
	if err != nil {
		log.Printf("Got Error: %s", err.Error())
		return err
	}
	log.Printf("Calling create")
	_, err = dst.Create(id, reader)
	return err
}

func (self *CachingChunkService) IsChunkLocalOnly(id ChunkID) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	value, ok := self.is_local[id]
	if !ok {
		return false
	}

	return value
}

func (self *CachingChunkService) UpdateChunkStatus(id ChunkID, isLocal bool) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.is_local[id] = isLocal
}

func (self *CachingChunkService) Read(id ChunkID, offset int64, size int64) (io.Reader, *FileMetadata, error) {
	log.Printf("CachingChunkService: Read %s %d %d", string(id), offset, size)
	has_chunk, err := self.local.HasChunk(id)
	if err != nil {
		panic(err.Error())
	}

	if(has_chunk) {
		log.Printf("CachingChunkService: had chunk locally")
		return self.local.Read(id, offset, size)
	}

	// if we don't have it locally, then pull it from the remote
	log.Printf("CachingChunkService: copying chunk from remote")
	copyErr := copyChunk(id, self.remote, self.local)
	if copyErr == nil {
		return nil, nil, copyErr
	}
	h, _ := self.local.HasChunk(id)
	if ! h {
		panic("Chunk missing after copy")
	}

	// Then serve it from local
	return self.local.Read(id, offset, size)
}

func (self *CachingChunkService) Create(id ChunkID, data io.Reader) (*FileMetadata, error) {
	metadata, err := self.local.Create(id, data)
	if err == nil {
		self.UpdateChunkStatus(id, true)
	}

	return metadata, err
}

func (self *CachingChunkService) Free(id ChunkID) error {
	return self.local.Free(id)
}

func (self *CachingChunkService) HasChunk(id ChunkID) (bool, error) {
	panic("unimp")
}