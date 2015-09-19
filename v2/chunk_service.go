package v2

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
)

type MemChunkService struct {
	lock   sync.Mutex
	chunks map[Key]Resource
}

func (c *MemChunkService) Get(key *Key) (Resource, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	resource := c.chunks[*key]
	if resource == nil {
		panic(fmt.Sprintf("Could not find key: %s", key))
	}

	return resource, nil
}

func (c *MemChunkService) Put(key *Key, resource Resource) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.chunks[*key] = resource

	return nil
}
func NewMemChunkService() *MemChunkService {
	return &MemChunkService{
		chunks: make(map[Key]Resource)}
}

func (self *MemChunkService) PrintDebug() {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("MemChunkService.PrintDebug %d chunks in %p\n", len(self.chunks), self)
	for id, _ := range self.chunks {
		log.Printf("chunk %s\n", id.String())
	}
}

type MemKeyIterator struct {
	keys  []*Key
	index int
}

func (self *MemKeyIterator) HasNext() bool {
	fmt.Printf("HasNext\n")
	return len(self.keys) > self.index
}

func (self *MemKeyIterator) Next() *Key {
	key := self.keys[self.index]
	self.index++
	fmt.Printf("Next() %s\n", key.String())
	return key
}

func (self *MemChunkService) Iterate() KeyIterator {
	self.lock.Lock()
	defer self.lock.Unlock()

	keys := make([]*Key, 0, len(self.chunks))
	for key, _ := range self.chunks {
		// make a copy because we're going to append a pointer to this to the list
		k := key
		keys = append(keys, &k)
	}

	return &MemKeyIterator{keys: keys, index: 0}
}

//Get(key *Key) Resource;
//Put(key *Key, resource Resource);

type MemResource struct {
	data []byte
}

func NewMemResource(data []byte) Resource {
	return &MemResource{data: data}
}

func (r *MemResource) AsBytes() []byte {
	return r.data
}

func (r *MemResource) GetLength() int64 {
	return int64(len(r.data))
}

func (r *MemResource) GetReader() io.Reader {
	return bytes.NewBuffer(r.data)
}
