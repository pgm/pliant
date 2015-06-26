package v2

import (
	"sync"
	"log"
	"fmt"
	"io"
	"bytes"
)

type MemChunkService struct {
	lock sync.Mutex
	chunks map[Key] Resource
}

func (c *MemChunkService) Get(key *Key) Resource {
	c.lock.Lock()
	defer c.lock.Unlock()

	resource := c.chunks[*key];
	if resource == nil {
		panic(fmt.Sprintf("Could not find key: %s", key))
	}
	return resource
}

func (c *MemChunkService) Put(key *Key, resource Resource) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.chunks[*key] = resource
}
func NewMemChunkService() *MemChunkService {
	return &MemChunkService{
		chunks: make(map[Key] Resource)}
}

func (self *MemChunkService) PrintDebug() {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("%d chunks in %p\n", len(self.chunks), self)
//	for id, _ := range(self.table) {
//		log.Printf("chunk %s\n", string(id))
//	}
}

//Get(key *Key) Resource;
//Put(key *Key, resource Resource);

type MemResource struct {
	data []byte;
}

func NewMemResource(data []byte) Resource {
	return &MemResource{data: data};
}

func (r *MemResource ) AsBytes() []byte {
	return r.data;
}

func (r *MemResource ) GetReader() io.Reader {
	return bytes.NewBuffer(r.data);
}

//func (self *MemChunkService) Get(key *Key) Resource {
//	self.lock.Lock()
//	data, hasKey := self.table[*key]
//	self.lock.Unlock()
//
//	if !hasKey {
//		panic("No such key");
//	}
//
//	return NewMemResource(data)
//}
//
//func (self *MemChunkService) Put(key *Key, resource Resource) {
//	self.lock.Lock()
//	self.table[*key] = MemResource(resource).data;
//	self.lock.Unlock()
//}
