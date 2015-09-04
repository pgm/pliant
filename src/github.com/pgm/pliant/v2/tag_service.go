package v2

import (
	"sync"
)

type MemTagService struct {
	lock sync.Mutex
	tags map[string]*Key
}

func NewMemTagService() *MemTagService {
	return &MemTagService{tags: make(map[string]*Key)}
}

func (m *MemTagService) Get(tag string) *Key {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.tags[tag]
}

func (m *MemTagService) Put(tag string, key *Key) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.tags[tag] = key
}
