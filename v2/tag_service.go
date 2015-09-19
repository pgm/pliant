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

func (m *MemTagService) Get(tag string) (*Key, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.tags[tag], nil
}

func (m *MemTagService) Put(tag string, key *Key) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.tags[tag] = key

	return nil
}

func (m *MemTagService) ForEach(callback func(name string, key *Key)) {
	m.lock.Lock()

	mapCopy := make(map[string]*Key)
	for k, v := range m.tags {
		mapCopy[k] = v
	}

	m.lock.Unlock()

	for k, v := range mapCopy {
		callback(k, v)
	}
}
