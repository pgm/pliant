package v2

import (
	"bytes"
	"encoding/base64"
	"io"
	"strings"
	"fmt"
)

type Key [32]byte

var EMPTY_DIR_KEY_ Key = ([32]byte{1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1,
		1, 1, 1, 1})
var EMPTY_DIR_KEY *Key = &EMPTY_DIR_KEY_

const KEY_STR_LEN = 44

//func (k Key) String() string {
//	return (&k).String()
//}

func (k Key) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 100))
	e := base64.NewEncoder(base64.StdEncoding, b)
	e.Write((k)[:])
	e.Close()
	s := string(b.Bytes())
	if len(s) != KEY_STR_LEN {
		panic(fmt.Sprintf("invalid length: %s", len(s)))
	}
	return s
}

func (k *Key) AsBytes() []byte {
	return k[:]
}

func NewKey(key string) *Key {
	if len(key) != KEY_STR_LEN {
		panic(fmt.Sprintf("invalid length: %s", len(key)))
	}
	e := base64.NewDecoder(base64.StdEncoding, strings.NewReader(key[:]))
	b := bytes.NewBuffer(make([]byte, 0, 100))
	b.ReadFrom(e)
	return KeyFromBytes(b.Bytes())
}

func KeyFromBytes(bytes []byte) *Key {
	var k Key
	copy(k[:], bytes)
	return &k
}

type Directory interface {
	// An immutable map of name -> FileMetadata.  Mutations (Put, Remove) return the key of newly created Directory
	// All methods threadsafe because they never mutate structures and all parameters are immutable.

	Get(name string) *FileMetadata
	Put(name string, metadata *FileMetadata) *Key
	Remove(name string) *Key
	Iterate() Iterator
}

type Iterator interface {
	HasNext() bool
	Next() (string, *FileMetadata)
}

type DirectoryService interface {
	GetDirectory(key *Key) Directory
}

type Resource interface {
	AsBytes() []byte
	GetReader() io.Reader
}

type FileResource struct {
	filename string
}

type ChunkService interface {
	// all methods are threadsafe
	Get(key *Key) Resource
	Put(key *Key, resource Resource)
}

type KeyIterator interface {
	HasNext() bool
	Next() *Key
}

type IterableChunkService interface {
	ChunkService
	Iterate() KeyIterator
}

type TagService interface {
	Put(name string, key *Key)
	Get(name string) *Key
	ForEach(callback func(name string, key *Key))
}

type Lease struct {
}
