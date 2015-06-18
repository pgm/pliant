package v2

import (
	"encoding/base64"
	"bytes"
	"strings"
)

type Key [32] byte;

var EMPTY_DIR_KEY_ Key = ([32]byte{1,0,0,0,0,0,0,0,0,0,0});
var EMPTY_DIR_KEY *Key = &EMPTY_DIR_KEY_;


func (k *Key) String() string {
	b := bytes.NewBuffer(make([]byte,100))
	e := base64.NewEncoder(base64.StdEncoding, b)
	e.Write((*k)[:])
	return string(b.Bytes())
}

func NewKey(key string) *Key {
	e := base64.NewDecoder(base64.StdEncoding, strings.NewReader(key[:]))
	b := bytes.NewBuffer(make([]byte,100))
	b.ReadFrom(e)
	return KeyFromBytes(b.Bytes())
}

func KeyFromBytes(bytes []byte) *Key {
	var k Key;
	copy(k[:], bytes);
	return &k;
}

type Directory interface {
	// An immutable map of name -> FileMetadata.  Mutations (Put, Remove) return the key of newly created Directory
	// All methods threadsafe because they never mutate structures and all parameters are immutable.

	Get(name string) *FileMetadata;
	Put(name string, metadata *FileMetadata) *Key;
	Remove(name string) *Key;
	Iterate() Iterator;
}

type Iterator interface {
	HasNext() bool;
	Next() (string, *FileMetadata);
}

type DirectoryService interface {
	GetDirectory(key *Key) Directory;
}

type Resource interface {
	AsBytes() []byte
}

type ChunkService interface {
	// all methods are threadsafe
	Get(key *Key) Resource;
	Put(key *Key, resource Resource);
}

type TagService interface {
	Put(name string, key *Key)
	Get(name string) *Key
}


type Lease struct {
}

