package v2

type Key [32] byte;

var EMPTY_DIR_KEY *Key = &([32]byte{1,0,0,0,0,0,0,0,0,0,0});

func (k *Key) String() string {
	panic();
}

func NewKey(key string) *Key {
	panic()
}

type Directory interface {
	// An immutable map of name -> FileMetadata.  Mutations (Put, Remove) return the key of newly created Directory
	// All methods threadsafe because they never mutate structures and all parameters are immutable.

	Get(name string) *FileMetadata;
	Put(name string, metadata *FileMetadata) *Key;
	Remove(name string) *Key;
	Iterate() *Iterator;
}

type Iterator interface {
	HasNext() bool;
	Next() (string, *FileMetadata);
}

type DirectoryService interface {
	GetDirectory(key *Key) *Directory;
}

type Resource interface {
}

type ChunkService interface {
	// all methods are threadsafe
	Get(key *Key) *Resource;
	Put(key *Key, *Resource);
}

type TagService interface {
	Put(name string, key *Key)
	Get(name string) *Key
}


type Lease struct {
}
