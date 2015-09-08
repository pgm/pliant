package v2

import (
	"crypto/sha256"
	"fmt"
	"github.com/golang/protobuf/proto"
	"sort"
	"strconv"
)

var EMPTY_DIR = Leaf{entries: make([]*LeafEntry, 0, 10)}

type NodeStore interface {
	// serialization and deserialization of BTree nodes

	GetLeaf(key *Key) *Leaf
	StoreLeaf(leaf *Leaf) *Key
	GetBranch(key *Key) *Branch
	StoreBranch(branch *Branch) *Key

	GetDirectory(key *Key) *Directory
}

type LeafEntry struct {
	name     string
	metadata *FileMetadata
}

type Leaf struct {
	entries []*LeafEntry
}

type BranchEntry struct {
	isLeaf   bool
	child    Key
	lastName string
}

type Branch struct {
	children []BranchEntry
}

type TreeSettings struct {
	MaxBlockSize int
	MinBlockSize int
}

type TreeStats struct {
	leavesSplit    uint32
	branchesSplit  uint32
	branchesMerged uint32
	leavesMerged   uint32
	valuesReplaced uint32
	valuesInserted uint32
}

func CopyLeafWithMutation(leaf *Leaf, replaceIndex int, entry *LeafEntry) *Leaf {
	newLeaf := &Leaf{entries: make([]*LeafEntry, len(leaf.entries))}
	for i := 0; i < len(leaf.entries); i++ {
		if i == replaceIndex {
			newLeaf.entries[i] = entry
		} else {
			newLeaf.entries[i] = leaf.entries[i]
		}
	}

	return newLeaf
}

func CopyLeafWithInsertion(leaf *Leaf, insertIndex int, entry *LeafEntry) *Leaf {
	newLeaf := &Leaf{entries: make([]*LeafEntry, len(leaf.entries)+1)}
	for i := 0; i < insertIndex; i++ {
		newLeaf.entries[i] = leaf.entries[i]
	}
	newLeaf.entries[insertIndex] = entry
	for i := insertIndex; i < len(leaf.entries); i++ {
		newLeaf.entries[i+1] = leaf.entries[i]
	}

	return newLeaf
}

func CopyLeafWithRemoval(leaf *Leaf, removeIndex int) *Leaf {
	newLeaf := &Leaf{entries: make([]*LeafEntry, len(leaf.entries)-1)}
	for i := 0; i < removeIndex; i++ {
		newLeaf.entries[i] = leaf.entries[i]
	}
	for i := removeIndex + 1; i < len(leaf.entries); i++ {
		newLeaf.entries[i-1] = leaf.entries[i]
	}

	return newLeaf
}

func (leaf *Leaf) get(name string) *FileMetadata {
	i := sort.Search(len(leaf.entries), func(i int) bool {
		return leaf.entries[i].name >= name
	})

	if i < len(leaf.entries) && leaf.entries[i].name == name {
		return leaf.entries[i].metadata
	}

	return nil
}

func (leaf *Leaf) insert(entry *LeafEntry) *Leaf {
	i := sort.Search(len(leaf.entries), func(i int) bool {
		return leaf.entries[i].name >= entry.name
	})

	// replace existing entry
	var newLeaf *Leaf
	if len(leaf.entries) > i && leaf.entries[i].name == entry.name {
		newLeaf = CopyLeafWithMutation(leaf, i, entry)
		//		stats.valuesReplaced ++;
	} else {
		// otherwise we have an insertion
		newLeaf = CopyLeafWithInsertion(leaf, i, entry)
		//		stats.valuesInserted ++;
	}

	return newLeaf
}

func (leaf *Leaf) remove(name string) *Leaf {
	i := sort.Search(len(leaf.entries), func(i int) bool {
		return leaf.entries[i].name >= name
	})

	if leaf.entries[i].name == name {
		newLeaf := CopyLeafWithRemoval(leaf, i)
		return newLeaf
	} else {
		// otherwise, no entry with that name found, so do nothing
		return nil
	}
}

type LeafDirService struct {
	chunks ChunkService
}

type LeafDir struct {
	chunks ChunkService
	key    *Key
}

func NewLeafDirService(chunks ChunkService) *LeafDirService {
	return &LeafDirService{chunks: chunks}
}

func (s *LeafDirService) GetDirectory(key *Key) Directory {
	return &LeafDir{chunks: s.chunks, key: key}
}

func UnpackLeafEntry(entry *LeafRecordEntry) *LeafEntry {
	return &LeafEntry{name: entry.GetName(), metadata: entry.GetMetadata()}
}

func UnpackLeaf(data []byte) *Leaf {
	dest := &LeafRecord{}
	err := proto.Unmarshal(data, dest)
	if err != nil {
		panic(fmt.Sprintf("Could not unmarshal leaf: %s", err.Error()))
	}

	// convert LeafRecord to Leaf
	entries := make([]*LeafEntry, 0, len(dest.GetEntries()))
	for _, entry := range dest.GetEntries() {
		entries = append(entries, UnpackLeafEntry(entry))
	}
	return &Leaf{entries: entries}
}

func PackLeaf(leaf *Leaf) []byte {
	entries := make([]*LeafRecordEntry, 0, len(leaf.entries))
	for _, entry := range leaf.entries {
		if entry.metadata == nil {
			panic("entry.metadata")
		}
		e := &LeafRecordEntry{Name: &entry.name, Metadata: entry.metadata}
		entries = append(entries, e)
	}

	src := &LeafRecord{Entries: entries}

	data, err := proto.Marshal(src)
	if err != nil {
		panic(fmt.Sprintf("Couldn't marshal metadata object: %s", err))
	}
	return data

}

func (d *LeafDir) readLeaf(key *Key) *Leaf {
	if *key == *EMPTY_DIR_KEY {
		return &EMPTY_DIR
	} else {
		resource := d.chunks.Get(key)
		return UnpackLeaf(resource.AsBytes())
	}
}

func writeLeaf(chunks ChunkService, leaf *Leaf) *Key {
	buffer := PackLeaf(leaf)
	newLeafKey := computeContentKey(buffer)
	chunks.Put(newLeafKey, NewMemResource(buffer))
	return newLeafKey
}

func (d *LeafDir) writeLeaf(leaf *Leaf) *Key {
	return writeLeaf(d.chunks, leaf)
}

func computeContentKey(buffer []byte) *Key {
	key := Key(sha256.Sum256(buffer))
	return &key
}

func (d *LeafDir) Get(name string) *FileMetadata {
	leaf := d.readLeaf(d.key)
	return leaf.get(name)
}

// create a leaf which only contains the specified metadata and the filenames do not matter
// this is used to create a set of references which are used in the transient refs.
func CreateAnonymousRefLeaf(chunks ChunkService, metadatas []*FileMetadata) *Key {
	leaf := &EMPTY_DIR
	for i, meta := range metadatas {
		leaf = leaf.insert(&LeafEntry{name: strconv.Itoa(i), metadata: meta})
	}
	return writeLeaf(chunks, leaf)
}

func (d *LeafDir) Put(name string, metadata *FileMetadata) *Key {
	leaf := d.readLeaf(d.key)
	if metadata == nil {
		panic(fmt.Sprintf(">>>> metadata = %s\n", metadata))
	}
	newLeaf := leaf.insert(&LeafEntry{name: name, metadata: metadata})
	return d.writeLeaf(newLeaf)
}

func (d *LeafDir) Remove(name string) *Key {
	leaf := d.readLeaf(d.key)
	newLeaf := leaf.remove(name)
	if newLeaf == nil {
		return d.key
	} else {
		return d.writeLeaf(newLeaf)
	}
}

type LeafIterator struct {
	leafIndex  int
	leaf       *Leaf
	reachedEnd bool
}

func (it *LeafIterator) HasNext() bool {
	return !it.reachedEnd
}

func (it *LeafIterator) Next() (string, *FileMetadata) {
	next := it.leaf.entries[it.leafIndex]

	it.leafIndex++

	if it.leafIndex >= len(it.leaf.entries) {
		it.reachedEnd = true
	}

	return next.name, next.metadata
}

func (d *LeafDir) Iterate() Iterator {
	leaf := d.readLeaf(d.key)
	return &LeafIterator{leafIndex: 0, leaf: leaf, reachedEnd: len(leaf.entries) == 0}
}

/*
func GetLeaf(key *Key) *Leaf {
	panic();
}

func GetBranch(key *Key) *Branch {
	panic();
}

func copyLeaf(leaf *Leaf) *Leaf {
	panic();
}

func persistLeaf(leaf *Leaf) *Key {
	panic();
}


type BTreeService interface {
	// root might key of either a Branch or Leaf
	Insert(root *Key, name string, metadata *FileMetadata) *Key;
	Delete(root *Key, name string) *Key;
	Get(root *Key, name string) *FileMetadata;
	// what is the best interface to provide for walking through entries?  An iterator or a callback?
	// Probably an iterator because an iterator can always be transformed into a callback
	Iterate(root *Key) *Iterator;
}


func NewNamespace(store *NodeStore) {
	panic()
}



// test insertion
// root1 := ns.Insert(root0, name, metadata)
// it := ns.Iterator(root1)
// assert it.HasNext()
// entry := it.Next()
// assert !it.HasNext()

// root2 := ns.Insert(root1, name, metadata)
// it := ns.Iterator(root1)
// assert it.HasNext()
// entry := it.Next()
// assert it.HasNext()
// entry := it.Next()

type Iterator interface {
	HasNext() bool;
	Next() *LeafEntry;
}

type iterator struct {
	store *NodeStore;
	leafIndex int;
	leaf *Leaf;
	branchIndex int;
	branch *Branch;
	reachedEnd bool;
}

func (it *iterator) HasNext() bool{
	return !it.reachedEnd;
}

func (it *iterator) Next() *LeafEntry {
	next := it.leaf.entries[it.leafIndex];

	it.leafIndex ++;

	if it.leafIndex >= len(it.leaf.entries) {
		if it.branch == nil {
			it.reachedEnd = true;
		} else {
			it.branchIndex ++;
		    if it.branchIndex >= len(it.branch.children) {
				it.reachedEnd = true;
			} else {
				it.leafIndex = 0;
				entry := it.branch.children[it.branchIndex];
				if !entry.isLeaf {
					panic("expected leaf");
				}
				it.leaf = it.store.GetLeaf(entry.child);
			}
		}
	}

	return next;
}

func insertIntoLeaf(leaf *Leaf, name string, metadata *FileMetadata, stats *TreeStats, MaxBlockSize int) []Key {
	i := sort.Search(len(leaf.entries), func(i int) bool {
			return leaf.entries[i].name >= name;
		});

	// replace existing entry
	var newLeaf *Leaf;
	if(leaf.entries[i].name == name) {
		newLeaf = CopyLeafWithMutation(leaf, i, metadata);
		stats.valuesReplaced ++;
	} else {
		// otherwise we have an insertion
		newLeaf = CopyLeafWithInsertion(leaf, i, name, metadata);
		stats.valuesInserted ++;
	}

	newLeaves := splitLeafIfTooLarge(newLeaf, MaxBlockSize);
	newLeafKeys := make([]Key, len(newLeaves));
	for i, l := range(newLeaves) {
		k := persistLeaf(l);
		newLeafKeys[i] = k;
	}

	return newLeafKeys;
}

func splitLeafIfTooLarge(leaf *Leaf, maxBlockSize int) []*Leaf {
	if(LeafSize(leaf) > maxBlockSize) {
		// this block has gotten too big, so split it in half
		leafA, leafB := SplitLeaf(leaf)
		return &Leaf{leafA, leafB};
	} else {
		newLeafKey := persistLeaf(leaf);
		return BranchEntry{isLeaf: true, key: newLeafKey}
	}
}

func findLeafContaining(root *Key, name string) ([]*Branch, []int) {
	branch := GetBranch(root)

	i := sort.Search(len(branch.entries), func(i int) bool {
			return branch.entries[i].lastName >= name;
		});
}

func Insert(root *Key, name string, metadata *FileMetadata, MaxBlockSize int, stats *TreeStats) Key {
	if(root.isLeaf) {
		leaf := GetLeaf(root.key)
		keys := insertIntoLeaf(leaf, name, metadata, stats, MaxBlockSize);
		if len(keys) > 1 {
			newBranch := NewBranch(keys)
			return PersistBranch(newBranch)
		} else {
			return keys[0];
		}
	} else {
		// we've got a branch, so first identify the path to the impacted leaf
		var path [] *Branch;
		var pathIndex[] int;
		var i int;
		var newPath[] *Branch;

		// perform the insert in the leaf (todo)
		// somehow find the actual leaf
		path, pathIndex = findLeafContaining(root, name)

		keys := insertIntoLeaf(leaf, name, metadata, stats, MaxBlockSize);
		if len(keys) == 1 {
			// the easy case: we get one leaf back, so we just need to update the existing entry in the
			newPath[i] = CopyBranchWithMutation(path[i], pathIndex[i], newEntry);
			newBranch := NewBranch(keys)
			// merge branch into parent
			return PersistBranch(newBranch)
		} else {
			// we have multiple leaves which need to merge in.  This might result in a cascading split of branches
			// todo: implement splitting of branches
			newPath[i] = CopyBranchWithMerge(path[i], pathIndex[i], entry1, entry2);
			return keys[0];
		}

		newEntry := Insert(path[i].children[pathIndex[i]], name, metadata, MaxBlockSize);

		var newPath[] *Branch;

		if(!newEntry.isLeaf) {
			// if this entry is actual a branch a split occurred and we need to merge it into the parent branch
			if(BranchSize(path[i]) > MaxBlockSize) {
				// the merge will be too big, so we need to split
				newBranch1, newBranch2 := SplitBranchAndMerge(path[i], entry1, entry2)
			} else {
				// the merge can safely be done by simple insertion
				newPath[i] = CopyBranchWithMerge(path[i], pathIndex[i], entry1, entry2);
			}
		} else {
			// otherwise, this entry is a leaf and we can just replace the element in the parent branch
		}

		return BranchEntry{isLeaf: false, key: newBranchKey}
	}
}

*/
