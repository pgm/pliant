package deep

import "github.com/pgm/pliant/v2"

type Store interface {
	Get(key *v2.Key) *Node
	Store(node *Node) *v2.Key
}

type Entry struct {
	name     string
	metadata *v2.FileMetadata
}

type Node struct {
	entries  []*Entry
	children []*v2.Key
}

type Btree struct {
	store   Store
	minSize int
	root    *Node
}

func NewBtree(store Store, minSize int) *Btree {
	return &Btree{store: store, minSize: minSize, root: &Node{entries: nil, children: nil}}
}

// returns the index of the first entry which is greater than or equal the name provided
// if no such entry then an index past the end is returned
func (node *Node) indexOf(name string) (int, bool) {
	//	fmt.Printf("indexOf\n")
	for i := range node.entries {
		//		fmt.Printf("indexOf node.entries[%d].name=%s == name=%s\n", i, node.entries[i].name, name)
		if node.entries[i].name == name {
			return i, true
		} else if node.entries[i].name > name {
			return i, false
		}
	}

	return len(node.entries), false
}

func (b *Btree) Get(name string) *v2.FileMetadata {
	return b.get(b.root, name)
}

func (b *Btree) Insert(name string, metadata *v2.FileMetadata) (*Btree, *v2.FileMetadata) {
	newRoot, orig := b.insert(b.root, name, metadata)
	return &Btree{root: newRoot, minSize: b.minSize, store: b.store}, orig
}

func (b *Btree) insert(node *Node, name string, metadata *v2.FileMetadata) (*Node, *v2.FileMetadata) {
	i, perfectMatch := node.indexOf(name)
	//	fmt.Printf("insert(name=%s): i=%s, perfect=%s\n", name, i, perfectMatch)
	if perfectMatch {
		return cloneWithMutation(node, i, &Entry{name: name, metadata: metadata})
	}

	if node.isInternal() {
		childNode := b.store.Get(node.children[i])

		// check child size
		if len(childNode.entries) >= b.minSize*2 {
			node, i = split(b, node, i, name)
		}

		return b.insert(childNode, name, metadata)
	} else {
		return cloneWithInsertion(node, i, &Entry{name: name, metadata: metadata}), nil
	}
}

func split(b *Btree, node *Node, childIndex int, name string) (*Node, int) {
	childNode := b.store.Get(node.children[childIndex])
	i := len(childNode.entries) / 2

	left := &Node{entries: childNode.entries[:i], children: childNode.children[:i]}
	median := childNode.entries[i]
	right := &Node{entries: childNode.entries[i+1:], children: childNode.children[i+1:]}

	newChildren := make([]*v2.Key, 0, len(node.children))
	newEntries := make([]*Entry, 0, len(node.entries))

	newEntries = append(newEntries, node.entries[:i]...)
	newEntries = append(newEntries, median)
	newEntries = append(newEntries, node.entries[i:]...)

	newChildren = append(newChildren, node.children[:i-1]...)
	newChildren = append(newChildren, b.store.Store(left))
	newChildren = append(newChildren, b.store.Store(right))
	newChildren = append(newChildren, node.children[:i+1]...)

	if median.name < name {
		i++
	}

	return &Node{entries: newEntries, children: newChildren}, i
}

func cloneWithInsertion(node *Node, i int, entry *Entry) *Node {
	copiedEntries := make([]*Entry, 0, len(node.entries)+1)
	copiedEntries = append(node.entries[:i], entry)
	copiedEntries = append(copiedEntries, node.entries[i:]...)

	//	fmt.Printf("i=%s, entry=%s\n", i, entry)

	return &Node{entries: copiedEntries, children: node.children}
}

func cloneWithMutation(node *Node, i int, entry *Entry) (*Node, *v2.FileMetadata) {
	copiedEntries := make([]*Entry, len(node.entries))
	copy(copiedEntries, node.entries)
	original := copiedEntries[i].metadata
	copiedEntries[i] = entry

	return &Node{entries: copiedEntries, children: node.children}, original
}

func (b *Btree) get(node *Node, name string) *v2.FileMetadata {
	i, perfectMatch := node.indexOf(name)
	if perfectMatch {
		return node.entries[i].metadata
	}

	if node.isInternal() {
		childNode := b.store.Get(node.children[i])
		return b.get(childNode, name)
	} else {
		return nil
	}
}

func (node *Node) isInternal() bool {
	return node.children != nil
}

/*
func findInsertionPoint(node *Node, name string) int {
	return len(node.entries)
}

func findExactMatch(node *Node, name string) *Entry {
	i := findInsertionPoint(node, name)
	if i == len(node.entries) || node.entries[i].name != name {
		return nil
	} else {
		return &node.entries[i]
	}
}

func lookupNode(key *v2.Key) *Node {
	return nil
}

type NodeIndex struct {
	node *Node
	index int
}


func find(store *Store, node *Node, name string) *v2.FileMetadata {
	if node.height == 0 {
		entry := findExactMatch(node, name)
		if entry == nil {
			return entry
		} else {
			return entry.metadata
		}
	} else {
		// TODO: I think this is wrong
		i := findInsertionPoint(node, name)
		child := store.Get(node.entries[i])
		return find(store, child, name)
	}
}

func insert(store *Store, node *Node, name string, metadata *v2.FileMetadata) *v2.Key {
		if node.height == 0 {
			return copyWithMutation(store, node, name, metadata *v2.FileMetadata)
		} else {
			// TODO: I think this is wrong
			i := findInsertionPoint(node, name)
			if i < 0 {
					// update the first node, which requires adjusting left bound

			} else {
				child := store.Get(node.entries[i])
				rewrittenChild := insert(store, child, name, metadata)
				return copyWithMutation(store, node, node.entries[i].name, rewrittenChild)
			}
		}
}

func remove(store *Store, node *Node, name string) *v2.Key {
	if node.height == 0 {
		return copyWithRemoval(store, node, name)
	} else {
		// TODO: I think this is wrong
		i := findInsertionPoint(node, name)
		child := store.Get(node.entries[i])
		rewrittenChild := remove(store, child, name)
		return copyWithMutation(store, node, node.entries[i].name, rewrittenChild)
	}
}



// returns pointer to leaf which needs to be updated
func findLeaf(node *Node, name string) []NodeIndex {
	if node.height == 0 {
			// find insertion point
			return node
	}

	for i := 0; i < len(node.entries)-1 ; i++ {
		if name < node.entries[i+1].name {
			child := lookupNode(node.entries[i].key)
		 	return findLeaf(child)
		}
	}

	return findLeaf(node.entries[len(node.entries)-1]), len(node.entries)
}

func search(node *Node, name string) *v2.FileMetadata {
	if node.height > 0 {
		// this is an branch node
		node_key := findLeaf(node, name)
		node = lookupNode(node_key)
	}

	return searchLeaf(node, name)
}

func insert(node *Node, name string, metadata *v2.FileMetadata) *Node {
	leaf, index := findLeaf(node, name)

	newEntries := make([]*Entries, len(leaf.entries+1))
	for src := 0, dst:=0 ; src < len(leaf.entries) ; src++ {
		if src == 0 {
			newEntries[dst] = &Entry{name: name, key: v2.KeyFromBytes(metadata.GetKey()),  metadata}
			dst ++;
		}
		newEntries[dst] = leaf.entries[src]
		dst++
		src++
	}

	// if we're inserting the smallest key, we also need to update the parent's entry
	if index == 0 {

	}

	return leaf
}

func remove(node *Node, name string) *Node {
	return nil
}
*/
