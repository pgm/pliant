package deep

/*
import "github.com/pgm/pliant/v2"

type Entry struct {
	name string
	key *v2.Key // populated for branch nodes
	metadata *v2.FileMetadata // populated for leaf nodes
}

type Node struct {
	height int
	entries []*Entry
}

func searchLeaf(node *Node, name string) *v2.FileMetadata {
	for i := range(node.entries) {
		if name == node.entries[i].name {
			return node.entries[i].metadata
		}
	}
	return nil
}

func findLeaf(node *Node, name string) *v2.Key {
	for i := range(node.entries) {
		if i+1 >= len(node.entries) || name < node.entries[i+1].name {
			if node.height == 1 {
				return node.entries[i]
			} else {
				return findLeaf(node.entries[i].node, name)
			}
		}
	}
	return nil
}

func search(node *Node, name string) *v2.FileMetadata {
	if node.height > 0 {
		// this is an branch node
		node = findLeaf(node, name)
	}

	return searchLeaf(node, name)
}

func insert(node *Node, name string, key *v2.FileMetadata) {
//	if node.height == 0 {
//		// if leaf node
//		for i := range(node.entries) {
//			if name < node.entries[i].name {
//				break
//			}
//	} else {
//		// if branch
//		for i := range(node.entries) {
//			if i+1 == len(node.entries) || name < node.entries[i+1].name {
//				parent := insert(node.entries[i].blah, name, value)
//				if parent == nil {
//					return nil
//				}
//			}
//			}
//		}
//	}
}

func remove(node *Node, name string) {

}
*/
