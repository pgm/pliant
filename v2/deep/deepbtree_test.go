package deep

import (
	"fmt"
	"testing"

	"github.com/pgm/pliant/v2"

	. "gopkg.in/check.v1"
)

type BtreeSuite struct {
	counter int
}

var _ = Suite(&BtreeSuite{})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }

func (s *BtreeSuite) makeMetadata() *v2.FileMetadata {
	s.counter++
	return &v2.FileMetadata{Key: []byte{byte(s.counter)}}
}

type MemStore struct {
	counter int
	m       map[v2.Key]*Node
}

func NewMemStore() *MemStore {
	return &MemStore{m: make(map[v2.Key]*Node), counter: 1}
}

func (m *MemStore) Get(key *v2.Key) *Node {
	return m.m[*key]
}

func (m *MemStore) Store(node *Node) *v2.Key {
	m.counter++
	key := v2.Key{10, byte(m.counter)}
	m.m[key] = node
	return &key
}

func verifyTreeIsValid(c *C, tree *Btree) {
	verifyNodeIsValid(c, tree, tree.root, false)
}

func verifyNodeIsValid(c *C, tree *Btree, node *Node, checkMin bool, left string) {
	if checkMin && len(node.entries) < tree.minSize {
		c.Fatalf("node had %d entries but minSize=%d", len(node.entries), tree.minSize)
	}

	if len(node.entries) > tree.minSize*2 {
		c.Fatalf("node had %d entries but maxSize=%d", len(node.entries), tree.minSize*2)
	}

	if !(len(node.children) == 0 || len(node.children) == len(node.entries)+1) {
		c.Fatalf("had %d children but node had %d entries", len(node.children), len(node.entries))
	}

	for _, child := range node.children {
		childNode := tree.store.Get(child)
		verifyNodeIsValid(c, tree, childNode, true)
	}
}

func (s *BtreeSuite) TestBranchingInserts(c *C) {
	tree := NewBtree(NewMemStore(), 1)
	// m := make([]*v2.FileMetadata, 3)

	for i := 0; i < 3; i++ {
		m := s.makeMetadata()
		name := fmt.Sprintf("%d", i)
		tree, _ = tree.Insert(name, m)
		c.Assert(tree.Get(name), Equals, m)
		verifyTreeIsValid(c, tree)
	}

}

func (s *BtreeSuite) TestNonbranchingInserts(c *C) {
	tree := NewBtree(NewMemStore(), 10)
	m1 := s.makeMetadata()

	c.Assert(tree.Get("1"), IsNil)
	tree1, orig1 := tree.Insert("1", m1)
	c.Assert(tree1.Get("1"), Equals, m1)
	c.Assert(orig1, IsNil)

	m2 := s.makeMetadata()
	tree2, orig2 := tree1.Insert("2", m2)
	c.Assert(tree.Get("1"), IsNil)
	c.Assert(tree2.Get("2"), Equals, m2)
	c.Assert(orig2, IsNil)

	m3 := s.makeMetadata()
	tree3, orig3 := tree2.Insert("2", m3)
	c.Assert(tree2.Get("2"), Equals, m2)
	c.Assert(tree3.Get("2"), Equals, m3)
	c.Assert(orig3, Equals, m2)

	// tree3 := tree2.remove(root2, "2")
	// c.Assert(search(root3, "1"), Equals, m1)
	// c.Assert(search(root3, "2"), IsNil)
	//
	// root4 := remove(root3, "1")
	// c.Assert(search(root4, "1"), IsNil)
}
