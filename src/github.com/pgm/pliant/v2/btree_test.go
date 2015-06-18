package v2

import (
. "gopkg.in/check.v1"
  "fmt"
	"testing"

)

type BtreeSuite struct{}
var _ = Suite(&BtreeSuite{})
var _ = fmt.Sprintf("hello!")
func Test(t *testing.T) { TestingT(t) }

func fetchNames(d Directory) []string {
	names := make([]string, 0);
	it := d.Iterate()
	for it.HasNext() {
		name, _ := it.Next()
		names = append(names, name)
	}
	return names
}

func (s *BtreeSuite) TestBtreeInserts(c *C) {
	chunks := NewMemChunkService()
	ds := NewLeafDirService(chunks)
	d0 := ds.GetDirectory(EMPTY_DIR_KEY)

	metadata := &FileMetadata{};
	key1 := d0.Put("z", metadata)
	d1 := ds.GetDirectory(key1)
	key2 := d1.Put("y", metadata)
	d2 := ds.GetDirectory(key2)
	key3 := d2.Put("t", metadata)
	d3 := ds.GetDirectory(key3)
	key4 := d3.Put("x", metadata)
	d4 := ds.GetDirectory(key4)

	e1 := [...]string{"t","x","y","z"};
	c.Assert(fetchNames(d4), DeepEquals, e1[:])

	key5 := d4.Remove("t")
	d5 := ds.GetDirectory(key5)
	key6 := d5.Remove("y")
	d6 := ds.GetDirectory(key6)
	key7 := d6.Remove("z")
	d7 := ds.GetDirectory(key7)

	e2 := [...]string{"x"}
	c.Assert(fetchNames(d7), DeepEquals, e2[:])
}

func (s *BtreeSuite) TestBtreeDirService (c *C) {
	chunks := NewMemChunkService()
	ds := NewLeafDirService(chunks)
	d0 := ds.GetDirectory(EMPTY_DIR_KEY)

	// has no entries
	it := d0.Iterate()
	c.Assert(it.HasNext(), Equals, false)

	metadata := &FileMetadata{};
	key1 := d0.Put("x", metadata)

	// now we have a new directory with one entry
	d1 := ds.GetDirectory(key1)

	// make sure iterator works properly
	it1 := d1.Iterate()
	c.Assert(it1.HasNext(), Equals, true)
	n1, _ := it1.Next()
	c.Assert(n1, Equals, "x")
	c.Assert(it1.HasNext(), Equals, false)

	// now make sure remove basically works
	key2 := d1.Remove("x")

	d2 := ds.GetDirectory(key2)

	// back down to no entries
	it2 := d2.Iterate()
	c.Assert(it2.HasNext(), Equals, false)
}
