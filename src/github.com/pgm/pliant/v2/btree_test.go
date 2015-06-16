package v2

import (
. "gopkg.in/check.v1"
"fmt"
)

type BtreeSuite struct{}
var _ = Suite(&BtreeSuite{})
var _ = fmt.Sprintf("hello!")

func (s *BtreeSuite) TestBtreeDirService () {
	chunks := NewMemChunkService()
	ds := NewLeafDirService(chunks)
	d0 := ds.GetDirectory(EMPTY_DIR_KEY)

	// has no entries
	it := d0.Iterate()
	s.Assert(it.HasNext(), Equals, false)

	metadata := &FileMetadata{};
	key1 := d0.Put("x", metadata)

	// now we have a new directory with one entry
	d1 := ds.GetDirectory(key1)

	// make sure iterator works properly
	it := d1.Iterate()
	s.Assert(it.HasNext(), Equals, true)
	n1, _ := it.Next()
	s.Assert(n1, Equals, "x")
	s.Assert(it.HasNext(), Equals, false)

	// now make sure remove basically works
	key2 := d0.Remove("x")

	d2 := ds.GetDirectory(key2)

	// back down to no entries
	it := d2.Iterate()
	s.Assert(it.HasNext(), Equals, false)
}
