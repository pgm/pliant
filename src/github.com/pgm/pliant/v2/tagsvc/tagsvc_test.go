package tagsvc

import (
	"fmt"
	. "gopkg.in/check.v1"
	"testing"
	"github.com/pgm/pliant/v2"
	"github.com/golang/protobuf/proto"

)

type TagSvcSuite struct{}

var _ = Suite(&TagSvcSuite{})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }

func (s *TagSvcSuite) TestLeases(c *C) {
	key1 := v2.Key{1};
	key2 := v2.Key{2};
	key3 := v2.Key{3};

	root := NewRoots()
	c.Assert(len(root.GetRoots()), Equals, 0)
	root.AddLease(100, &key1)
	c.Assert(len(root.GetRoots()), Equals, 1)
	root.AddLease(101, &key2)
	c.Assert(len(root.GetRoots()), Equals, 2)
	root.AddLease(102, &key3)

	c.Assert(len(root.GetRoots()), Equals, 3)

	root.Expire(100)
	c.Assert(len(root.GetRoots()), Equals, 3)
	root.Expire(101)
	c.Assert(len(root.GetRoots()), Equals, 2)
	root.Expire(103)
	c.Assert(len(root.GetRoots()), Equals, 0)
}

func (s *TagSvcSuite) TestSetRoot(c *C) {
	key1 := v2.Key{1};
	key2 := v2.Key{2};
	key3 := v2.Key{3};

	root := NewRoots()
	root.Set("1", &key1)
	root.Set("2", &key2)
	c.Assert(len(root.GetRoots()), Equals, 2)

	root.Set("2", &key3)
	c.Assert(len(root.GetRoots()), Equals, 2)

	root.Set("1", nil)
	c.Assert(len(root.GetRoots()), Equals, 1)
}

func (s *TagSvcSuite) TestSimpleGC(c *C) {
	fileKey1 := v2.Key{10};
	fileKey2 := v2.Key{11};
	fileKey3 := v2.Key{12};

	root := NewRoots()
	count := 0
	countPtr := &count
	chunks := v2.NewMemChunkService()
	chunks.Put(&fileKey1, v2.NewMemResource(make([]byte, 1)))
	chunks.Put(&fileKey2, v2.NewMemResource(make([]byte, 1)))
	chunks.Put(&fileKey3, v2.NewMemResource(make([]byte, 1)))
	dirService := v2.NewLeafDirService(chunks)
	dir := dirService.GetDirectory(v2.EMPTY_DIR_KEY)
	dirKey := dir.Put("a", &v2.FileMetadata{Length: proto.Int64(1), Key: fileKey1.AsBytes(), IsDir: proto.Bool(false)})
	root.Set("1", dirKey)

	fmt.Printf("GC\n")
	root.GC(dirService, chunks, func(key *v2.Key) {
			fmt.Printf("free %s\n", key.String())
			*countPtr += 1
		})

	c.Assert(*countPtr, Equals, 2)
}
