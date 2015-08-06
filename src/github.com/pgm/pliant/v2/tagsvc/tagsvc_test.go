package tagsvc

import (
	"fmt"
	. "gopkg.in/check.v1"
	"testing"
	"github.com/pgm/pliant/v2"
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
