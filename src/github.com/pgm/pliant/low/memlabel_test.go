package low

import (
//	"testing"
	. "gopkg.in/check.v1"
)

//type MySuite struct{}

type MemlabelSuite struct{}
var _ = Suite(&MemlabelSuite{})


func assertHasLabel(c *C, svc LabelService, name string, expectation bool) {
	flag, err := svc.HasLabel(name)
	c.Assert(err, Equals, nil)
	c.Assert(flag, Equals, expectation)
}

func (s *MemlabelSuite) TestAddRemoveLabel (c *C) {
	svc := NewMemLabelService()
	assertHasLabel(c, svc, "a", false)
	svc.UpdateLabel("a", ChunkID("x"))
	assertHasLabel(c, svc, "a", true)
	svc.RemoveLabel("a")
}


func (s *MemlabelSuite) TestVisitEachLabel (c *C) {
	svc := NewMemLabelService()
	svc.UpdateLabel("a", ChunkID("x"))
	callCount := 0
	t := func(Label string, Chunk ChunkID) {
		c.Assert(Label, Equals, "a")
		c.Assert(Chunk, Equals, ChunkID("x"))
		callCount++;
	}
	svc.VisitEach(t)
	c.Assert(callCount, Equals, 1)
}
