package low

import (
	"testing"
	"bytes"
	. "gopkg.in/check.v1"
	"io"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MemChunkSuite struct{}

var _ = Suite(&MemChunkSuite{})

/*
func (s *MySuite) TestHelloWorld(c *C) {
	c.Assert(42, Equals, "42")
	c.Assert(io.ErrClosedPipe, ErrorMatches, "io: .*on closed pipe")
	c.Check(42, Equals, 42)
}
*/

func assertHasChunk(c *C, svc ChunkService, name ChunkID, expectation bool) {
	flag, err := svc.HasChunk(name)
	c.Assert(err, Equals, nil)
	c.Assert(flag, Equals, expectation)
}

func (s *MemChunkSuite) TestAddRemove (c *C) {
	svc := NewMemChunkService()
	assertHasChunk(c, svc, "a", false)
	data := [...]byte{1, 2, 3}
	metadata, _ := svc.Create("a", bytes.NewBuffer(data[:]))
	c.Assert(metadata.GetLength(), Equals, int64(3))
	assertHasChunk(c, svc, "a", true)

	var err error
	var reader io.Reader

	reader, metadata, err = svc.Read("a", 0, 3)
	c.Assert(metadata.GetLength(), Equals, int64(3))
	c.Assert(reader, NotNil)
	c.Assert(err, IsNil)
	//c.Assert(data, Equals, [...]byte{1, 2, 3}[:])

	reader, _, err = svc.Read("a", 0, 1)
	c.Assert(reader, NotNil)
	c.Assert(err, IsNil)
	//c.Assert(data, Equals, [...]byte{0}[:])

	reader, _, err = svc.Read("a", 2, 1)
	c.Assert(reader, NotNil)
	c.Assert(err, IsNil)
	//c.Assert(data, Equals, [...]byte{3}[:])

	svc.Free("a")
	assertHasChunk(c, svc, "a", false)
}


func (s *MemChunkSuite) TestVisitEach (c *C) {
	svc := NewMemChunkService()
	data := [...]byte{1, 2, 3}
	svc.Create("a", bytes.NewBuffer(data[:]))
	callCount := [...]int{0}
	t := func(Chunk ChunkID) {
		c.Assert(Chunk, Equals, ChunkID("a"))
		callCount[0]++;
	}
	svc.VisitEach(t)
	c.Assert(callCount[0], Equals, 1)
}
