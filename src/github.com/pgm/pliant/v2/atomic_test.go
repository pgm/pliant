package v2

import (
	. "gopkg.in/check.v1"
	"fmt"
//	"testing"

	"os"
)

type AtomicSuite struct{}
var _ = Suite(&AtomicSuite{})
var _ = fmt.Sprintf("hello!")

func fetchNamesFromIter(it Iterator) []string {
	names := make([]string, 0);
	for it.HasNext() {
		name, _ := it.Next()
		names = append(names, name)
	}
	return names
}

func (s *AtomicSuite) TestAtomicDirOps(c *C) {
	chunks := NewMemChunkService()
	ds := NewLeafDirService(chunks)
	as := NewAtomicState(ds)
	ac := &AtomicClient{atomic: as}

	ac.Link(EMPTY_DIR_KEY.String(), "a", true)
	it0, _ := as.GetDirectoryIterator(NewPath(""))

	e1 := [...]string{"a"};
	c.Assert(fetchNamesFromIter(it0), DeepEquals, e1[:])

	ac.Link(EMPTY_DIR_KEY.String(), "b", true)
	it1, _ := as.GetDirectoryIterator(NewPath(""))

	e2 := [...]string{"a", "b"};
	c.Assert(fetchNamesFromIter(it1), DeepEquals, e2[:])

	ac.Link(EMPTY_DIR_KEY.String(), "a/b", true)
	ac.Link(EMPTY_DIR_KEY.String(), "a/b/c", true)
	it2, _ := as.GetDirectoryIterator(NewPath("a"))

	e3 := [...]string{"b"};
	c.Assert(fetchNamesFromIter(it2), DeepEquals, e3[:])
}

func (s *AtomicSuite) TestAtomicFileOps(c *C) {
	chunks := NewMemChunkService()
	ds := NewLeafDirService(chunks)
	as := NewAtomicState(ds)
	ac := &AtomicClient{atomic: as}

	tempFile := "tmpfile"
	wfile, _ := os.Create(tempFile)
	wfile.WriteString("test")
	wfile.Close()

	ac.Link(EMPTY_DIR_KEY.String(), "a", true)
	key := ac.PutLocalPath(tempFile)
	ac.Link(key, "a/b", false)
	finalFile := ac.GetLocalPath("a/b")

	file, _ := os.Open(finalFile)
	b := make([]byte, 4)
	n, _ := file.Read(b)
	c.Assert(n, Equals, 4);
	c.Assert("test", Equals, string(b))
}
