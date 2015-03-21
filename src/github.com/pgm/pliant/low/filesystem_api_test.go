package low

import (
	"fmt"
	. "gopkg.in/check.v1"
	"bytes"
)


type FilesystemApiSuite struct{}
var _ = Suite(&FilesystemApiSuite{})
var _ = fmt.Sprintf("hello!")

func makeFilesystem2() *Filesystem{
	chunks := NewMemChunkService()
	labels := NewMemLabelService()

	rawFs := NewRawFilesystem(chunks)
	fs := NewFilesystem(labels, rawFs)
	return &fs
}

func (s *FilesystemApiSuite) TestMakeFiles (c *C) {
	fs := makeFilesystem2()
	buffer := make([]byte, 1)

	c.Assert(fs.LabelEmptyDir("x"), Equals, nil)
	c.Assert(fs.WriteFile("x", "file", bytes.NewBufferString("a")), Equals, nil)
	_, err := fs.ReadFile("x", "file", 0, buffer)
	c.Assert(err, Equals, nil)

	dir, readDirErr := fs.ReadDir("x", ".")
	c.Assert(readDirErr, IsNil)
	c.Assert(dir, Not(IsNil))
	c.Assert(len(dir.GetEntries()), Equals, 1)
}

func (s *FilesystemApiSuite) TestUnlink (c *C) {
	fs := makeFilesystem2()

	c.Assert(fs.LabelEmptyDir("x"), Equals, nil)
	c.Assert(fs.WriteFile("x", "file", bytes.NewBufferString("a")), Equals, nil)

	dir, readDirErr := fs.ReadDir("x", ".")
	c.Assert(readDirErr, IsNil)
	c.Assert(dir, Not(IsNil))
	c.Assert(len(dir.GetEntries()), Equals, 1)

	c.Assert(fs.Unlink("x", "file"), Equals, nil)
	dir, readDirErr = fs.ReadDir("x", ".")
	c.Assert(readDirErr, IsNil)
	c.Assert(dir, Not(IsNil))
//	fmt.Printf("entry name=%s\n", dir.GetEntries()[0].GetName())
	c.Assert(len(dir.GetEntries()), Equals, 0)
}

func (s *FilesystemApiSuite) TestMakeDirs (c *C) {
	fs := makeFilesystem2()
	buffer := make([]byte, 1)

	c.Assert(fs.LabelEmptyDir("x"), Equals, nil)

	c.Assert(fs.MakeDir("x", "y"), Equals, nil)
	c.Assert(fs.WriteFile("x", "y/file", bytes.NewBufferString("a")), Equals, nil)
	_, err := fs.ReadFile("x", "y/file", 0, buffer)
	c.Assert(err, Equals, nil)
	c.Assert(buffer[0], Equals, uint8('a'))

	// check readdir
	dir, readDirErr := fs.ReadDir("x", "y")
	c.Assert(readDirErr, IsNil)
	c.Assert(dir, Not(IsNil))
	c.Assert(len(dir.GetEntries()), Equals, 1)

	// create nested dir
	c.Assert(fs.MakeDir("x", "y"), Not(Equals), nil)
	c.Assert(fs.MakeDir("x", "y/z"), Equals, nil)
	c.Assert(fs.WriteFile("x", "y/z/file", bytes.NewBufferString("b")), Equals, nil)
	_, err = fs.ReadFile("x", "y/z/file", 0, buffer)
	c.Assert(err, Equals, nil)
	c.Assert(buffer[0], Equals, uint8('b'))

	// check readdir on nested dir
	dir, readDirErr = fs.ReadDir("x", "y/z")
	c.Assert(readDirErr, IsNil)
	c.Assert(dir, Not(IsNil))
	c.Assert(len(dir.GetEntries()), Equals, 1)
}

