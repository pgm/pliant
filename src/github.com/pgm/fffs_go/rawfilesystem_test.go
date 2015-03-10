package fffs_go

import (
	"bytes"
	. "gopkg.in/check.v1"
	"fmt"
)

type RawFilesystemSuite struct{}
var _ = Suite(&RawFilesystemSuite{})
var _ = fmt.Sprintf("hello!")

func makeFilesystem() *RawFilesystem{
	chunks := NewMemChunkService()
	metadata := NewMemChunkService()

	return NewRawFilesystem(chunks, metadata)
}

func (s *RawFilesystemSuite) TestRawFilesystemDirs (c *C) {
	fs := makeFilesystem()

	entry := NewDirEntry("child", EMPTY_DIR_ID, DIR_TYPE)
	entries := []*DirEntry{entry}

	parentDirId, _ := fs.NewDir(&Dir{Entries: entries })

	dir,_:= fs.ReadDir(parentDirId)
	c.Assert(len(dir.GetEntries()), Equals, 1)
	c.Assert(dir.Get("child"), Not(IsNil))

	dir,_ = fs.ReadDir(EMPTY_DIR_ID)
	c.Assert(len(dir.GetEntries()), Equals, 0)
}

func (s *RawFilesystemSuite) TestRawFilesystemFiles (c *C) {
	fs := makeFilesystem()

	fileId,_ := fs.NewFile(bytes.NewBufferString("x"))

	metadata,_ := fs.GetFileMetadata(fileId)
	c.Assert(metadata.GetLength(), Equals, int64(1))

	buffer := make([]byte, 1, 1)
	fs.ReadFile(fileId, 0, 1, buffer)
	c.Assert(buffer[0], Equals, uint8('x'))
}

func (s *RawFilesystemSuite) TestCloneWithReplacement (c *C) {
	fs := makeFilesystem()

	file1Id,_ := fs.NewFile(bytes.NewBufferString("1"))
	file2Id,_ := fs.NewFile(bytes.NewBufferString("2"))

	parentDirId,_ := fs.NewDir(&Dir{Entries:make([]*DirEntry, 0)})

	newRoot1,_ := fs.recursiveCloneDirWithReplacement(parentDirId, "a", NewDirEntry("a", file1Id, FILE_TYPE), true)
	c.Assert(newRoot1, Not(Equals), INVALID_ID)

	newRoot2,_ := fs.recursiveCloneDirWithReplacement(newRoot1, "a", NewDirEntry("a", file2Id, FILE_TYPE), true)
	c.Assert(newRoot2, Not(Equals), INVALID_ID)

	c.Assert(fs.FileExists(parentDirId, "a"), Equals, false)
	c.Assert(fs.FileExists(newRoot1, "a"), Equals, true)
	c.Assert(fs.FileExists(newRoot2, "a"), Equals, true)
}

func (s *RawFilesystemSuite) TestCloneWithNestedReplacement (c *C) {
	fs := makeFilesystem()

	fileId,_ := fs.NewFile(bytes.NewBufferString("1"))
	rootDirId,_ := fs.NewDir(&Dir{Entries:make([]*DirEntry, 0)})
	emptyDirId,_ := fs.NewDir(&Dir{Entries:make([]*DirEntry, 0)})

	newRoot1,_ := fs.recursiveCloneDirWithReplacement(rootDirId, ".", NewDirEntry("parent", emptyDirId, DIR_TYPE), true)
	c.Assert(newRoot1, Not(Equals), INVALID_ID)

	newRoot2,_ := fs.recursiveCloneDirWithReplacement(newRoot1, "parent", NewDirEntry("child", emptyDirId, DIR_TYPE), true)
	c.Assert(newRoot2, Not(Equals), INVALID_ID)

	newRoot3,_ := fs.recursiveCloneDirWithReplacement(newRoot2, "parent/child", NewDirEntry("file", fileId, FILE_TYPE), true)
	c.Assert(newRoot3, Not(Equals), INVALID_ID)

//	dir, _ := fs.ReadDir(newRoot3)
//	fmt.Printf("entries\n")
//	for i, e := range(dir.GetEntries()) {
//		fmt.Printf("Entry %d: %s\n", i, e.GetName())
//	}

	c.Assert(fs.FileExists(newRoot3, "parent"), Equals, true)
	c.Assert(fs.FileExists(newRoot3, "parent/child"), Equals, true)
	c.Assert(fs.FileExists(newRoot3, "parent/child/file"), Equals, true)
}
