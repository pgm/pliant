package fffs_go

import (
	"bytes"
	. "gopkg.in/check.v1"
)

type RawFilesystemSuite struct{}
var _ = Suite(&RawFilesystemSuite{})

func makeFilesystem() *RawFilesystem{
	chunks := NewMemChunkService()
	metadata := NewMemChunkService()

	return NewRawFilesystem(chunks, metadata)
}

/*
func (s *MySuite) TestRawFilesystemDirs (c *C) {
	fs := makeFilesystem()

	entry := NewDirEntry("child", EMPTY_DIR_ID, DIR_TYPE)

	parentDirId, _ := fs.NewDir(&Dir{Entries: []*DirEntry{&entry} })

	dir,_:= fs.ReadDir(parentDirId)
	c.Assert(len(dir.Entries), Equals, 1)
	dir,_ = fs.ReadDir(EMPTY_DIR_ID)
	c.Assert(len(dir.Entries), Equals, 0)
}
*/

func (s *RawFilesystemSuite) TestRawFilesystemFiles (c *C) {
	fs := makeFilesystem()

	fileId,_ := fs.NewFile(bytes.NewBufferString("x"))

	metadata,_ := fs.GetFileMetadata(fileId)
	c.Assert(metadata.GetLength(), Equals, int64(1))

	buffer := make([]byte, 1, 1)
	fs.ReadFile(fileId, 0, 1, buffer)
	c.Assert(buffer[0], Equals, uint8('x'))
}

/*
func (s *RawFilesystemSuite) TestCloneWithReplacement (c *C) {
	fs := makeFilesystem()

	file1Id,_ := fs.NewFile(bytes.NewBufferString("1"))
	file2Id,_ := fs.NewFile(bytes.NewBufferString("2"))

	parentDirId,_ := fs.NewDir(&Dir{Entries:make([]*DirEntry, 0)})

//func (self *RawFilesystem) recursiveCloneDirWithReplacement(rootId ChunkID, parentDir string, newDirEntry *DirEntry, replaceExisting bool) (ChunkID, error) {

	newRoot1,_ := fs.recursiveCloneDirWithReplacement(parentDirId, "/a", NewDirEntry("a", file1Id, FILE_TYPE), true)
	newRoot2,_ := fs.recursiveCloneDirWithReplacement(newRoot1, "/a", NewDirEntry("a", file2Id, FILE_TYPE), true)

	c.Assert(fs.FileExists(parentDirId, "/a"), Equals, false)
	c.Assert(fs.FileExists(newRoot1, "/a"), Equals, true)
	c.Assert(fs.FileExists(newRoot2, "/a"), Equals, true)
}
*/
