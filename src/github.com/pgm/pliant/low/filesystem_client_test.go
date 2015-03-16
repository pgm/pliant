package low

import (

	. "gopkg.in/check.v1"
	"fmt"
)

type FilesystemClientSuite struct{}
var _ = Suite(&FilesystemClientSuite{})
var _ = fmt.Sprintf("hello!")

//func makeFilesystemClient() (*FilesystemClient,*FilesystemClient) {
//	local_chunks1 := NewMemChunkService()
//	local_labels1 := NewMemLabelService()
//	local_chunks2 := NewMemChunkService()
//	local_labels2 := NewMemLabelService()
//	master_chunks := NewMemChunkService()
//	master_labels := NewMemLabelService()
//
////	return NewFilesystemClient(local_chunks1, local_labels1, master_chunks, master_labels), NewFilesystemClient(local_chunks2, local_labels2, master_chunks, master_labels)
//
//}
//
//func (s *FilesystemClient) TestRawFilesystemDirs (c *C) {
//	buffer := make([]byte, 1)
//
//	client1, client2 := makeFilesystemClient()
//
//	c.Assert(client1.LabelEmptyDir("x"), IsNil)
//	c.Assert(client1.WriteFile("x", "y", bytes.NewBufferString("a")), IsNil)
//	c.Assert(client1.Push("x"), IsNil)
//
//	c.Assert(client2.Pull("x"), IsNil)
//	c.Assert(client2.ReadFile("x", "a", 0, 1, buffer), IsNil)
//	c.Assert(buffer[0], Equals, uint8('a'))
//}

