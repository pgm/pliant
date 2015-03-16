package low

import (
	"bytes"
	. "gopkg.in/check.v1"
	"github.com/golang/protobuf/proto"
)

type ProtobufSuite struct{}

var _ = Suite(&ProtobufSuite{})

func (s *ProtobufSuite) TestProtobuf (c *C) {
	buffer := PackFileMetadata(&FileMetadata{Length: proto.Int64(1)})
	UnpackFileMetadata(bytes.NewBuffer(buffer))
}
