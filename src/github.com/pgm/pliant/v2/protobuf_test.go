package v2

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
)

type ProtobufSuite struct{}

var _ = Suite(&ProtobufSuite{})

func (s *ProtobufSuite) TestProtobuf(c *C) {
	buffer := PackFileMetadata(&FileMetadata{Length: proto.Int64(1)})
	UnpackFileMetadata(bytes.NewBuffer(buffer))
}
