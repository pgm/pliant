package v2

import (
"fmt"
"io"
"bytes"
"github.com/golang/protobuf/proto"
)

func PackFileMetadata(metadata *FileMetadata) []byte {
	data, err := proto.Marshal(metadata)
	if err != nil {
		panic("Couldn't marshal metadata object")
	}
	return data
}

func UnpackFileMetadata(r io.Reader) *FileMetadata {
	dest := &FileMetadata{}
	buffer := bytes.Buffer{}
_, readErr := buffer.ReadFrom(r)
if readErr != nil {
panic("Could not read")
}
err := proto.Unmarshal(buffer.Bytes(), dest)
if err != nil {
panic(fmt.Sprintf("Could not unmarshal metadata: %s", err.Error()))
}

return dest
}
