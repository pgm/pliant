package fffs_go

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

//type MetadataService interface {
//	GetFileMetadata(id ChunkID) (*FileMetadata, error)
//	SetFileMetadata(id ChunkID, metadata *FileMetadata) error
//}
//
//type MetadataAdapter struct {
//	chunks ChunkService
//}
//
//func (self *MetadataAdapter) GetFileMetadata(id ChunkID) (*FileMetadata, error) {
//	chunk, _, err := self.chunks.Read(id, 0, -1)
//	if err != nil {
//		return nil, err
//	}
//	return UnpackFileMetadata(chunk), nil
//}
//
//func (self *MetadataAdapter) SetFileMetadata(id ChunkID, metadata *FileMetadata) error {
//	return err
//}
