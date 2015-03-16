package low

//import (
//	"sync"
//)

type FilesystemClient struct {
	Filesystem
	master_labels LabelService
	cache *CachingChunkService
}

func NewFilesystemClient(local_chunks ChunkService, local_metadata ChunkService,
	local_labels LabelService, master_chunks ChunkService, master_metadata ChunkService,
	master_labels LabelService) * FilesystemClient {
	panic("unimp")

//	data_cache := &CachingChunkService{local: local_chunks, remote: master_chunks}
//	metadata_cache := &CachingChunkService{local: local_metadata, remote: master_metadata}

//	rawFs := NewRawFilesystem(data_cache)
//	return &FilesystemClient{
//		labels: local_labels,
//		fs: rawFs,
//		labelLocks: make(map[string] *sync.RWMutex),
//		master_labels: master_labels,
//		cache: cache}
}

// write all reachable blocks to target and then update lable
func (self *FilesystemClient) Push(label string) error {
	rootId, getRootErr := self.getLabelRoot(label)

	if getRootErr != nil {
		return getRootErr
	}

	self.fs.VisitReachable(rootId, func (chunkId ChunkID) {
			if self.cache.IsChunkLocalOnly(chunkId) {
				err := copyChunk(chunkId, self.cache.local, self.cache.remote)
				if err != nil {
					panic(err.Error())
				}
				self.cache.UpdateChunkStatus(chunkId, false)
			}
		})

	self.master_labels.UpdateLabel(label, rootId)

	return nil
}

func (self *FilesystemClient) Sync(label string) error {
	rootId, getRootErr := self.master_labels.GetRoot(label)
	if getRootErr != nil {
		return getRootErr
	}

	self.labels.UpdateLabel(label, rootId)
	return getRootErr
}

