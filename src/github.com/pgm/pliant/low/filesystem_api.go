package low

import (
	"sync"
	"github.com/golang/protobuf/proto"
	"io"
	"errors"
	"strings"
)

// higher level filesystem interface.  Incorporates concept of label for identifying root dir, and paths within that tree.
// all operations based on label and path

type Filesystem struct {
	labels  LabelService
	fs      *RawFilesystem
	mapLock sync.Mutex
	labelLocks map[string] *sync.RWMutex
}

func NewFilesystem(labels LabelService, rawFs *RawFilesystem) *Filesystem {
	fs := &Filesystem{labels: labels, fs: rawFs, labelLocks: make(map[string] *sync.RWMutex)}
	return fs
}

func (self *Filesystem) LabelEmptyDir(label string) error {
	wlock := self.getLabelLock(label)
	wlock.Lock()
	defer wlock.Unlock()

	return self.labels.UpdateLabel(label, EMPTY_DIR_ID)
}

func (self *Filesystem) getLabelLock(label string) *sync.RWMutex {
	self.mapLock.Lock()
	defer self.mapLock.Unlock()

	lock, exists := self.labelLocks[label]

	if !exists {
		lock = new(sync.RWMutex)
		self.labelLocks[label] = lock
	}

	return lock
}

func NewDirEntry(name string, chunk ChunkID, chunk_type ChunkType, metadata *FileMetadata) *DirEntry {
	return &DirEntry{Name: proto.String(string(name)), Chunk: proto.String(string(chunk)), Type: proto.Int32(int32(chunk_type)), Metadata: metadata}
}

func (self *Filesystem) MakeDir(label string, vpath string) error {
	wlock := self.getLabelLock(label)
	wlock.Lock()
	defer wlock.Unlock()

	origRootId, getRootErr := self.labels.GetRoot(label)
	if getRootErr != nil {
		return getRootErr
	}

	parentPath, name := oddSplit(vpath)

	newRootId, cloneErr := self.fs.recursiveCloneDirWithReplacement(origRootId, parentPath, name, NewDirEntry(name, EMPTY_DIR_ID, DIR_TYPE, nil), false)
	if cloneErr != nil {
		return cloneErr
	}

	updateLabelErr := self.labels.UpdateLabel(label, newRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}

func (self *Filesystem) WriteFile(label string, vpath string, content io.Reader) error {
	wlock := self.getLabelLock(label)
	wlock.Lock()
	defer wlock.Unlock()

	origRootId, getRootErr := self.labels.GetRoot(label)
	if getRootErr != nil {
		return getRootErr
	}

	parentPath, name := oddSplit(vpath)

	fileId, metadata, newFileErr := self.fs.NewFile(content)
	if(newFileErr != nil) {
		return newFileErr
	}

	newRootId, cloneErr := self.fs.recursiveCloneDirWithReplacement(origRootId, parentPath, name, NewDirEntry(name, fileId, FILE_TYPE, metadata), false)
	if cloneErr != nil {
		return cloneErr
	}

	updateLabelErr := self.labels.UpdateLabel(label, newRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}

// rename this function
func oddSplit(vpath string) (string, string) {
	var parentPath string
	var name string
	if strings.Contains(vpath, "/") {
		parentPath, name = splitPathTo(vpath)
	} else {
		parentPath = "."
		name = vpath
	}

	return parentPath, name
}

func (self *Filesystem) Unlink(label string, vpath string) error {
	wlock := self.getLabelLock(label)
	wlock.Lock()
	defer wlock.Unlock()

	origRootId, getRootErr := self.labels.GetRoot(label)
	if getRootErr != nil {
		return getRootErr
	}

	parentPath, name := oddSplit(vpath)

	newRootId, cloneErr := self.fs.recursiveCloneDirWithReplacement(origRootId, parentPath, name, nil, true)
	if cloneErr != nil {
		return cloneErr
	}

	updateLabelErr := self.labels.UpdateLabel(label, newRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}

func (self *Filesystem) Rename(src_label string, src_path string, dst_label string, dst_path string) error {
	linkErr := self.Link(src_label, src_path, dst_label, dst_path)
	if linkErr != nil {
		return linkErr
	}

	unlinkErr := self.Unlink(src_label, src_path)
	return unlinkErr
}

func (self *Filesystem) Link(src_label string, src_path string, dst_label string, dst_path string) error {
	// lock in alphabetical order
	if src_label == dst_label {
		wlock := self.getLabelLock(dst_label)
		wlock.Lock()
		defer wlock.Unlock()
	} else if src_label < dst_label {
		rlock := self.getLabelLock(src_label)
		rlock.RLock()
		defer rlock.Unlock()

		wlock := self.getLabelLock(dst_label)
		wlock.Lock()
		defer wlock.Unlock()
	} else {
		wlock := self.getLabelLock(dst_label)
		wlock.Lock()
		defer wlock.Unlock()

		rlock := self.getLabelLock(src_label)
		rlock.RLock()
		defer rlock.Unlock()
	}

	srcRootId, srcGetRootErr := self.labels.GetRoot(src_label)
	if srcGetRootErr != nil {
		return srcGetRootErr
	}

	dstRootId, dstGetRootErr := self.labels.GetRoot(dst_label)
	if dstGetRootErr != nil {
		return dstGetRootErr
	}

	dirEntry, getFileIdErr := self.fs.GetFileId(srcRootId, src_path)
	if getFileIdErr != nil {
		return getFileIdErr
	}
	dst_parent, dst_name := oddSplit(dst_path)

	newDirEntry := &DirEntry{
		Name: &dst_name,
		Chunk: proto.String(dirEntry.GetChunk()),
		Metadata: dirEntry.GetMetadata(),
		Type: proto.Int32(dirEntry.GetType())}
	newDstRootId, cloneErr := self.fs.recursiveCloneDirWithReplacement(dstRootId, dst_parent, dst_name, newDirEntry, false)

	if cloneErr != nil {
		return cloneErr
	}

	updateLabelErr := self.labels.UpdateLabel(dst_label, newDstRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}

func (self *Filesystem) Label(existing_label string, vpath string, new_label string) error {
	if existing_label == new_label {
		panic("src and dst labels are the same")
	}

	// lock in alphabetical order
	if existing_label < new_label {
		rlock := self.getLabelLock(existing_label)
		rlock.RLock()
		defer rlock.Unlock()

		wlock := self.getLabelLock(new_label)
		wlock.Lock()
		defer wlock.Unlock()
	} else {
		wlock := self.getLabelLock(new_label)
		wlock.Lock()
		defer wlock.Unlock()

		rlock := self.getLabelLock(existing_label)
		rlock.RLock()
		defer rlock.Unlock()
	}

	srcRootId, srcGetRootErr := self.labels.GetRoot(existing_label)
	if srcGetRootErr != nil {
		return srcGetRootErr
	}

	newDstRootId, getChunkIdErr := self.getChunkId(srcRootId, vpath)
	if getChunkIdErr != nil {
		return getChunkIdErr
	}

	updateLabelErr := self.labels.UpdateLabel(new_label, newDstRootId)
	if updateLabelErr != nil {
		return updateLabelErr
	}

	return nil
}


func (self *Filesystem) getLabelRoot(label string) (ChunkID, error) {
	rlock := self.getLabelLock(label)
	rlock.RLock()
	rootId, getRootErr := self.labels.GetRoot(label)
	rlock.RUnlock()
	return rootId, getRootErr
}

func (self *Filesystem) ReadFile(label string, vpath string, offset int64, buffer []byte) (int, error) {
	rootId, getRootErr := self.getLabelRoot(label)

	if getRootErr != nil {
		return 0, getRootErr
	}

	entry, getFileIdErr := self.fs.GetFileId(rootId, vpath)
	if(getFileIdErr != nil) {
		return 0, getFileIdErr
	}

	if entry == nil {
		return 0, errors.New("No such file")
	}

	if entry.GetType() != int32(FILE_TYPE) {
		return 0, errors.New("Was not file")
	}

	n, readErr := self.fs.ReadFile(ChunkID(entry.GetChunk()), offset, buffer)

	return n, readErr
}

func (self *Filesystem) getChunkId(rootId ChunkID, vpath string) (ChunkID, error) {
	var dirId ChunkID
	if vpath == "." {
		dirId = rootId
	} else {
		entry, getFileIdErr := self.fs.GetFileId(rootId, vpath)
		if(getFileIdErr != nil) {
			return INVALID_ID, getFileIdErr
		}

		if entry == nil {
			return INVALID_ID, errors.New("No such file")
		}

		if entry.GetType() != int32(DIR_TYPE) {
			return INVALID_ID, errors.New("Was not directory")
		}

		dirId = ChunkID(entry.GetChunk())
	}
	return dirId, nil
}

func (self *Filesystem) ReadDir(label string, vpath string) (*Dir, error) {
	rootId, getRootErr := self.getLabelRoot(label)

	if getRootErr != nil {
		return nil, getRootErr
	}

	dirId, getChunkIdErr := self.getChunkId(rootId, vpath)
	if getChunkIdErr != nil {
		return nil, getChunkIdErr
	}

	dir, readErr := self.fs.ReadDir(dirId)
	return dir, readErr
}





//Write operations:
//Read operations:
//  FileExists(label string, vpath string) (bool, error)


