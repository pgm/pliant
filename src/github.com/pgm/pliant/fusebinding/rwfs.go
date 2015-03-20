package fusebinding

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/pgm/pliant/low"
	"os"
	"path/filepath"
)

type RwFs struct {
	pathfs.FileSystem

	wFs *TransientFs
	rFs *RuseFs

	started chan struct{}
}

func NewRwFs(workDir string, filesystem *low.Filesystem, label string) *RwFs {
	rfs := NewRuseFs(label, filesystem)
	callback := func (parentDir string, name string, handle *os.File) {
		// copy the file
		handle.Seek(0, 0)
		rfs.fs.WriteFile(rfs.label, filepath.Join(parentDir, name), handle)
	}
	tfs := NewTransientFilesystem(workDir, callback)
	return &RwFs{started: make(chan struct{}),
		FileSystem: pathfs.NewDefaultFileSystem(), wFs: tfs, rFs: rfs}
}


func (self *RwFs) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	attr, status := self.rFs.GetAttr(path, context)
	if status != fuse.ENOENT {
		return attr, status
	}

	return self.wFs.GetAttr(path, context)
}

func (self *RwFs) Create(path string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	return self.wFs.Create(path, flags, mode, context)
}

func (self *RwFs) Open(path string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	attr, status := self.rFs.Open(path, flags, context)
	if status != fuse.ENOENT {
		return attr, status
	}

	return self.wFs.Open(path, flags, context)
}

func (self *RwFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	rFiles, rStatus := self.rFs.OpenDir(name, context)
	if rStatus != fuse.OK {
		return rFiles, rStatus
	}

	wFiles, _ := self.wFs.OpenDir(name, context)
	entries := make([]fuse.DirEntry, 0, len(rFiles) + len(wFiles))
	for _,x := range(rFiles) {
		entries = append(entries, x)
	}
	for _,x := range(wFiles) {
		entries = append(entries, x)
	}

	return entries, fuse.OK
}

func (self *RwFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	return self.rFs.Mkdir(name, mode, context)
}

func (self *RwFs) 	Unlink(name string, context *fuse.Context) (code fuse.Status) {
	return self.rFs.Unlink(name, context)
}

func (self *RwFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return self.rFs.Rmdir(name, context)
}


func (self *RwFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	close(self.started)
	return fuse.ENOSYS
}

func (self *RwFs) WaitForAccess() {
	<-self.started
}
