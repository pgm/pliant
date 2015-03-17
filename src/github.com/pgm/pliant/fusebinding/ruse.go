package fusebinding

import (


	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/pgm/pliant/low"
	"log"
	"path/filepath"
)

type RuseFs struct {
	pathfs.FileSystem
	fs *low.Filesystem
	label string
}

func (self *RuseFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	parentPath, filename := filepath.Split(name)
	if parentPath == "" {
		parentPath = "."
	} else {
		parentPath = parentPath[:len(parentPath)-1]
	}
	log.Printf("RuseFs.GetAttr('%s', '%s')", parentPath, filename)

	dir, err := self.fs.ReadDir(self.label, parentPath)
	if err != nil {
		log.Printf("RuseFs.GetAttr, ReadDir err = %s", err.Error())
		return nil, fuse.ENOENT
	}

	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	} else {
		entry := dir.Get(filename)
		if entry == nil {
			log.Printf("RuseFs.GetAttr ret enoent: %s", name)
			return nil, fuse.ENOENT
		} else {
			creationTime := uint64(entry.GetMetadata().GetCreationTime())
			mode := uint32(0644)
			if(low.ChunkType(entry.GetType()) == low.DIR_TYPE) {
				mode = mode | fuse.S_IFDIR | 0777
			} else {
				mode = mode | fuse.S_IFREG
			}
			log.Printf("RuseFs.GetAttr ret ent")
			return &fuse.Attr{
				Mode: mode,
				Atime: creationTime,
				Mtime: creationTime,
				Ctime: creationTime,
				Size: uint64(entry.GetMetadata().GetLength())}, fuse.OK
		}
	}
}

func (self *RuseFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		dir, _ := self.fs.ReadDir(self.label, ".")

		c = make([]fuse.DirEntry, 0, len(dir.GetEntries()))
		for _, e := range(dir.GetEntries()) {
//			fmt.Printf("file: %s %d %d\n", e.GetName(), e.GetMetadata().GetLength(), e.GetMetadata().GetCreationTime() )
			c = append(c, fuse.DirEntry{Name: e.GetName(), Mode: fuse.S_IFREG})
		}

		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (self *RuseFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}

	return NewRuseFile(self.label, name, self.fs), fuse.OK
}

type ruseFile struct {
	label string
	name string
	fs *low.Filesystem
	nodefs.File
}

func NewRuseFile(label string, name string, fs *low.Filesystem) * ruseFile {
	return &ruseFile{label: label, name: name, fs: fs, File: nodefs.NewDefaultFile()}
}

func (self *ruseFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	n, err := self.fs.ReadFile(self.label, self.name, off, buf)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(buf[0:n]), fuse.OK
}

func (self *RuseFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Printf("Mkdir(%s)", name)
	err := self.fs.MakeDir(self.label, name)
	if err == nil {
		return fuse.OK
	} else {
		log.Printf("Mkdir failed: %s", err.Error())
		return fuse.EIO
	}
}

func NewRuseFs(label string, filesystem *low.Filesystem) *RuseFs {
	return &RuseFs{FileSystem: pathfs.NewDefaultFileSystem(), fs: filesystem, label: label}
}

func (self *RuseFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	log.Printf("Unlink(%s)", name)
	err := self.fs.Unlink(self.label, name)
	if err != nil {
		log.Printf("Got error in unlink: %s", err.Error())
		return fuse.EIO
	}
	return fuse.OK
}

func (self *RuseFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return self.Unlink(name, context)
}
