package fusebinding

import (


	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"github.com/pgm/pliant/low"
	"log"
)

type RuseFs struct {
	pathfs.FileSystem
	fs *low.Filesystem
	label string
}

func (self *RuseFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	parentPath := "."

	dir, _ := self.fs.ReadDir(self.label, parentPath)

	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	} else {
		entry := dir.Get(name)
		if entry == nil {
			return nil, fuse.ENOENT
		} else {
			creationTime := uint64(entry.GetMetadata().GetCreationTime())
			mode := uint32(0644)
			if(low.ChunkType(entry.GetType()) == low.DIR_TYPE) {
				mode = mode | fuse.S_IFDIR | 0777
			} else {
				mode = mode | fuse.S_IFREG
			}
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
	return nodefs.NewDataFile([]byte(name)), fuse.OK
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

