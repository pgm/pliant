// A Go mirror of libfuse's hello.c

package main

import (
	"flag"
	"log"
	"fmt"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/pgm/fffs_go"
	"bytes"
)

type RuseFs struct {
	pathfs.FileSystem
	fs *fffs_go.Filesystem
}

func (self *RuseFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	parentPath := "."

	dir, _ := self.fs.ReadDir("root", parentPath)

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
			return &fuse.Attr{
				Mode: fuse.S_IFREG | 0644,
				Atime: creationTime,
				Mtime: creationTime,
				Ctime: creationTime,
				Size: uint64(entry.GetMetadata().GetLength())}, fuse.OK
		}
	}
}

func (self *RuseFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		dir, _ := self.fs.ReadDir("root", ".")

		c = make([]fuse.DirEntry, 0, len(dir.GetEntries()))
		for _, e := range(dir.GetEntries()) {
//			fmt.Printf("file: %s %d %d\n", e.GetName(), e.GetMetadata().GetLength(), e.GetMetadata().GetCreationTime() )
			c = append(c, fuse.DirEntry{Name: e.GetName(), Mode: fuse.S_IFREG})
		}

		fmt.Printf("Count %d\n", len(c))

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

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}

	chunks := fffs_go.NewMemChunkService()
	labels := fffs_go.NewMemLabelService()

	rawFs := fffs_go.NewRawFilesystem(chunks)
	fs := fffs_go.NewFilesystem(labels, rawFs)

	fs.LabelEmptyDir("root")
	fs.WriteFile("root", "x", bytes.NewBufferString("z"))

	nfs := pathfs.NewPathNodeFs(&RuseFs{FileSystem: pathfs.NewDefaultFileSystem(), fs: fs}, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
