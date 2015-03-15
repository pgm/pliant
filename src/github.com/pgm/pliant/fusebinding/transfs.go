// A Go mirror of libfuse's hello.c

package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"os"
	"sync"
	"time"
	pathpkg "path"
)

type TempFile struct {
	name string
	creationTime uint64
	length uint64
	handle *os.File
}

type TransientFs struct {
	pathfs.FileSystem

	workDir string

	lock sync.Mutex
	parentDirs map [string] []*TempFile
	nextFn int
}

func NewTransientFilesystem(workDir string) *TransientFs {
	return &TransientFs{FileSystem: pathfs.NewDefaultFileSystem(), parentDirs: make(map[string] []*TempFile), workDir: workDir}
}


func findByName(files []*TempFile, name string) *TempFile {
	var found *TempFile = nil

	for _, f := range(files) {
		if f.name == name {
			found = f
		}
	}

	return found
}

func (self *TransientFs) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Printf("GetAttr(%s)", path)
	self.lock.Lock()
	defer self.lock.Unlock()

	if path == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}

	parentDir, name := pathpkg.Split(path)

	files, ok := self.parentDirs[parentDir]
	if ! ok {
		return nil, fuse.ENOENT
	}

	found := findByName(files, name)

	if found == nil {
		return nil, fuse.ENOENT
	}

	return &fuse.Attr{
		Mode: fuse.S_IFREG | 0644,
		Atime: found.creationTime,
		Mtime: found.creationTime,
		Ctime: found.creationTime,
		Size: found.length}, fuse.OK
}

func (self *TransientFs) getTempFilename() string {
//	self.lock.Lock()
//	defer self.lock.Unlock()
	self.nextFn ++;
	return fmt.Sprintf("%s/%d", self.workDir, self.nextFn)
}

func (self *TransientFs) Create(path string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	log.Printf("Create %s", path)

	return self.Open(path, flags, context)
}

func (self *TransientFs) Open(path string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("Open %s", path)

	parentDir, name := pathpkg.Split(path)

	files, ok := self.parentDirs[parentDir]
	if ! ok {
		files = make([]*TempFile, 0, 10)
	}

	// check to see if this file already exists
	found := findByName(files, name)

	if (flags & fuse.O_ANYWRITE) != 0 {
		if found != nil {
			return nil, fuse.EPERM
		}

		handle, err := os.Create(self.getTempFilename())
		if err != nil {
			log.Printf("returning err")
			return nil, fuse.EIO
		}
		creationTime := time.Now().Unix()
		state := &TempFile{	name: name, creationTime: uint64(creationTime), length: 0, handle: handle }
		files = append(files, state )

		self.parentDirs[parentDir] = files

		// should I really be creating a new File instance with each time the file is opened?  Or reusing them?
		// Need to check on how release works.
		return NewTransientFile(state), fuse.OK

	} else {
		// if we opened this read-only
		if found == nil {
			return nil, fuse.ENOENT
		}

		return NewTransientFile(found), fuse.OK
	}
}

type transientFile struct {
	state *TempFile

	nodefs.File
}

func NewTransientFile(state *TempFile) nodefs.File {
	f := new(transientFile)
	f.state = state
	f.File = nodefs.NewDefaultFile()
	return f
}

func (self *transientFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return fuse.ReadResultFd(self.state.handle.Fd(), off, len(dest)), fuse.OK
}

func (self *transientFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	n, err := self.state.handle.WriteAt(data, off)
	self.state.length += uint64(n)
	written = uint32(n)
	if err == nil {
		return written, fuse.OK
	} else {
		log.Printf("Write failed: %s", err.Error())
		return written, fuse.EIO
	}
}

func (self *transientFile) Release() {
	log.Printf("Release called")
}

func (self *transientFile) Truncate(size uint64) fuse.Status {
	err := self.state.handle.Truncate(int64(size))
	if err == nil {
		return fuse.OK
	} else {
		log.Printf("Truncate failed: %s", err.Error())
		return fuse.EIO
	}
}

//func (self *transientFile) GetAttr(out *fuse.Attr) fuse.Status {
//	log.Printf("transientFile GetAttr")
//
//	return fuse.OK
//}

//Allocate(off uint64, size uint64, mode uint32) (code fuse.Status)


func (self *TransientFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	self.lock.Lock()
	defer self.lock.Unlock()


	files, ok := self.parentDirs[name]
	if ! ok {
		// TODO: Add handling of "", or perhaps make this always an empty list instead of fuse/ENOENT?
		return nil, fuse.OK
//		return nil, fuse.ENOENT
	}

	c = make([]fuse.DirEntry, len(files))
	for i, f := range(files) {
		c[i] = fuse.DirEntry{Mode: fuse.S_IFREG, Name: f.name}
	}

	return c, fuse.OK
}


func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}

	nfs := pathfs.NewPathNodeFs(NewTransientFilesystem("/tmp/transfs"), nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	server.SetDebug(true)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
