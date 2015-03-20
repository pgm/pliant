package fusebinding

import (
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

type flushCallback func (parentDir string, name string, handle *os.File)

type transientFile struct {
	callback flushCallback
	name string
	parentDir string
	creationTime uint64
	length uint64
	handle *os.File

	nodefs.File
}

type TransientFs struct {
	pathfs.FileSystem

	workDir string
	callback flushCallback

	lock sync.Mutex
	parentDirs map [string] map [string] *transientFile
	nextFn int
}

func NewTransientFilesystem(workDir string, callback flushCallback) *TransientFs {
	return &TransientFs{FileSystem: pathfs.NewDefaultFileSystem(), parentDirs: make(map[string] map [string]*transientFile), workDir: workDir, callback: callback}
}

func findByName(files map [string] *transientFile, name string) *transientFile {
	found, ok := files[name]
	if !ok {
		return nil
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
		log.Printf("No Entries in parentDirs[%s]", parentDir)
		return nil, fuse.ENOENT
	}

	log.Printf("Entries in parentDirs[%s] = %d", parentDir, len(files))
	for k, _ := range(files) {
		log.Printf("Entries %s, looking for %s", k, name)
	}
	found := findByName(files, name)

	if found == nil {
		log.Printf("GetAttr return ENO")
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
		files = make(map[string]*transientFile)
	}

	// check to see if this file already exists
	found := findByName(files, name)

	if (flags & fuse.O_ANYWRITE) != 0 {
		if found != nil {
			return nil, fuse.EPERM
		}

		handle, err := os.Create(self.getTempFilename())
		if err != nil {
			log.Printf("returning error: %s", err.Error())
			return nil, fuse.EIO
		}
		creationTime := time.Now().Unix()
		newFile := NewTransientFile(parentDir, name, uint64(creationTime), 0, handle, self.callback)
		files[name] = newFile

		self.parentDirs[parentDir] = files

		// should I really be creating a new File instance with each time the file is opened?  Or reusing them?
		// Need to check on how release works.
		return newFile, fuse.OK

	} else {
		// if we opened this read-only
		if found == nil {
			return nil, fuse.ENOENT
		}

		return found, fuse.OK
	}
}

func NewTransientFile(parentDir string, name string, creationTime uint64, length uint64, handle *os.File, callback flushCallback) *transientFile {
	f := new(transientFile)
	f.File = nodefs.NewDefaultFile()
	f.parentDir = parentDir
	f.name = name
	f.creationTime = creationTime
	f.length = length
	f.handle = handle
	f.callback = callback
	return f
}

func (self *transientFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return fuse.ReadResultFd(self.handle.Fd(), off, len(dest)), fuse.OK
}

func (self *transientFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	n, err := self.handle.WriteAt(data, off)
	self.length += uint64(n)
	written = uint32(n)
	if err == nil {
		return written, fuse.OK
	} else {
		log.Printf("Write failed: %s", err.Error())
		return written, fuse.EIO
	}
}

func (self *transientFile) Truncate(size uint64) fuse.Status {
	err := self.handle.Truncate(int64(size))
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
	}

	c = make([]fuse.DirEntry, len(files))
	i := 0
	for name, _ := range(files) {
		c[i] = fuse.DirEntry{Mode: fuse.S_IFREG, Name: name}
		i++
	}

	return c, fuse.OK
}

// it'd be faster/more efficient to just move the file into the cached storage
func (self *transientFile) Flush() fuse.Status {
	log.Printf("Flush(%s)", self.name)
	self.callback(self.parentDir, self.name, self.handle)
	return fuse.OK
}

func (self *transientFile) Release() {
	os.Remove(self.handle.Name())
	log.Printf("Release(%s)", self.name)
}

//func main() {
//	flag.Parse()
//	if len(flag.Args()) < 1 {
//		log.Fatal("Usage:\n  hello MOUNTPOINT")
//	}
//
//	nfs := pathfs.NewPathNodeFs(NewTransientFilesystem("/tmp/transfs"), nil)
//	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
//	server.SetDebug(true)
//	if err != nil {
//		log.Fatalf("Mount fail: %v\n", err)
//	}
//	server.Serve()
//}
