// Hellofs implements a simple "hello world" file system.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/pgm/pliant/v2"
	"golang.org/x/net/context"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s CTLPATH MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}
	addr := flag.Arg(0)
	mountpoint := flag.Arg(1)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("pliantfuse"),
		fuse.Subtype("pliant"),
		fuse.LocalVolume(),
		fuse.VolumeName("pliant"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	client := connectToServer(addr)
	filesystem := FS{client: client}
	err = fs.Serve(c, &filesystem)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct {
	client *rpc.Client
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{path: "", client: f.client}, nil
}

func connectToServer(addr string) *rpc.Client {
	client, err := rpc.Dial("unix", addr)
	if err != nil {
		panic(err.Error())
	}
	return client
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	path   string
	client *rpc.Client
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	var result v2.ListFilesRecord
	filename := d.path + "/" + name
	err := d.client.Call("AtomicClient.Stat", filename, &result)
	if err != nil {
		//		return nil, err
		return nil, fuse.ENOENT
	}
	if result.IsDir {
		fmt.Printf("lookup(%s) -> dir\n", filename)
		return &Dir{path: filename, client: d.client}, nil
	} else {
		fmt.Printf("lookup(%s) -> file\n", filename)
		return &File{path: filename, client: d.client, size: uint64(result.Size)}, nil
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var result []v2.ListFilesRecord
	err := d.client.Call("AtomicClient.ListFiles", d.path, &result)
	if err != nil {
		return nil, err
	}
	dirDirs := make([]fuse.Dirent, len(result))
	for i := 0; i < len(result); i++ {
		dirDirs[i].Name = result[i].Name
		if result[i].IsDir {
			dirDirs[i].Type = fuse.DT_Dir
		} else {
			dirDirs[i].Type = fuse.DT_File
		}
	}
	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	path   string
	client *rpc.Client
	size   uint64
}

type FileHandle struct {
	file *os.File
}

const greeting = "hello, world\n"

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//	a.Inode = 2
	a.Mode = 0444
	a.Size = f.size
	return nil
}

func (f *FileHandle) Forget() {
	f.file.Close()
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var localPath string

	err := f.client.Call("AtomicClient.GetLocalPath", f.path, &localPath)
	if err != nil {
		return nil, err
	}

	// FIXME: make sure this is not already open
	file, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}

	return &FileHandle{file: file}, nil
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	_, err := f.file.Seek(req.Offset, 0)
	if err != nil {
		return err
	}

	buffer := make([]byte, req.Size)
	n, err := f.file.Read(buffer)
	if err != nil {
		return err
	}
	// TODO: Was Data allocated before this call?
	resp.Data = buffer[:n]
	return err
}
