// Hellofs implements a simple "hello world" file system.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sync"

	"gopkg.in/gcfg.v1"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/codegangsta/cli"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/startup"
	"github.com/pgm/pliant/v2/tagsvc"
	"golang.org/x/net/context"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s CTLPATH MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func startMount(addr string, mountpoint string) {
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
	transient := NewTransientClient()
	filesystem := FS{client: client, transient: transient}
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

func main() {
	app := cli.NewApp()
	app.Name = "pliantfuse"
	app.Usage = "pliant fuse client"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "dev", Value: "", Usage: "Enable development mode.  Root service, local service and fuse client all run in-process."},
	}

	app.Action = func(c *cli.Context) {
		mountpoint := c.Args().Get(0)
		bindAddr := ""
		fmt.Printf("mount point: %s\n", mountpoint)

		devConfig := c.GlobalString("dev")
		if devConfig != "" {
			cfg := struct {
				S3 struct {
					AccessKeyId     string
					SecretAccessKey string
					Endpoint        string
					Bucket          string
					Prefix          string
				}
				Settings struct {
					Port        int
					PersistPath string
					AuthSecret  string
				}
			}{}

			fd, err := os.Open(devConfig)
			if err != nil {
				log.Fatalf("Could not open %s", devConfig)
			}
			err = gcfg.ReadInto(&cfg, fd)
			if err != nil {
				log.Fatalf("Failed to parse %s: %s", devConfig, err)
			}
			fd.Close()

			config := &tagsvc.Config{AccessKeyId: cfg.S3.AccessKeyId,
				SecretAccessKey: cfg.S3.SecretAccessKey,
				Endpoint:        cfg.S3.Endpoint,
				Bucket:          cfg.S3.Bucket,
				Prefix:          cfg.S3.Prefix,
				MasterPort:      cfg.Settings.Port,
				PersistPath:     cfg.Settings.PersistPath,
				AuthSecret:      cfg.Settings.AuthSecret}
			_, err = tagsvc.StartServer(config)

			if err != nil {
				panic(err.Error())
			}

			tempRoot, err := ioutil.TempDir("", "pliant")
			if err != nil {
				panic(err.Error())
			}

			ctlPath := tempRoot + "/ctl"
			bindAddr = ctlPath
			cachePath := tempRoot + "/cache"

			//bindAddr := fmt.Sprintf("http://localhost:%d", cfg.Local.Port)
			startup.StartLocalService(fmt.Sprintf("localhost:%d", config.MasterPort),
				config.AuthSecret,
				cachePath,
				ctlPath, "")
		}
		startMount(bindAddr, mountpoint)
	}

	app.Run(os.Args)
}

type TransientFile struct {
	lock      sync.Mutex
	localPath string
	file      *os.File
}

type TransientDir struct {
	files map[string]*TransientFile
}

type TransientClient struct {
	lock  sync.Mutex
	paths map[string]*TransientDir
}

func NewTransientClient() *TransientClient {
	return &TransientClient{paths: make(map[string]*TransientDir)}
}

func (t *TransientClient) ListFiles(path string) []string {
	t.lock.Lock()
	defer t.lock.Unlock()

	dir, found := t.paths[path]
	if !found {
		return nil
	}

	files := make([]string, 0, len(dir.files))
	for k, _ := range dir.files {
		files = append(files, k)
	}

	return files
}

func (t *TransientClient) GetFile(fullPath string) *TransientFile {
	t.lock.Lock()
	defer t.lock.Unlock()

	parent, name := path.Split(fullPath)
	dir, found := t.paths[parent]
	if !found {
		return nil
	}

	file, found := dir.files[name]
	if !found {
		return nil
	}

	return file
}

func (t *TransientClient) CreateFile(path string) (*TransientFile, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	panic("unimp")
}

type FS struct {
	client    *rpc.Client
	transient *TransientClient
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{path: "", fs: f}, nil
}

func connectToServer(addr string) *rpc.Client {
	client, err := rpc.Dial("unix", addr)
	if err != nil {
		panic(fmt.Sprintf("%s: %s", err.Error(), addr))
	}
	return client
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	path string
	fs   *FS
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0777
	fmt.Printf("Attr(%s) -> %o\n", d.path, a.Mode)
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	var result v2.StatResponse
	filename := d.path + "/" + name
	err := d.fs.client.Call("AtomicClient.Stat", filename, &result)
	if err != nil {
		fmt.Printf("Stat failed: %s, returning no such file\n", err.Error())
		return nil, fuse.ENOENT
	}

	if result.Error == v2.STAT_ERROR_NONE {
		if result.IsDir {
			d := &Dir{path: filename, fs: d.fs}
			_ = fs.NodeCreater(d)
			fmt.Printf("Lookup(%s) -> dir\n", filename)
			return d, nil
		} else {
			return &File{path: filename, fs: d.fs, size: uint64(result.Size)}, nil
		}
	} else {
		if result.Error != v2.STAT_ERROR_MISSING {
			panic("bad error")
		}

		// not in pliant filesystem, check transient files
		file := d.fs.transient.GetFile(filename)
		if file == nil {
			return nil, fuse.ENOENT
		}

		return file, nil
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var result []v2.ListFilesRecord
	err := d.fs.client.Call("AtomicClient.ListFiles", d.path, &result)
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

	// merge in transient files
	transientFiles := d.fs.transient.ListFiles(d.path)
	for _, file := range transientFiles {
		dirDirs = append(dirDirs, fuse.Dirent{Name: file, Type: fuse.DT_File})
	}

	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	path string
	fs   *FS
	size uint64
}

type FileHandle struct {
	file *os.File
}

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

	fmt.Printf("open(%s)\n", f.path)

	err := f.fs.client.Call("AtomicClient.GetLocalPath", f.path, &localPath)
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

func (f *TransientFile) Attr(ctx context.Context, a *fuse.Attr) error {
	//	a.Inode = 2
	a.Mode = 0777
	fi, err := f.file.Stat()
	if err != nil {
		return err
	}
	a.Size = uint64(fi.Size())
	return nil
}

func (f *TransientFile) Forget() {
	f.file.Close()
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	filename := d.path + "/" + req.Name
	fmt.Printf("Create %s\n", filename)
	node, err := d.fs.transient.CreateFile(filename)
	if err != nil {
		return nil, nil, err
	}
	return node, node, err
}

// func (f *TransientFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
// 	// FIXME: make sure this is already open
// 	file, err := os.Open(f.localPath)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return f, nil
// }

func (f *TransientFile) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
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

func (f *TransientFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := f.file.WriteAt(req.Data, req.Offset)
	if err != nil {
		resp.Size = 0
		return err
	}

	resp.Size = n
	return nil
}
