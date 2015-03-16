package fusebinding

import (
	"github.com/pgm/pliant/low"

	. "gopkg.in/check.v1"
	"fmt"
	"bytes"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"log"
	"io/ioutil"
	"os"
	"github.com/hanwen/go-fuse/fuse"
	"testing"
//	"time"
)

type RwfsSuite struct{
	mountPoint string
	workDir string
	server *fuse.Server
}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&RwfsSuite{})
var _ = fmt.Sprintf("unused")

func (s *RwfsSuite) SetUpTest(c *C) {
	log.Printf("Setup test start")
	var err error

	s.mountPoint, err = ioutil.TempDir("", "rwfstest-mount")
	if err != nil {
		panic(err.Error())
	}

	s.workDir, err = ioutil.TempDir("", "rwfstest-work")
	if err != nil {
		panic(err.Error())
	}

	log.Printf("Setup test: workDir: %s", s.workDir)
	log.Printf("Setup test: mountPoint: %s", s.mountPoint)

	chunks := low.NewMemChunkService()
	labels := low.NewMemLabelService()

	rawFs := low.NewRawFilesystem(chunks)
	fs := low.NewFilesystem(labels, rawFs)

	fs.LabelEmptyDir("root")
	fs.WriteFile("root", "x", bytes.NewBufferString("z"))

	//	chunks := fffs_go.NewMemChunkService()
	//	labels := fffs_go.NewMemLabelService()
	//	rawFs := fffs_go.NewRawFilesystem(chunks)
	//	filesystem := NewFilesystem(labels, rawFs)

	rwfs := NewRwFs(s.workDir, fs, "root")
	nfs := pathfs.NewPathNodeFs(rwfs, nil)
	server, _, err := nodefs.MountRoot(s.mountPoint, nfs.Root(), nil)
	s.server = server
	s.server.SetDebug(true)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	log.Printf("Setup test: Serve")
	go s.server.Serve()
	log.Printf("Setup test: Wait Mount")
	s.server.WaitMount()
	log.Printf("Setup test: done")
	// TODO: figure out how to block until the mount has completed
//	time.Sleep(2 * time.Second)
	rwfs.WaitForAccess()
}

func (s *RwfsSuite) TearDownTest(c *C) {
	log.Printf("Teardown: start")
	err := s.server.Unmount()
	if err != nil {
		panic(err.Error())
	}

	os.RemoveAll(s.mountPoint)
	os.RemoveAll(s.workDir)

	log.Printf("Teardown: done")
}

//func makeFilesystemClient() (*FilesystemClient,*FilesystemClient) {
//	local_chunks1 := NewMemChunkService()
//	local_labels1 := NewMemLabelService()
//	local_chunks2 := NewMemChunkService()
//	local_labels2 := NewMemLabelService()
//	master_chunks := NewMemChunkService()
//	master_labels := NewMemLabelService()
//
////	return NewFilesystemClient(local_chunks1, local_labels1, master_chunks, master_labels), NewFilesystemClient(local_chunks2, local_labels2, master_chunks, master_labels)
//
//}

func (s *RwfsSuite) TestReadFromFuseFile (c *C) {
	log.Printf("TestRead start")

	buffer := make([]byte, 100)

	file, err := os.Open(fmt.Sprintf("%s/x", s.mountPoint))
	c.Assert(err, IsNil)
	n, err := file.Read(buffer)
	c.Assert(n, Equals, 1)
	c.Assert(buffer[0], Equals, uint8('z'))

	log.Printf("TestRead done")
}

