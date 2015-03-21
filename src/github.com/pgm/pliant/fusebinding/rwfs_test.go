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
	"time"
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

	rwfs := NewRwFs(s.workDir, fs, "root")
	nfs := pathfs.NewPathNodeFs(rwfs, nil)
	server, _, err := nodefs.MountRoot(s.mountPoint, nfs.Root(), nil)
	s.server = server
//	s.server.SetDebug(true)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	log.Printf("Setup test: Serve")
	go s.server.Serve()
	log.Printf("Setup test: Wait Mount")
	s.server.WaitMount()
	// on macos this seems necessary.  Wait mount seems insufficient
	rwfs.WaitForAccess()
	log.Printf("Setup test: done")
}

func (s *RwfsSuite) TearDownTest(c *C) {
	log.Printf("Teardown: start")

	for {
		err := s.server.Unmount()
		if err == nil {
			break
		}
		fmt.Printf("Got error unmounting: %s\n", err.Error())
		time.Sleep(1 * time.Second)
	}

	os.RemoveAll(s.mountPoint)
	os.RemoveAll(s.workDir)

	log.Printf("Teardown: done")
}
/*
func (s *RwfsSuite) TestReadFromFuseFile (c *C) {
	log.Printf("TestRead start")

	buffer := make([]byte, 100)

	file, err := os.Open(fmt.Sprintf("%s/x", s.mountPoint))
	c.Assert(err, IsNil)
	n, err := file.Read(buffer)
	c.Assert(n, Equals, 1)
	c.Assert(buffer[0], Equals, uint8('z'))
	file.Close()

	log.Printf("TestRead done")
}

func (s *RwfsSuite) TestMkdirRwfs (c *C) {
	log.Printf("TestRead start")

	err := os.Mkdir(fmt.Sprintf("%s/a", s.mountPoint), os.FileMode(0777))
	c.Assert(err, IsNil)

	fi, err := os.Stat(fmt.Sprintf("%s/a", s.mountPoint))
	c.Assert(err, IsNil)
	c.Assert(fi.IsDir(), Equals, true)

	err = os.Mkdir(fmt.Sprintf("%s/a/b", s.mountPoint), os.FileMode(0777))
	c.Assert(err, IsNil)

	err = os.Remove(fmt.Sprintf("%s/a/b", s.mountPoint))
	c.Assert(err, IsNil)

	err = os.Remove(fmt.Sprintf("%s/a", s.mountPoint))
	c.Assert(err, IsNil)

	log.Printf("TestRead done")
}
*/

func (s *RwfsSuite) TestWriteRead (c *C) {
	log.Printf("TestWriteRead start")
	fn := fmt.Sprintf("%s/file.txt", s.mountPoint)
	file, err := os.Create(fn)
	c.Assert(err, IsNil)

	n, err := file.WriteString("data")
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 4)

	c.Assert(file.Close(), IsNil)

	file, err = os.Open(fn)
	buffer := make([]byte, 100)
	n, err = file.Read(buffer)
	c.Assert(n, Equals, 4)
	c.Assert(string(buffer[:n]), Equals, "data")
	file.Close()

	log.Printf("TestWriteRead done")
}
