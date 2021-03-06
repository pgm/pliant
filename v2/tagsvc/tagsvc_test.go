package tagsvc

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pgm/pliant/v2"
	. "gopkg.in/check.v1"
)

type TagSvcSuite struct {
	tempfile string
}

var _ = Suite(&TagSvcSuite{})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }

func (s *TagSvcSuite) TearDownTest(c *C) {
	if s.tempfile != "" {
		os.Remove(s.tempfile)
		s.tempfile = ""
	}
}

func (s *TagSvcSuite) TestLeases(c *C) {
	tempfp, _ := ioutil.TempFile("", "tagsvc_test")
	s.tempfile = tempfp.Name()

	key1 := v2.Key{1}
	key2 := v2.Key{2}
	key3 := v2.Key{3}

	root := NewRoots(s.tempfile)
	c.Assert(len(root.GetRoots()), Equals, 0)
	root.AddLease(100, &key1)
	c.Assert(len(root.GetRoots()), Equals, 1)
	root.AddLease(101, &key2)
	c.Assert(len(root.GetRoots()), Equals, 2)
	root.AddLease(102, &key3)

	c.Assert(len(root.GetRoots()), Equals, 3)

	root.Expire(100)
	c.Assert(len(root.GetRoots()), Equals, 3)
	root.Expire(101)
	c.Assert(len(root.GetRoots()), Equals, 2)
	root.Expire(103)
	c.Assert(len(root.GetRoots()), Equals, 0)
}

func (s *TagSvcSuite) TestSetRoot(c *C) {
	tempfp, _ := ioutil.TempFile("", "tagsvc_test")
	s.tempfile = tempfp.Name()

	key1 := v2.Key{1}
	key2 := v2.Key{2}
	key3 := v2.Key{3}

	root := NewRoots(s.tempfile)
	root.Set("1", &key1)
	root.Set("2", &key2)
	c.Assert(len(root.GetRoots()), Equals, 2)

	root.Set("2", &key3)
	c.Assert(len(root.GetRoots()), Equals, 2)

	root.Set("1", nil)
	c.Assert(len(root.GetRoots()), Equals, 1)
}

func (s *TagSvcSuite) TestSimpleGC(c *C) {
	tempfp, _ := ioutil.TempFile("", "tagsvc_test")
	s.tempfile = tempfp.Name()

	fileKey1 := v2.Key{10}
	fileKey2 := v2.Key{11}
	fileKey3 := v2.Key{12}

	root := NewRoots(s.tempfile)
	count := 0
	countPtr := &count
	chunks := v2.NewMemChunkService()
	chunks.Put(&fileKey1, v2.NewMemResource(make([]byte, 1)))
	chunks.Put(&fileKey2, v2.NewMemResource(make([]byte, 1)))
	chunks.Put(&fileKey3, v2.NewMemResource(make([]byte, 1)))
	dirService := v2.NewLeafDirService(chunks)
	dir := dirService.GetDirectory(v2.EMPTY_DIR_KEY)
	dirKey, _, _ := dir.Put("a", &v2.FileMetadata{Size: proto.Int64(1), Key: fileKey1.AsBytes(), IsDir: proto.Bool(false)})
	root.Set("1", dirKey)

	fmt.Printf("GC\n")
	root.GC(dirService, chunks, func(key *v2.Key) {
		fmt.Printf("free %s\n", key.String())
		*countPtr += 1
	})

	c.Assert(*countPtr, Equals, 2)
}

func (s *TagSvcSuite) TestClientServer(c *C) {
	tempfp, _ := ioutil.TempFile("", "tagsvc_test")
	s.tempfile = tempfp.Name()

	config := &Config{
		AccessKeyId:     "access",
		SecretAccessKey: "secret",
		Endpoint:        "http://endpoint",
		Bucket:          "bucket",
		MasterPort:      0,
		Prefix:          "prefix",
		PersistPath:     s.tempfile,
		AuthSecret:      "x",
	}

	l, err := StartServer(config)
	c.Assert(err, IsNil)

	client := NewClient(l.Addr().String(), []byte("x"))
	vconfig, err := client.GetConfig()
	c.Assert(err, IsNil)
	c.Assert(vconfig, DeepEquals, config)

	key1 := v2.Key{10}
	err2 := client.AddLease(uint64(100), &key1)
	c.Assert(err2, IsNil)

	key2 := v2.Key{10}
	tagSvc := NewTagService(client)
	tagSvc.Put("label", &key2)
	vkey, _ := tagSvc.Get("label")

	c.Assert(vkey, DeepEquals, &key2)

	k, _ := tagSvc.Get("label2")
	c.Assert(k, IsNil)

	l.Close()
}
