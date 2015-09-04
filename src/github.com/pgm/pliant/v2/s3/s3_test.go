package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	ss "github.com/aws/aws-sdk-go/service/s3"
	"github.com/pgm/pliant/v2"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"testing"
)

type S3Suite struct {
	prefix string
	bucket string

	tempdir string
}

var _ = Suite(&S3Suite{prefix: "test/labels", bucket: "pliantdemo"})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }

func (s *S3Suite) TearDownTest(c *C) {
	os.RemoveAll(s.tempdir)
}

func (s *S3Suite) SetUpTest(c *C) {
	var err error
	s.tempdir, err = ioutil.TempDir("", "s3test")
	if err != nil {
		panic(err.Error())
	}

	s3c := ss.New(aws.DefaultConfig)
	listObjectsInputs := &ss.ListObjectsInput{Bucket: &s.bucket, Prefix: &s.prefix}
	s3c.ListObjectsPages(listObjectsInputs, func(page *ss.ListObjectsOutput, lastPage bool) (shouldContinue bool) {
		for _, obj := range page.Contents {
			fmt.Printf("Cleaning up test folder: Deleting %s\n", *obj.Key)
			s3c.DeleteObject(&ss.DeleteObjectInput{Bucket: &s.bucket, Key: obj.Key})
		}
		return true
	})
}

func (s *S3Suite) TestSimpleS3ChunkOps(c *C) {
	getDestFn := func() string {
		f, err := ioutil.TempFile(s.tempdir, "dest")
		if err != nil {
			panic(err.Error())
		}
		f.Close()
		return f.Name()
	}

	p := NewS3ChunkService("s3.amazonaws.com", s.bucket, s.prefix, getDestFn)

	it := p.Iterate()
	c.Assert(!it.HasNext(), Equals, true)

	var testkey v2.Key = ([32]byte{1, 2, 3, 4})

	key := &testkey
	resourceContent := []byte("A")
	resource := v2.NewMemResource(resourceContent)
	p.Put(key, resource)

	fetchedResource := p.Get(key)
	c.Assert(fetchedResource.AsBytes(), DeepEquals, resourceContent)

	it = p.Iterate()
	c.Assert(it.HasNext(), Equals, true)
	nextKey := it.Next()
	c.Assert(!it.HasNext(), Equals, true)
	c.Assert(nextKey, DeepEquals, key)
}
