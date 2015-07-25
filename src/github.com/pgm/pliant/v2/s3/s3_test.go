package s3

import (
	"fmt"
	. "gopkg.in/check.v1"
	"testing"
	"github.com/pgm/pliant/v2"
)

type S3Suite struct{}

var _ = Suite(&S3Suite{})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }


func (s *S3Suite) TestSimpleS3LabelOps(c *C) {

	p := NewS3TagService("s3.amazonaws.com", "pliantdemo", "test/labels")

	var testkey v2.Key = ([32]byte{1,2,3,4})
	key := &testkey

	p.Put("tag", key)

	fetchedKey := p.Get("tag")
	c.Assert(fetchedKey.String(), Equals, key.String())
}

func (s *S3Suite) TestSimpleS3ChunkOps(c *C) {
	getDestFn := func() string {
		return "tempoutput"
	}

	p := NewS3ChunkService("s3.amazonaws.com", "pliantdemo", "test/labels", getDestFn)

	var testkey v2.Key = ([32]byte{1,2,3,4})

	key := &testkey
	resourceContent := []byte("A")
	resource := v2.NewMemResource(resourceContent)
	p.Put(key, resource)

	fetchedResource := p.Get(key)
	c.Assert(fetchedResource.AsBytes(), DeepEquals, resourceContent)
}

/*
type S3Parameters struct {
	EndPoint string
	Bucket string
	keys s3gof3r.Keys
	getDestFn AllocTempDestFn
}
*/
