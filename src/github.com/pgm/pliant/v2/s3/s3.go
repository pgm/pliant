package s3

import (
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"os"
//	"bytes"
	"github.com/pgm/pliant/v2"
	"net/http"

	ss "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"fmt"
)

type AllocTempDestFn func () string

type S3Parameters struct {
	EndPoint string
	Bucket string
	Keys s3gof3r.Keys
	GetDestFn AllocTempDestFn
	Prefix string
}

type S3ChunkService struct {
	S3Parameters
	DownloadDir string
	MaxFetchKeys int64
}

func NewS3ChunkService(endpoint string, bucket string, prefix string, getDestFn AllocTempDestFn ) *S3ChunkService {
	keys, err := s3gof3r.EnvKeys()
	if err != nil {
		panic(err.Error())
	}


	p := &S3ChunkService{}
	p.EndPoint = endpoint
	p.Bucket = bucket
	p.Keys = keys
	p.GetDestFn = getDestFn
	p.Prefix = prefix
	p.MaxFetchKeys = 2

	return p
}

func (c *S3ChunkService) Delete(key *v2.Key) {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig

	s3 := s3gof3r.New(c.EndPoint, c.Keys)
	b := s3.Bucket(c.Bucket)
	path := c.Prefix + "/" + key.String()
	err := b.Delete(path)
	if err != nil {
		panic("Error Delete")
	}
}


type S3KeyIterator struct {
	Bucket string
	Prefix string
	MaxFetchKeys int64
	S3C *ss.S3
	MorePages bool
	NextMarker string

	keyBatch []*v2.Key
	batchIndex int
}

func (c *S3KeyIterator) fetchNext(nextMarker *string) {
	delimiter := "/"
	p := &ss.ListObjectsInput{Bucket: &c.Bucket, Delimiter: &delimiter, Prefix: &c.Prefix, MaxKeys: &c.MaxFetchKeys, Marker: nextMarker}
	page, err := c.S3C.ListObjects(p)
	if(err != nil) {
		panic(err.Error())
	}
	c.MorePages = *page.IsTruncated
	if(c.MorePages) {
		c.NextMarker = *page.NextMarker
	}

	objects := page.Contents
	c.keyBatch = make([]*v2.Key, len(objects))
	for i, obj := range(objects) {
		fmt.Printf("obj=%s\n", obj)
		keyComponent := ((*obj.Key)[len(c.Prefix):])
		c.keyBatch[i] = v2.NewKey(keyComponent)
	}
	c.batchIndex = 0
}

/*
type Object struct {
	ETag *string `type:"string"`

	Key *string `type:"string"`

	LastModified *time.Time `type:"timestamp" timestampFormat:"iso8601"`

	Owner *Owner `type:"structure"`

	Size *int64 `type:"integer"`

	// The class of storage used to store the object.
	StorageClass *string `type:"string"`

	metadataObject `json:"-" xml:"-"`
}
*/

func (c *S3KeyIterator) HasNext() bool {
	return c.batchIndex < len(c.keyBatch)
}

func (c *S3KeyIterator) Next() *v2.Key {
	key := c.keyBatch[c.batchIndex]
	c.batchIndex++

	if (c.batchIndex >= len(c.keyBatch) && c.MorePages) {
		nextMarker := c.NextMarker
		c.fetchNext(&nextMarker)
	}

	return key
}

func (c *S3ChunkService) Iterate () v2.KeyIterator {
	s3c := ss.New(aws.DefaultConfig)
	it := &S3KeyIterator{Bucket: c.Bucket, Prefix: c.Prefix+"/", MaxFetchKeys: c.MaxFetchKeys, S3C: s3c}
	it.fetchNext(nil)
	return it
}


func (c *S3ChunkService) Get(key *v2.Key) v2.Resource {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig

	s3 := s3gof3r.New(c.EndPoint, c.Keys)
	b := s3.Bucket(c.Bucket)

	destFile := c.GetDestFn()
	w, err := os.Create(destFile)
	if err != nil {
		panic("Error Create")
	}
	defer w.Close()

	path := c.Prefix + "/" + key.String()
	r, _, err := b.GetReader(path, conf)
	if err != nil {
		panic("Error GetReader")
	}
	defer r.Close()

	if _, err = io.Copy(w, r); err != nil {
		panic("Error Copying")
	}

	resource, err := v2.NewFileResource(destFile)
	if err != nil {
		panic("Error creating resource")
	}

	return resource
}

func (c *S3ChunkService) Put(key *v2.Key, resource v2.Resource) {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig
	s3 := s3gof3r.New(c.EndPoint, c.Keys)
	b := s3.Bucket(c.Bucket)

	r := resource.GetReader()
	if rCloser, ok := r.(io.Closer); ok {
		defer rCloser.Close()
	}

	header := make(http.Header)
	path := c.Prefix + "/" + key.String()
	w, err := b.PutWriter(path, header, conf)
	if err != nil {
		panic(err.Error())
	}
	defer w.Close()

	if _, err = io.Copy(w, r); err != nil {
		panic("Error Copying")
	}
}

