package s3

import (
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"os"
	"bytes"
	"github.com/pgm/pliant/v2"
	"net/http"
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
}

type S3TagService struct {
	S3Parameters
}

func NewS3TagService(endpoint string, bucket string, prefix string) *S3TagService {
	keys, err := s3gof3r.EnvKeys()
	if err != nil {
		panic(err.Error())
	}
	p := &S3TagService{}
	p.EndPoint = endpoint
	p.Bucket = bucket
	p.Keys = keys
	p.Prefix = prefix
	return p
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

	return p
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


func (c *S3TagService) Put(name string, key *v2.Key) {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig
	s3 := s3gof3r.New(c.EndPoint, c.Keys)
	b := s3.Bucket(c.Bucket)

	r := bytes.NewBuffer(key.AsBytes())

	header := make(http.Header)
	path := c.Prefix + "/" + name
	w, err := b.PutWriter(path, header, conf)
	if err != nil {
		panic(err.Error())
	}
	defer w.Close()

	if _, err = io.Copy(w, r); err != nil {
		panic(err.Error())
	}
}

func (c *S3TagService) Get(name string) *v2.Key {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig
	s3 := s3gof3r.New(c.EndPoint, c.Keys)
	b := s3.Bucket(c.Bucket)

	w := bytes.NewBuffer(make([]byte, 0))

	path := c.Prefix + "/" + name
	r, _, err := b.GetReader(path, conf)
	if err != nil {
		panic(err.Error())
	}
	defer r.Close()

	if _, err = io.Copy(w, r); err != nil {
		panic("Error Copying")
	}

	return v2.KeyFromBytes(w.Bytes())
}

