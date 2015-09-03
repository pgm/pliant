package tagsvc

import (
	"net"
	"net/http"
	"net/rpc"
//	"os"
	"time"
	"log"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"fmt"
	"strconv"
)

type Config struct {
	AccessKeyId string
	SecretAccessKey string
	Endpoint string
	Bucket string
	MasterPort int
	Prefix string
}

type Master struct {
	roots *Roots
	config *Config
}

type SetArgs struct {
	label string
	key *v2.Key
}

type AddLeaseArgs struct {
	Timeout uint64 ;
	Key *v2.Key
}

func (t *Master) Set(args *SetArgs, reply *bool) error {
	t.roots.Set(args.label, args.key)

	*reply = true;

	return nil
}

func (t *Master) Get(label *string, reply *v2.Key) error {
	*reply = *t.roots.Get(*label)

	return nil
}

func (t *Master) AddLease(args *AddLeaseArgs, reply *bool) error {
	now := uint64(time.Now().Unix())
	t.roots.AddLease(args.Timeout + now, args.Key)

	*reply = true

	return nil
}

func (t *Master) GC(label *string, reply *v2.Key) error {
	cache, _ := v2.NewFilesystemCacheDB("cache")
	chunkService := s3.NewS3ChunkService(t.config.Endpoint, t.config.Bucket, t.config.Prefix, cache.AllocateTempFilename)
	dirService := v2.NewLeafDirService(chunkService)
	t.roots.GC(dirService, chunkService, chunkService.Delete)

	return nil
}


func (t *Master) GetConfig(nothing *string, reply *Config) error {
	*reply = *t.config

	return nil
}

func StartServer(config *Config) error {
	ac := Master{}
	rpc.Register(ac)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", strconv.Itoa(config.MasterPort)))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	return nil
}

type Client struct {
	client *rpc.Client
}

func (c *Client) GetConfig() (*Config, error) {
	var config Config;
	err := c.client.Call("GetConfig", nil, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (c *Client) Get(label string) (*v2.Key, error) {
	var key v2.Key
	err := c.client.Call("Get", label, &key)
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (c *Client) Set(label string, key *v2.Key) error {
	err := c.client.Call("Set", &SetArgs{label, key}, nil)
	return err
}

func (c *Client) AddLease(Timeout uint64, Key *v2.Key) error {
	err := c.client.Call("Set", &AddLeaseArgs{Timeout, Key}, nil)
	return err
}

func NewClient(address string) *Client {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return &Client{client: client}
}

type TagService struct {
	client *Client
}

func (t *TagService) Put(name string, key *v2.Key) {
	err := t.client.Set(name, key)
	if err != nil {
		panic(err.Error())
	}
}

func (t *TagService) Get(name string) *v2.Key {
	key, err := t.client.Get(name)
	if err != nil {
		panic(err.Error())
	}
	return key
}

func NewTagService(c *Client) v2.TagService {
	return &TagService{client : c}
}

