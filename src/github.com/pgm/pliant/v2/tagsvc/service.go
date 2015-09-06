package tagsvc

import (
	"fmt"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"log"
	"net"
	"net/http"
	"net/rpc"
	//	"strconv"
	"time"
	"errors"
)

var NO_SUCH_KEY error = errors.New("No such key")

type Config struct {
	AccessKeyId     string
	SecretAccessKey string
	Endpoint        string
	Bucket          string
	MasterPort      int
	Prefix          string
	PersistPath     string
}

type Master struct {
	roots  *Roots
	config *Config
}

type SetArgs struct {
	Label string
	Key   *v2.Key
}

type AddLeaseArgs struct {
	Timeout uint64
	Key     *v2.Key
}

func (t *Master) Set(args *SetArgs, reply *bool) error {
	t.roots.Set(args.Label, args.Key)

	*reply = true

	return nil
}

func (t *Master) Get(label *string, reply *v2.Key) error {
	replyPtr := t.roots.Get(*label)
	if replyPtr == nil {
		return NO_SUCH_KEY;
	}
	*reply = *replyPtr

	return nil
}

func (t *Master) AddLease(args *AddLeaseArgs, reply *bool) error {
	now := uint64(time.Now().Unix())
	t.roots.AddLease(args.Timeout+now, args.Key)

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
	fmt.Printf("reply=%s\nconfig=%s\n", reply, t.config)
	*reply = *t.config

	return nil
}

func StartServer(config *Config) (net.Listener, error) {
	ac := &Master{config: config, roots: NewRoots(config.PersistPath)}
	rpc.Register(ac)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf("localhost:%d", config.MasterPort))
	if e != nil {
		log.Fatal("listen error:", e)
		return nil, e
	}
	go http.Serve(l, nil)
	return l, nil
}

type Client struct {
	client *rpc.Client
}

func (c *Client) GetConfig() (*Config, error) {
	var config Config
	param := "nil"
	err := c.client.Call("Master.GetConfig", param, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (c *Client) Get(label string) (*v2.Key, error) {
	var key v2.Key
	err := c.client.Call("Master.Get", label, &key)
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (c *Client) Set(label string, key *v2.Key) error {
	err := c.client.Call("Master.Set", &SetArgs{label, key}, nil)
	return err
}

func (c *Client) AddLease(Timeout uint64, Key *v2.Key) error {
	err := c.client.Call("Master.AddLease", &AddLeaseArgs{Timeout, Key}, nil)
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
		if err.Error() == NO_SUCH_KEY.Error() {
			return nil;
		}
		panic(err.Error())
	}
	return key
}

func NewTagService(c *Client) v2.TagService {
	return &TagService{client: c}
}
