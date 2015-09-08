package tagsvc

import (
	"fmt"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"log"
	"net"
	//"net/http"
	"net/rpc"
	//	"strconv"
	"time"
	"errors"
	"bytes"
	"crypto/rand"
	"crypto/md5"
)

const CHALLENGE_SIZE int = 64
const GREETING string = "minion_v1\n"

func RandomChallenge() []byte {
	b := make([]byte, CHALLENGE_SIZE)
	rand.Read(b)
	return b
}

func ComputeResponse(secret []byte, client []byte, server []byte) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	buf.Write(secret)
	buf.Write(client)
	buf.Write(server)
	response := md5.Sum(buf.Bytes())

	return response[:]
}

var NO_SUCH_KEY error = errors.New("No such key")

type Config struct {
	AccessKeyId     string
	SecretAccessKey string
	Endpoint        string
	Bucket          string
	MasterPort      int
	Prefix          string
	PersistPath     string
	AuthSecret	 string
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

func (t *Master) GetAll(ignored *string, reply *[]NameAndKey) error {
	*reply = t.roots.GetNamedRoots()

	return nil
}

func (t *Master) AddLease(args *AddLeaseArgs, reply *bool) error {
	now := uint64(time.Now().Unix())
	t.roots.AddLease(args.Timeout+now, args.Key)

	*reply = true

	return nil
}

func (t *Master) GC(label *string, reply *v2.Key) error {
	panic("todo: update code to pass in bolt db ref")
	cache, _ := v2.NewFilesystemCacheDB("cache", nil)
	chunkService := s3.NewS3ChunkService(t.config.AccessKeyId, t.config.SecretAccessKey, t.config.Endpoint, t.config.Bucket, t.config.Prefix, cache.AllocateTempFilename)
	dirService := v2.NewLeafDirService(chunkService)
	t.roots.GC(dirService, chunkService, chunkService.Delete)

	return nil
}

func (t *Master) GetConfig(nothing *string, reply *Config) error {
	fmt.Printf("reply=%s\nconfig=%s\n", reply, t.config)
	*reply = *t.config

	return nil
}

/*	clientChallenge := RandomChallenge()

	conn.Write([]byte(GREETING))
	conn.Write(clientChallenge)

	serverChallenge := make([]byte, CHALLENGE_SIZE)
	n, err := conn.Read(serverChallenge)
	if n != CHALLENGE_SIZE {
		log.Fatalf("expecting challenge but read %d", n)
	}

	response := ComputeResponse([]byte(authSecret), clientChallenge, serverChallenge)
	conn.Write(response)
*/

func StartServer(config *Config) (net.Listener, error) {
	ac := &Master{config: config, roots: NewRoots(config.PersistPath)}
	rpc.Register(ac)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf("localhost:%d", config.MasterPort))
	if e != nil {
		log.Fatal("listen error:", e)
		return nil, e
	}
	go (func() {
	log.Printf("Serve starting")

	conn, err := l.Accept()

	serverChallenge := RandomChallenge()
	clientChallenge := make([]byte, CHALLENGE_SIZE)

	greetingBuffer := make([]byte, len([]byte(GREETING)))
	conn.Read(greetingBuffer)
	n, _ := conn.Read(clientChallenge)
	if n != CHALLENGE_SIZE {
		log.Fatalf("expecting challenge but read %d", n)
	}

	conn.Write(serverChallenge)

	expected := ComputeResponse([]byte(config.AuthSecret), clientChallenge, serverChallenge)
	response := make([]byte, len(expected))
	conn.Read(response)
	if bytes.Compare(expected, response) == 0 {
		fmt.Printf("Auth succeeded!\n")
		rpc.ServeConn(conn)
		if err != nil {
			log.Fatalf("serve failed %s", err)
		}
	} else {
		log.Fatalf("Auth failed")
	}

	})()

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

type NameAndKey struct {
	Name string
	Key *v2.Key
}

func (c *Client) GetAll() ([]NameAndKey, error) {
	var input = ""
	var result []NameAndKey
	err := c.client.Call("Master.GetAll", &input, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}


func (c *Client) Set(label string, key *v2.Key) error {
	err := c.client.Call("Master.Set", &SetArgs{label, key}, nil)
	return err
}

func (c *Client) AddLease(Timeout uint64, Key *v2.Key) error {
	err := c.client.Call("Master.AddLease", &AddLeaseArgs{Timeout, Key}, nil)
	return err
}

func NewClient(address string, authSecret []byte) *Client {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	clientChallenge := RandomChallenge()

	conn.Write([]byte(GREETING))
	conn.Write(clientChallenge)

	serverChallenge := make([]byte, CHALLENGE_SIZE)
	n, err := conn.Read(serverChallenge)
	if n != CHALLENGE_SIZE {
		log.Fatalf("expecting challenge but read %d", n)
	}

	response := ComputeResponse([]byte(authSecret), clientChallenge, serverChallenge)
	conn.Write(response)

	client := rpc.NewClient(conn)
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

func (t *TagService) ForEach(callback func (name string, key *v2.Key)) {
	result, err := t.client.GetAll()
	if err != nil {
		panic(err.Error())
	}
	for _, nameAndKey := range(result) {
		callback(nameAndKey.Name, nameAndKey.Key)
	}
}

func NewTagService(c *Client) v2.TagService {
	return &TagService{client: c}
}
