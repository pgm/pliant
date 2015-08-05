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

func StartServer(bindAddr string, config *Config) error {
	ac := Master{}
	rpc.Register(ac)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d",config.MasterPort.String()))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

//	go http.Serve(l, nil)
//	server := rpc.NewServer()
//	server.Register(&ac)
//	l, err := net.ListenUnix("unix", &net.UnixAddr{bindAddr, "unix"})
//	if err != nil {
//		return err
//	}
//	defer os.Remove(bindAddr)
//
//	log.Printf("Ready to accept requests via %s\n", bindAddr)
//	server.Accept(l)
//	return nil
}


