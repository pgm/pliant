package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"github.com/pgm/pliant/v2/tagsvc"
	"net/rpc"
	"os"
	"log"
	gcfg "gopkg.in/gcfg.v1"
)

const SERVER_BINDING string = "pliantctl"

func connectToServer() *rpc.Client {
	client, err := rpc.Dial("unix", SERVER_BINDING)
	if err != nil {
		panic(err.Error())
	}
	return client
}

func panicIfError(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "pliant"
	app.Usage = "pliant client"
	app.Commands = []cli.Command{
		{
			Name:  "key",
			Usage: "print the key for a given path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var key string
				panicIfError(ac.Call("AtomicClient.GetKey", c.Args().First(), &key))
				println(key)
			},
		},
		{
			Name:  "mkdir",
			Usage: "make an empty directory",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var key string
				panicIfError(ac.Call("AtomicClient.MakeDir", c.Args().First(), &key))
				println(key)
			},
		},
		{
			Name:  "minion",
			Usage: "starts master service",
			Action: func(c *cli.Context) {
				// contact the master and get the config
				masterAddress := c.Args().Get(0)
				tagsvcClient := tagsvc.NewClient(masterAddress)
				config, err := tagsvcClient.GetConfig()
				if err != nil {
					panic(err.Error())
				}

				if _, err := os.Stat(SERVER_BINDING); err == nil {
					os.Remove(SERVER_BINDING)
				}

				root := "cache"
				_, err = os.Stat(root)
				if os.IsNotExist(err) {
					os.MkdirAll(root, 0770)
				}

				db, err := v2.InitDb(root+"/db.bolt")
				if err != nil {
					panic(err.Error())
				}

				cache, _ := v2.NewFilesystemCacheDB(root, db)
				tags := tagsvc.NewTagService(tagsvcClient)
				chunkService := s3.NewS3ChunkService(config.AccessKeyId, config.SecretAccessKey, config.Endpoint, config.Bucket, config.Prefix, cache.AllocateTempFilename)
				chunks := v2.NewChunkCache(chunkService, cache)
				ds := v2.NewLeafDirService(chunks)
				as := v2.NewAtomicState(ds, chunks, cache, tags, v2.NewDbRootMap(db))
				panicIfError(v2.StartServer(SERVER_BINDING, as))
			},
		},
		{
			Name:  "root",
			Usage: "starts root service",
			Action: func(c *cli.Context) {
				cfg := struct {
						S3 struct {
							AccessKeyId string
							SecretAccessKey string
							Endpoint string
							Bucket string
							Prefix string
						}
						Settings struct {
							Port int
							PersistPath string
						}
					}{}

				filename := "config.ini"
				fd, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Could not open %s", filename)
				}
				err = gcfg.ReadInto(&cfg, fd)
				if err != nil {
					log.Fatalf("Failed to parse %s: %s", filename, err)
				}
				fd.Close()

				config := &tagsvc.Config{AccessKeyId: cfg.S3.AccessKeyId,
					SecretAccessKey: cfg.S3.SecretAccessKey,
					Endpoint: cfg.S3.Endpoint,
					Bucket: cfg.S3.Bucket,
					Prefix: cfg.S3.Prefix,
					MasterPort: cfg.Settings.Port,
					PersistPath: cfg.Settings.PersistPath}
				_, err = tagsvc.StartServer(config)
				if err != nil {
					log.Fatalf("StartServer failed %s", err)
				}
				log.Printf("StartServer completed")
				select{}
			},
		},
		{
			Name:  "gc",
			Usage: "Runs GC",
			//func gc(roots []*Key, chunks IterableChunkService) {
			Action: func(c *cli.Context) {
			},
		},
		{
			Name:  "link",
			Usage: "link the given key into the specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				key := c.Args().Get(0)
				path := c.Args().Get(1)
				isDir := true

				panicIfError(ac.Call("AtomicClient.Link", &v2.LinkArgs{Key: key, Path: path, IsDir: isDir}, &result))

				println(result)
			},
		},
		{
			Name:  "unlink",
			Usage: "unlink remove specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError(ac.Call("AtomicClient.Unlink", c.Args().First(), &result))

				println(result)
			},
		},
		{
			Name:  "local",
			Usage: "Get local path to specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError(ac.Call("AtomicClient.GetLocalPath", c.Args().First(), &result))

				println(result)
			},
		},
		{
			Name:  "put",
			Usage: "put local file into specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError(ac.Call("AtomicClient.PutLocalPath", &v2.PutLocalPathArgs{LocalPath: c.Args().Get(0), DestPath: c.Args().Get(1)}, &result))

				println(result)
			},
		},
		{
			Name:  "ls",
			Usage: "list files at specified directory",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result []v2.ListFilesRecord

				panicIfError(ac.Call("AtomicClient.ListFiles", c.Args().First(), &result))

				for _, rec := range result {
					fmt.Printf("%s\n", rec.Name)
				}
			},
		},
		{
			Name:  "push",
			Usage: "push source tag ",
			Action: func(c *cli.Context) {
				ac := connectToServer()

				var result string

				panicIfError(ac.Call("AtomicClient.Push", &v2.PushArgs{Source: c.Args().Get(0), Tag: c.Args().Get(1)}, &result))
			},
		},
		{
			Name:  "pull",
			Usage: "pull tag destination",
			Action: func(c *cli.Context) {
				ac := connectToServer()

				var result string

				panicIfError(ac.Call("AtomicClient.Pull", &v2.PullArgs{Tag: c.Args().Get(0), Destination: c.Args().Get(1)}, &result))
			},
		},
		{
			Name:  "roots",
			Usage: "list roots",
			Action: func(c *cli.Context) {
				ac := connectToServer()

				var prefix string = ""
				var result []v2.ListRootsRecord

				panicIfError(ac.Call("AtomicClient.ListRoots", &prefix, &result))

				for _, r := range(result) {
					fmt.Printf("%s\t%s\n", r.Name, r.Key)
				}
			},
		},
	}

	app.Run(os.Args)
}
