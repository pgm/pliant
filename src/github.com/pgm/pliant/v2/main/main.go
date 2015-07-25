package main

import (
	"os"
	"github.com/codegangsta/cli"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/s3"
	"net/rpc"
	"fmt"
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
	if(err != nil) {
		panic(err.Error())
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "pliant"
	app.Usage = "pliant client"
	app.Commands = []cli.Command{
		{
			Name:      "key",
			Usage:     "print the key for a given path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var key string
				panicIfError( ac.Call("AtomicClient.GetKey", c.Args().First(), &key) )
				println(key)
			},
		},
		{
			Name:      "mkdir",
			Usage:     "make an empty directory",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var key string
				panicIfError( ac.Call("AtomicClient.MakeDir", c.Args().First(), &key) )
				println(key)
			},
		},
		{
			Name:      "start",
			Usage:     "starts server",
			Action: func(c *cli.Context) {
				if _, err := os.Stat(SERVER_BINDING); err == nil {
					os.Remove(SERVER_BINDING)
				}

//				chunkService := v2.NewMemChunkService()
//				tags := v2.NewMemTagService()

				cache,_ := v2.NewFilesystemCacheDB("cache")
				tags := s3.NewS3TagService("s3.amazonaws.com", "pliantdemo", "labels")
				chunkService := s3.NewS3ChunkService("s3.amazonaws.com", "pliantdemo", "chunks", cache.AllocateTempFilename)
				chunks := v2.NewChunkCache(chunkService, cache)
				ds := v2.NewLeafDirService(chunks)
				as := v2.NewAtomicState(ds, chunks, cache, tags)
				panicIfError(v2.StartServer(SERVER_BINDING, as))
			},
		},
		{
			Name:      "link",
			Usage:     "link the given key into the specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				key := c.Args().Get(0)
				path := c.Args().Get(1)
				isDir := true

				panicIfError( ac.Call("AtomicClient.Link", &v2.LinkArgs{Key: key, Path: path, IsDir: isDir}, &result) )

				println(result)
			},
		},
		{
			Name:      "unlink",
			Usage:     "unlink remove specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError( ac.Call("AtomicClient.Unlink", c.Args().First(), &result) )

				println(result)
			},
		},
		{
			Name:      "local",
			Usage:     "Get local path to specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError(ac.Call("AtomicClient.GetLocalPath", c.Args().First(), &result))

				println(result)
			},
		},
		{
			Name:      "put",
			Usage:     "put local file into specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result string

				panicIfError(ac.Call("AtomicClient.PutLocalPath", &v2.PutLocalPathArgs{LocalPath: c.Args().Get(0), DestPath: c.Args().Get(1)}, &result))

				println(result)
			},
		},
		{
			Name:      "ls",
			Usage:     "list files at specified directory",
			Action: func(c *cli.Context) {
				ac := connectToServer()
				var result []v2.ListFilesRecord

				panicIfError(ac.Call("AtomicClient.ListFiles", c.Args().First(), &result))

				for _, rec := range(result) {
					fmt.Printf("%s\n", rec.Name)
				}
			},
		},
		{
			Name: "push",
			Usage: "push source tag ",
			Action: func(c *cli.Context) {
				ac := connectToServer()

				var result string

				panicIfError(ac.Call("AtomicClient.Push", &v2.PushArgs{Source: c.Args().Get(0), Tag: c.Args().Get(1)}, &result));
			},
		},
		{
			Name: "pull",
			Usage: "pull tag destination",
			Action: func(c *cli.Context) {
				ac := connectToServer()

				var result string

				panicIfError(ac.Call("AtomicClient.Pull", &v2.PullArgs{Tag: c.Args().Get(0), Destination: c.Args().Get(1)}, &result))
			},
		},
	}

	app.Run(os.Args)
}
