package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/pgm/pliant/v2"
	"github.com/pgm/pliant/v2/startup"
	"github.com/pgm/pliant/v2/tagsvc"
	gcfg "gopkg.in/gcfg.v1"
)

const SERVER_BINDING string = "/tmp/pliantctl"

func connectToServer(addr string) *rpc.Client {
	client, err := rpc.Dial("unix", addr)
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

func expectArgs(c *cli.Context, hasOptionalAdditional bool, reqArgNames ...string) {
	// this is pretty backwards way to get a list of the args
	first := c.Args().First()
	argsTail := c.Args().Tail()
	var args []string
	if first == "" && argsTail == nil {
		args = nil
	} else {
		args = make([]string, len(argsTail)+1)
		args[0] = first
		for i, arg := range argsTail {
			args[i+1] = arg
		}
	}

	if !hasOptionalAdditional && len(args) != len(reqArgNames) {
		log.Fatalf("Expected arguments: %s", strings.Join(reqArgNames, ", "))
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "pliant"
	app.Usage = "pliant client"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "addr", Value: SERVER_BINDING, Usage: "The path to bind for communication"},
		&cli.StringFlag{Name: "jsonaddr", Value: "", Usage: "The path to bind for communication"},
	}
	app.Commands = []cli.Command{
		{
			Name:  "key",
			Usage: "print the key for a given path",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))
				var key string
				expectArgs(c, false, "path")
				path := c.Args().First()
				panicIfError(ac.Call("AtomicClient.GetKey", path, &key))
				println(key)
			},
		},
		{
			Name:  "mkdir",
			Usage: "make an empty directory",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))
				var key string
				expectArgs(c, false, "path")
				path := c.Args().First()
				panicIfError(ac.Call("AtomicClient.MakeDir", path, &key))
				println(key)
			},
		},
		{
			Name:  "minion",
			Usage: "starts master service",
			Action: func(c *cli.Context) {
				expectArgs(c, false, "configFile")
				filename := c.Args().Get(0)
				jsonBindAddr := c.GlobalString("jsonaddr")

				cfg := struct {
					Minion struct {
						MasterAddress        string
						AuthSecret           string
						CachePath            string
						PliantServiceAddress string
					}
				}{}

				fd, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Could not open %s", filename)
				}
				err = gcfg.ReadInto(&cfg, fd)
				if err != nil {
					log.Fatalf("Failed to parse %s: %s", filename, err)
				}
				fd.Close()

				//bindAddr := c.GlobalString("addr")
				bindAddr := cfg.Minion.PliantServiceAddress

				completed := startup.StartLocalService(cfg.Minion.PliantServiceAddress, cfg.Minion.AuthSecret, cfg.Minion.PliantServiceAddress, cfg.Minion.CachePath, bindAddr, jsonBindAddr)
				<-completed
			},
		},
		{
			Name:  "root",
			Usage: "starts root service",
			Action: func(c *cli.Context) {
				cfg := struct {
					S3 struct {
						AccessKeyId     string
						SecretAccessKey string
						Endpoint        string
						Bucket          string
						Prefix          string
					}
					Settings struct {
						Port        int
						PersistPath string
						AuthSecret  string
					}
				}{}

				expectArgs(c, false, "configFile")
				filename := c.Args().Get(0)
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
					Endpoint:        cfg.S3.Endpoint,
					Bucket:          cfg.S3.Bucket,
					Prefix:          cfg.S3.Prefix,
					MasterPort:      cfg.Settings.Port,
					PersistPath:     cfg.Settings.PersistPath,
					AuthSecret:      cfg.Settings.AuthSecret}
				_, err = tagsvc.StartServer(config)
				if err != nil {
					log.Fatalf("StartServer failed %s", err)
				}
				log.Printf("StartServer completed")
				select {}
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
				ac := connectToServer(c.GlobalString("addr"))
				var result string

				expectArgs(c, false, "key", "path")
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
				ac := connectToServer(c.GlobalString("addr"))
				var result string

				expectArgs(c, false, "path")
				path := c.Args().Get(0)

				panicIfError(ac.Call("AtomicClient.Unlink", path, &result))

				println(result)
			},
		},
		{
			Name:  "local",
			Usage: "Get local path to specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))
				var result string

				expectArgs(c, false, "path")
				path := c.Args().Get(0)

				panicIfError(ac.Call("AtomicClient.GetLocalPath", path, &result))

				println(result)
			},
		},
		{
			Name:  "put",
			Usage: "put local file into specified path",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))
				var result string

				expectArgs(c, false, "localpath", "path")
				localpath := c.Args().Get(0)
				remotepath := c.Args().Get(1)

				absLocalPath, err := filepath.Abs(localpath)
				if err != nil {
					panic(err.Error())
				}

				panicIfError(ac.Call("AtomicClient.PutLocalPath", &v2.PutLocalPathArgs{LocalPath: absLocalPath, DestPath: remotepath}, &result))

				println(result)
			},
		},
		{
			Name:  "ls",
			Usage: "list files at specified directory",
			Flags: []cli.Flag{cli.BoolFlag{Name: "l", Usage: "if set, will display details.  Otherwise only the names are displayed"}},
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))
				var result []v2.ListFilesRecord

				expectArgs(c, false, "path")
				path := c.Args().Get(0)

				isLong := c.Bool("l")

				panicIfError(ac.Call("AtomicClient.ListFiles", path, &result))

				for _, rec := range result {
					if !isLong {
						fmt.Printf("%s\n", rec.Name)
					} else {
						var prefix string
						if rec.IsDir {
							prefix = "d"
						} else {
							prefix = "-"
						}
						creationTime := time.Unix(rec.CreationTime, 0)
						fmt.Printf("%s % 12d  % 12d  %s  %s\n", prefix, rec.Size, rec.TotalSize, creationTime.Local().Format(time.UnixDate), rec.Name)
					}
				}
			},
		},
		{
			Name:  "push",
			Usage: "push source tag ",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))

				var result string

				expectArgs(c, false, "path", "label")
				path := c.Args().Get(0)
				label := c.Args().Get(1)

				panicIfError(ac.Call("AtomicClient.Push", &v2.PushArgs{Source: path, Tag: label}, &result))
			},
		},
		{
			Name:  "pull",
			Usage: "pull tag destination",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))

				var result string
				expectArgs(c, false, "label", "path")
				path := c.Args().Get(1)
				label := c.Args().Get(0)

				panicIfError(ac.Call("AtomicClient.Pull", &v2.PullArgs{Tag: label, Destination: path}, &result))
			},
		},
		{
			Name:  "roots",
			Usage: "list roots",
			Action: func(c *cli.Context) {
				ac := connectToServer(c.GlobalString("addr"))

				var prefix string = ""
				var result []v2.ListRootsRecord

				panicIfError(ac.Call("AtomicClient.ListRoots", &prefix, &result))

				for _, r := range result {
					fmt.Printf("%s\t%s\n", r.Name, r.Key)
				}
			},
		},
	}

	app.Run(os.Args)
}
