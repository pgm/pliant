package v2

import (
	"os"
	"github.com/codegangsta/cli"
)

func keyCommand(args [] string) {
	if len(args) != 1 {

	}
}

func linkCommand(args [] string) {
	panic("unimp")
}

func unlinkCommand(args [] string) {
	panic("unimp")
}

func putCommand(args[] string){
	panic("unimp")
}

func localCommand(args[] string){
	panic("unimp")
}

func lsCommand(args[] string){
	panic("unimp")
}

func main() {
	var ac AtomicClient

	app := cli.NewApp()
	app.Name = "pliant"
	app.Usage = "pliant client"
	app.Commands = []cli.Command{
		{
			Name:      "key",
			Usage:     "print the key for a given path",
			Action: func(c *cli.Context) {
				var key string
				ac.GetKey(c.Args().First(), &key)
				println(key)
			},
		},
		{
			Name:      "link",
			Usage:     "link the given key into the specified path",
			Action: func(c *cli.Context) {
				var result string

				key := c.Args().Get(0)
				path := c.Args().Get(1)
				isDir := true

				ac.Link(&LinkArgs{Key: key, Path: path, IsDir: isDir}, &result)
				println(result)
			},
		},
		{
			Name:      "unlink",
			Usage:     "unlink remove specified path",
			Action: func(c *cli.Context) {
				var result string

				ac.Unlink(c.Args().First(), &result)
				println(result)
			},
		},
		{
			Name:      "local",
			Usage:     "Get local path to specified path",
			Action: func(c *cli.Context) {
				var result string

				ac.GetLocalPath(c.Args().First(), &result)
				println(result)
			},
		},
		{
			Name:      "put",
			Usage:     "put local file into specified path",
			Action: func(c *cli.Context) {
				var result string

				ac.GetLocalPath(c.Args().First(), &result)
				println(result)
			},
		},
		{
			Name:      "ls",
			Usage:     "list files at specified directory",
			Action: func(c *cli.Context) {
				panic("unimp")
			},
		},
	}

	app.Run(os.Args)
}
