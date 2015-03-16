package main

import (
	"flag"
	"log"
	"bytes"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/pgm/pliant/low"
	"github.com/pgm/pliant/fusebinding"
)



func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}

	chunks := low.NewMemChunkService()
	labels := low.NewMemLabelService()

	rawFs := low.NewRawFilesystem(chunks)
	fs := low.NewFilesystem(labels, rawFs)

	fs.LabelEmptyDir("root")
	fs.WriteFile("root", "x", bytes.NewBufferString("z"))

//	chunks := fffs_go.NewMemChunkService()
//	labels := fffs_go.NewMemLabelService()
//	rawFs := fffs_go.NewRawFilesystem(chunks)
//	filesystem := NewFilesystem(labels, rawFs)

	workDir := "/tmp/transfs"

	nfs := pathfs.NewPathNodeFs(fusebinding.NewRwFs(workDir, fs, "root"), nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	server.SetDebug(true)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
