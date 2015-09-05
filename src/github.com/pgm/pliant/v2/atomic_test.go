package v2

import (
	"fmt"
	. "gopkg.in/check.v1"
	//	"testing"

	"os"
)

type AtomicSuite struct{
}

var _ = Suite(&AtomicSuite{})
var _ = fmt.Sprintf("hello!")

func fetchNamesFromIter(it Iterator) []string {
	names := make([]string, 0)
	for it.HasNext() {
		name, _ := it.Next()
		names = append(names, name)
	}
	return names
}

func (*AtomicSuite) TestChunkCache(c *C) {
	cache, _ := NewFilesystemCacheDB(c.MkDir())
	chunks := NewChunkCache(NewMemChunkService(), cache)

	aRes := NewMemResource([]byte("A"))
	bRes := NewMemResource([]byte("B"))

	chunks.Put(&Key{100}, aRes)
	chunks.Put(&Key{101}, bRes)

	fetched := chunks.Get(&Key{100})
	c.Assert(fetched.AsBytes(), DeepEquals, []byte("A"))
	fetched = chunks.Get(&Key{101})
	c.Assert(fetched.AsBytes(), DeepEquals, []byte("B"))
}

func (s *AtomicSuite) TestAtomicDirOps(c *C) {
	cache, _ := NewFilesystemCacheDB(c.MkDir())
	chunks := NewChunkCache(NewMemChunkService(), cache)
	ds := NewLeafDirService(chunks)
	tags := NewMemTagService()
	as := NewAtomicState(ds, chunks, cache, tags)
	ac := &AtomicClient{atomic: as}

	defer func() {
		r := recover()
		if r != nil {
			fmt.Printf("Caught r %s", r)
			cache.Dump()
		}
		panic(r)
	}()

	var result string
	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "a", IsDir: true}, &result)
	it0, _ := as.GetDirectoryIterator(NewPath(""))

	e1 := [...]string{"a"}
	c.Assert(fetchNamesFromIter(it0), DeepEquals, e1[:])

	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "c", IsDir: true}, &result)
	it1, _ := as.GetDirectoryIterator(NewPath(""))

	e2 := [...]string{"a", "c"}
	c.Assert(fetchNamesFromIter(it1), DeepEquals, e2[:])

	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "b", IsDir: true}, &result)
	it4, _ := as.GetDirectoryIterator(NewPath(""))

	e4 := [...]string{"a", "b", "c"}
	c.Assert(fetchNamesFromIter(it4), DeepEquals, e4[:])

	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "a/c", IsDir: true}, &result)
	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "a/c/d", IsDir: true}, &result)
	it2, _ := as.GetDirectoryIterator(NewPath("a"))

	e3 := [...]string{"c"}
	c.Assert(fetchNamesFromIter(it2), DeepEquals, e3[:])
}

func (s *AtomicSuite) TestAtomicFileOps(c *C) {
	cache, _ := NewFilesystemCacheDB(c.MkDir())
	chunks := NewChunkCache(NewMemChunkService(), cache)
	ds := NewLeafDirService(chunks)
	tags := NewMemTagService()
	as := NewAtomicState(ds, chunks, cache, tags)
	ac := &AtomicClient{atomic: as}

	tempFile := "tmpfile"
	wfile, _ := os.Create(tempFile)
	wfile.WriteString("test")
	wfile.Close()

	var result string
	ac.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "a", IsDir: true}, &result)

	err := ac.PutLocalPath(&PutLocalPathArgs{LocalPath: tempFile, DestPath: "a/b"}, &result)
	c.Assert(err, Equals, nil)

	var finalFile string
	err = ac.GetLocalPath("a/b", &finalFile)
	c.Assert(err, Equals, nil)

	fmt.Printf("Got local file path: %s\n", finalFile)
	file, _ := os.Open(finalFile)
	b := make([]byte, 4)
	n, _ := file.Read(b)
	c.Assert(n, Equals, 4)
	c.Assert("test", Equals, string(b))
}

func (s *AtomicSuite) TestPush(c *C) {
	fmt.Printf("TestPush start\n")
	remoteChunks := NewMemChunkService()
	tags := NewMemTagService()

	cache1, _ := NewFilesystemCacheDB(c.MkDir())
	chunks1 := NewChunkCache(remoteChunks, cache1)
	ds1 := NewLeafDirService(chunks1)
	as1 := NewAtomicState(ds1, chunks1, cache1, tags)
	ac1 := &AtomicClient{atomic: as1}

	cache2, _ := NewFilesystemCacheDB(c.MkDir())
	chunks2 := NewChunkCache(remoteChunks, cache2)
	ds2 := NewLeafDirService(chunks2)
	as2 := NewAtomicState(ds2, chunks2, cache2, tags)
	ac2 := &AtomicClient{atomic: as2}

	tempFile := "tmpfile"
	wfile, _ := os.Create(tempFile)
	wfile.WriteString("test")
	wfile.Close()

	var result string
	ac1.Link(&LinkArgs{Key: EMPTY_DIR_KEY.String(), Path: "a", IsDir: true}, &result)

	err := ac1.PutLocalPath(&PutLocalPathArgs{LocalPath: tempFile, DestPath: "a/b"}, &result)
	c.Assert(err, Equals, nil)

	ac1.Push(&PushArgs{Source: "a", Tag: "tag"}, &result)
	remoteChunks.PrintDebug()
	ac2.Pull(&PullArgs{Tag: "tag", Destination: "z"}, &result)
	as2.DumpDebug()

	var finalFile string
	err = ac2.GetLocalPath("z/b", &finalFile)
	c.Assert(err, Equals, nil)

	fmt.Printf("Got local file path: %s\n", finalFile)
	file, _ := os.Open(finalFile)
	b := make([]byte, 4)
	n, _ := file.Read(b)
	c.Assert(n, Equals, 4)
	c.Assert("test", Equals, string(b))
}
