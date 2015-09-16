package tagsvc

import (
	"bytes"
	"fmt"
	"github.com/pgm/pliant/v2"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"os"
)

type LogSuite struct {
	tempfile string
}

var _ = Suite(&LogSuite{tempfile: ""})
var _ = fmt.Sprintf("hello!")

//func Test(t *testing.T) { TestingT(t) }

func (s *LogSuite) TearDownTest(c *C) {
	if s.tempfile != "" {
		os.Remove(s.tempfile)
		s.tempfile = ""
	}
}

func (s *LogSuite) TestLog(c *C) {
	tempfp, _ := ioutil.TempFile("", "log_test")
	s.tempfile = tempfp.Name()
	logfile := s.tempfile

	key1 := v2.Key{1}
	key2 := v2.Key{2}

	keyToStr := func(key *v2.Key) string {
		if key == nil {
			return "nil"
		} else if *key == key1 {
			return "k1"
		} else if *key == key2 {
			return "k2"
		} else {
			panic("bad key?")
		}
	}

	buffer := bytes.NewBuffer(nil)
	replayLabels := func(label string, key *v2.Key) {
		buffer.WriteString(fmt.Sprintf("label(%s,%s);", label, keyToStr(key)))
	}

	replayLeases := func(key *v2.Key, timestamp uint64) {
		buffer.WriteString(fmt.Sprintf("lease(%s,%d);", keyToStr(key), timestamp))
	}

	log1 := OpenLog(logfile, replayLabels, replayLeases)
	c.Assert(string(buffer.Bytes()), Equals, "")
	log1.appendLabel("a", &key1)
	log1.appendLabel("a", &key2)
	log1.appendLease(&key1, uint64(10))
	log1.appendLabel("a", nil)
	log1.Close()

	buffer.Reset()
	log2 := OpenLog(logfile, replayLabels, replayLeases)
	c.Assert(string(buffer.Bytes()), Equals, "label(a,k1);label(a,k2);lease(k1,10);label(a,nil);")
	log2.Close()
}
