package tagsvc

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/pgm/pliant/v2"
	"os"
	//	"fmt"
)

type Log struct {
	w *os.File
}

func (log *Log) write(buffer []byte) {
	paddedBuffer := bytes.NewBuffer(make([]byte, 0, len(buffer)+2))
	paddedBuffer.WriteByte(byte(len(buffer) >> 8))
	paddedBuffer.WriteByte(byte(len(buffer)))
	paddedBuffer.Write(buffer)
	log.w.Write(paddedBuffer.Bytes())
}

func (log *Log) read() []byte {
	var padding [2]byte
	n, err := log.w.Read(padding[:])
	//fmt.Printf("read -> %d (%d, %d)\n", n, int(padding[0]&0xff), int(padding[1]&0xff))
	if n == 0 {
		return nil
	}
	if err != nil || n != 2 {
		panic(err.Error())
	}
	length := int((int(padding[1]) & 0xff) | ((int(padding[0]) & 0xff) << 8))
	buffer := make([]byte, length, length)
	n, err = log.w.Read(buffer)
	if err != nil {
		panic(err.Error())
	}
	if n != length {
		panic("failed full read")
	}

	//fmt.Printf("read returning buffer, len(buffer)=%d, length=%d\n", len(buffer), n)
	return buffer
}

func (log *Log) appendLabel(label string, key *v2.Key) {
	et := v2.RootLog_LABEL
	var keyBytes []byte
	if key == nil {
		keyBytes = nil
	} else {
		keyBytes = key.AsBytes()
	}
	buffer, err := proto.Marshal(&v2.RootLog{EntryType: &et, Name: proto.String(label), Key: keyBytes})
	if err != nil {
		panic(err.Error())
	}
	log.write(buffer)
}

func (log *Log) Close() {
	log.w.Close()
}

func (log *Log) appendLease(key *v2.Key, timestamp uint64) {
	et := v2.RootLog_LEASE
	buffer, err := proto.Marshal(&v2.RootLog{EntryType: &et, Key: key.AsBytes(), Expiry: proto.Uint64(timestamp)})
	if err != nil {
		panic(err.Error())
	}
	log.write(buffer)
}

func OpenLog(filename string, replayLabel func(label string, key *v2.Key), replayLease func(key *v2.Key, timestamp uint64)) *Log {
	w, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0700)
	if err != nil {
		panic(err.Error())
	}

	log := &Log{w: w}

	for {
		buffer := log.read()
		//fmt.Printf("Read() -> len %d\n", len(buffer))
		if len(buffer) == 0 {
			break
		}

		var entry v2.RootLog
		proto.Unmarshal(buffer, &entry)

		if entry.GetEntryType() == v2.RootLog_LEASE {
			replayLease(v2.KeyFromBytes(entry.GetKey()), entry.GetExpiry())
		} else if entry.GetEntryType() == v2.RootLog_LABEL {
			var key *v2.Key
			if entry.Key == nil {
				key = nil
			} else {
				key = v2.KeyFromBytes(entry.GetKey())
			}
			replayLabel(entry.GetName(), key)
		} else {
			panic("invalid entry type")
		}
	}

	return log
}
