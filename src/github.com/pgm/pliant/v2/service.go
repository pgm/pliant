package v2

import ( "net"
	"os"
	"github.com/golang/protobuf/proto"
	"fmt"
	"net/rpc"
)

const MAX_COMMAND_PACKET = 1024*50

func handleConnection(connection *net.UnixConn, atomic Atomic) {
	defer connection.Close()


	var buf [MAX_COMMAND_PACKET]byte

	for {
		n, err := connection.Read(buf[:])
		if err != nil {
			panic(err)
		}

		msg := &Request{}
		var b []byte = buf[:n]
		err = proto.Unmarshal(b, msg)
		if err != nil {
			panic(err)
		}

		var response interface{};
		switch msg.GetType() {
		case Request_GET_KEY:
			key, err := ac.GetKey(msg.GetGetKey().GetPath())
			response = &GetKeyResp{IsSuccess: err == nil, Key: key}
		case Request_GET_LOCAL_PATH:
			path, err := ac.GetLocalPath(msg.GetGetLocalPath().GetPath())
			response = &GetLocalPathResp{IsSuccess: err == nil, Path: path}
		case Request_PUT_LOCAL_PATH:
			err = ac.PutLocalPath(msg.GetPutLocalPath().GetPath())
			response = &SimpleResp{IsSuccess: err == nil}
		case Request_LINK:
			l := msg.GetLink()
			err = ac.Link(l.GetPath(), l.GetKey(), l.GetIsDir())
			response = &SimpleResp{IsSuccess: err == nil}
		case Request_UNLINK:
			l := msg.GetUnlink()
			err = ac.Unlink(l.GetPath())
			response = &SimpleResp{IsSuccess: err == nil}
		}

		responseBuffer, err := proto.Marshal(response)
		if err != nil {
			panic(fmt.Sprintf("Couldn't marshal metadata object: %s", err))
		}
		n, err = connection.Write(responseBuffer)
		if n != len(responseBuffer) {
			panic("Did not write full buffer")
		}
		if err != nil {
			panic(err.Error())
		}
	}
}

func StartServer(bindAddr string, atomic Atomic) {
	ac := AtomicClient{atomic: atomic}

	server := rpc.NewServer()
	server.Register(ac)
	l, err := net.ListenUnix("unix",  &net.UnixAddr{bindAddr, "unix"})
	if err != nil {
		panic(err)
	}
	defer os.Remove(bindAddr)

	server.Accept(l)
}
