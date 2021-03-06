package v2

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
//	"net/http"
//	"code.google.com/p/go.net/websocket"
//	"fmt"
)

func ServerAccept(server *rpc.Server, lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatal("rpc.Serve: accept:", err.Error()) // TODO(r): exit?
		}
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

func StartJsonRpc(bindAddr string, ac *AtomicClient) error {
	server := rpc.NewServer()
	server.Register(ac)

	l, err := net.ListenUnix("unix", &net.UnixAddr{bindAddr, "unix"})
	if err != nil {
		return err
	}
	defer os.Remove(bindAddr)

	log.Printf("Ready to accept requests via %s\n", bindAddr)

	ServerAccept(server, l)

	return nil
}

func StartServer(bindAddr string, jsonBindAddr string, atomic Atomic) error {
	ac := AtomicClient{atomic: atomic}

	if jsonBindAddr != "" {
		go StartJsonRpc(jsonBindAddr, &ac)
	}

	server := rpc.NewServer()
	server.Register(&ac)
	l, err := net.ListenUnix("unix", &net.UnixAddr{bindAddr, "unix"})
	if err != nil {
		return err
	}
	defer os.Remove(bindAddr)

	log.Printf("Ready to accept requests via %s\n", bindAddr)
	server.Accept(l)
	return nil
}
