package v2

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"net/http"
	"code.google.com/p/go.net/websocket"
	"fmt"
)

func StartJsonRpc(ac *AtomicClient) {
	server := rpc.NewServer()
	server.Register(ac)
	//server.
//	rpc.Register(&ac)

	mux := http.NewServeMux()
	mux.Handle("/conn", websocket.Handler(func (ws *websocket.Conn) {
			fmt.Printf("server: %s\n", server)
			server.ServeCodec(jsonrpc.NewServerCodec(ws))
		}))
	httpserver := &http.Server{Addr: "localhost:7788", Handler: mux, }
	httpserver.ListenAndServe()
}

func StartServer(bindAddr string, atomic Atomic) error {
	ac := AtomicClient{atomic: atomic}

	go StartJsonRpc(&ac)

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
