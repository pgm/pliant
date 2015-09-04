package v2

import (
	"log"
	"net"
	"net/rpc"
	"os"
)

func StartServer(bindAddr string, atomic Atomic) error {
	ac := AtomicClient{atomic: atomic}

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
