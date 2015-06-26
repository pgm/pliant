package v2

import ( "net"
	"os"
	"net/rpc"
)


func StartServer(bindAddr string, atomic Atomic) error {
	ac := AtomicClient{atomic: atomic}

	server := rpc.NewServer()
	server.Register(&ac)
	l, err := net.ListenUnix("unix",  &net.UnixAddr{bindAddr, "unix"})
	if err != nil {
		return err;
	}
	defer os.Remove(bindAddr)

	server.Accept(l)
	return nil
}
