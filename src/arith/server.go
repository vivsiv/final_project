package arith

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
)

func MakeServer(portNo int) {
	arith := new(Arith)

	//Registers arith object on the default server
	srv := rpc.NewServer()

	err := srv.Register(arith)
	if err != nil {
		log.Fatalf("Register Error: %s", err)
	}

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux

	//Registers an HTTP handler for RPCs on the default server
	srv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	http.DefaultServeMux = oldMux

	//Creates a listener
	addr := fmt.Sprintf(":%d", portNo)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen Error: %s", err)
	}

	//Accept connections
	go http.Serve(ln, mux)
}