package arith

import (
	"fmt"
	"log"
	"net/rpc"
)

func MakeClient(portNo int) *rpc.Client {
	addr := fmt.Sprintf(":%d", portNo)
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatalf("Dial Error: %s", err)
	}

	return client
}