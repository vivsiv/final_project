package arith

import (
	"os"
	"log"
	"testing"
	"net/rpc"
)

var (
	server_ports = [2]int{8080, 8081}
	clients []*rpc.Client
)

func TestOne(t *testing.T) {
	args := &Args{}
	args.A = 7
	args.B = 8

	var reply int
	err := clients[0].Call("Arith.Multiply", args, &reply)
	if err != nil {
		log.Fatalf("Call Error: %s", err)
	}

	log.Printf("Multiply %d, %d = %d", args.A, args.B, reply)

	if reply != (args.A * args.B) {
		t.Errorf("Multiply(%d,%d) should be %d, got: %d", args.A, args.B, args.A * args.B, reply)
	}	
}

func TestTwo(t *testing.T){
	args := &Args{}
	args.A = 7
	args.B = 8

	var replyOne int
	err := clients[0].Call("Arith.Multiply", args, &replyOne)
	if err != nil {
		log.Fatalf("Call Error: %s", err)
	}

	log.Printf("Multiply %d, %d = %d", args.A, args.B, replyOne)

	if replyOne != (args.A * args.B) {
		t.Errorf("Multiply(%d,%d) should be %d, got: %d", args.A, args.B, args.A * args.B, replyOne)
	}

	replyTwo := Quotient{}
	err = clients[1].Call("Arith.Divide", args, &replyTwo)
	if err != nil {
		log.Fatalf("Call Error: %s", err)
	}

	log.Printf("Divide %d, %d = (q:%d,r:%d)", args.A, args.B, replyTwo.Quo, replyTwo.Rem)

	if (replyTwo.Quo != (args.A / args.B)) || (replyTwo.Rem != (args.A % args.B)) {
		t.Errorf("Multiply(%d,%d) should be (q:%d,r:%d), got: (q:%d,r:%d)", args.A, args.B, args.A / args.B, args.A % args.B, replyTwo.Quo, replyTwo.Rem)
	}
}

func TestMain(m *testing.M){
	clients = make([]*rpc.Client, len(server_ports))
	for i := 0; i < len(server_ports); i++ {
		MakeServer(server_ports[i])
		clients[i] = MakeClient(server_ports[i])
	}
	

	retCode := m.Run()
	os.Exit(retCode)
}