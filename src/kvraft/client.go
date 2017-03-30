package kvraft

import (
	"net/rpc"
	"crypto/rand"
	"math/big"
	"fmt"
)




type Clerk struct {
	servers     []*rpc.Client
	me          int64 //this clerks id
	lastLeader  int
	lastRequest int
	DEBUG       bool
	// You will have to modify this struct.
}

func (ck *Clerk) toString() string {
	return fmt.Sprintf("<Client:%d>", ck.me)
}

func (ck *Clerk) logDebug(msg string){
	if ck.DEBUG { fmt.Printf("%s:%s\n", ck.toString(), msg) }
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*rpc.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.lastLeader = 0
	ck.lastRequest = 0

	ck.DEBUG = true

	ck.logDebug(fmt.Sprintf("Started Up"))

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.ClientId = ck.me
	args.RequestId = ck.lastRequest + 1
	args.Key = key

	ck.lastRequest += 1
	
	//Want to iterate through servers starting at the lastLeader
	servNum := ck.lastLeader
	//Keep trying request until it succeeds
	for {
		reply := GetReply{}

		//ck.logDebug(fmt.Sprintf("Sending RPC %v to Server:%d", args, servNum))
		err := ck.servers[servNum].Call("RaftKV.Get", args, &reply)

		//If the RPC call succeeded then return the value 
		if err == nil && reply.Status == OK {
			ck.logDebug(fmt.Sprintf("RPC %v succeeded, got value:%s", args, reply.Value))

			//Set the last leader for use in the future
			ck.lastLeader = servNum
			return reply.Value
		} 

		//Otherwise try the next server
		//ck.logDebug(fmt.Sprintf("RPC %v call to Server:%d failed", args, servNum))
		servNum = (servNum + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	args := PutAppendArgs{}
	args.ClientId = ck.me
	args.RequestId = ck.lastRequest + 1
	args.Type = opType
	args.Key = key
	args.Value = value

	ck.lastRequest += 1

	//Want to iterate through servers starting at the lastLeader
	servNum := ck.lastLeader
	//Keep trying request until it succeeds
	for {
		reply := PutAppendReply{}

		//ck.logDebug(fmt.Sprintf("Sending RPC %v to Server:%d", args, servNum))
		err := ck.servers[servNum].Call("RaftKV.PutAppend", args, &reply)

		if err == nil && reply.Status == OK {
			ck.logDebug(fmt.Sprintf("RPC %v succeeded", args))

			//Set the last leader for use in the future
			ck.lastLeader = servNum
			return
		}

		//Otherwise try the next server
		//ck.logDebug(fmt.Sprintf("RPC %v call to Server:%d failed", args, servNum))
		servNum = (servNum + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.logDebug(fmt.Sprintf("Put(key:%s, value:%s) called", key, value))
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.logDebug(fmt.Sprintf("Append(key:%s, value:%s) called", key, value))
	ck.PutAppend(key, value, Append)
}
