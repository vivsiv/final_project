package kvraft

import (
	"bytes"
	"encoding/gob"
	"net/rpc"
	//"log"
	"raft"
	"sync"
	"time"
	"fmt"

)

const RPC_TIMEOUT = 3000

type Op struct {
	Type      OpType
    ClientId  int64 
    RequestId int //Per client for duplicate detection
	Key       string
	Value     string
}

type PendingRpc struct {
	op        *Op
	appliedCh chan bool
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	maxraftstate int //Max allowed size of raft log, snapshot if it grows larger than this
	Database     map[string]string
	pendingRpcs  map[int]*PendingRpc //Maps expectedIndex in this server's log to PendingRpc structs
	LastApplied  map[int64]int //Maps ClientId to last applied RequestId per client
	DEBUG        bool
	LOCK_DEBUG   bool
	SNAPSHOT     bool
}

func (kv *RaftKV) toString() string {
	return fmt.Sprintf("<Peer:%d>", kv.me)
}

func (kv *RaftKV) logDebug(msg string) {
	if kv.DEBUG { fmt.Printf("%s:%s\n", kv.toString(), msg) }
}

func (kv *RaftKV) loadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	dec.Decode(&kv.Database)
	dec.Decode(&kv.LastApplied)

}

func (kv *RaftKV) createSnapshot(lastAppliedIndex int){
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(kv.Database)
	enc.Encode(kv.LastApplied)
	snapshot := buf.Bytes()
	kv.persister.SaveSnapshot(snapshot)

	go kv.rf.TrimLog(lastAppliedIndex)
}

func (kv *RaftKV) Get(args GetArgs, reply *GetReply) error {
	newOp := Op{}
	newOp.ClientId = args.ClientId
	newOp.RequestId = args.RequestId
	newOp.Type = Get
	newOp.Key = args.Key

	//Try Persisting Command to Raft
	expectedIndex, _, isLeader := kv.rf.Start(newOp)

	//If this server isnt the leader then return
	if !isLeader {
		reply.Status = Error
		return nil
	}

	//If it is the leader then service the request
	kv.logDebug(fmt.Sprintf("Got Get RPC:%v, persisting to Raft...", newOp))

	pendingRpc := &PendingRpc{}
	pendingRpc.op = &newOp
	pendingRpc.appliedCh = make(chan bool)

	kv.mu.Lock()
	kv.pendingRpcs[expectedIndex] = pendingRpc
	kv.mu.Unlock()

	rpcTimeout := time.After(time.Duration(RPC_TIMEOUT) * time.Millisecond)
	//Wait to see if the command gets applied to the raft
	select {
	//This Op gets applied to the raft
	case applied := <-pendingRpc.appliedCh:	
		if applied {
			kv.logDebug(fmt.Sprintf("Notified %v Op was persisted to Raft, updating database...", newOp))
			//Then we can query the database
			kv.mu.Lock()
			value := kv.Database[newOp.Key]
			kv.mu.Unlock()

			reply.Status = OK
			reply.Value = value
		} else {
			reply.Status = Error
			//kv.logDebug(fmt.Sprintf("Notified %v Op failed to persist to Raft, reporting error...", newOp))
		}
	case <-rpcTimeout:
		kv.logDebug(fmt.Sprintf("RPC for Op %v timed out", newOp))

		kv.mu.Lock()
		delete(kv.pendingRpcs, expectedIndex)
		kv.mu.Unlock()

		reply.Status = Error	
	}
	kv.logDebug(fmt.Sprintf("Returning Reply %v for Get RPC %v", *reply, newOp))

	return nil
}

func (kv *RaftKV) PutAppend(args PutAppendArgs, reply *PutAppendReply) error {
	newOp := Op{}
	newOp.ClientId = args.ClientId
	newOp.RequestId = args.RequestId
	newOp.Type = args.Type
	newOp.Key = args.Key
	newOp.Value = args.Value

	//Need to Persist Command to Raft
	expectedIndex, _, isLeader := kv.rf.Start(newOp)

	if !isLeader {
		reply.Status = Error
		return nil
	}

	pendingRpc := &PendingRpc{}
	pendingRpc.op = &newOp
	pendingRpc.appliedCh = make(chan bool)

	kv.mu.Lock()
	kv.pendingRpcs[expectedIndex] = pendingRpc
	kv.mu.Unlock()

	kv.logDebug(fmt.Sprintf("Got Put/Append RPC:%v, persisting to Raft...", newOp))

	rpcTimeout := time.After(time.Duration(RPC_TIMEOUT) * time.Millisecond)
	select {
	//This Op gets applied to the raft
	case applied := <-pendingRpc.appliedCh:
		if applied {
			reply.Status = OK
			//kv.logDebug(fmt.Sprintf("Notified %v Op was persisted to Raft, Returning Reply %v", newOp, *reply))
		} else {
			reply.Status = Error
			//kv.logDebug(fmt.Sprintf("Notified %v Op was NOT persisted to Raft, Returning Reply %v", newOp, *reply))
		}
	case <-rpcTimeout:
		kv.logDebug(fmt.Sprintf("RPC for Op %v timed out", newOp))

		kv.mu.Lock()
		delete(kv.pendingRpcs, expectedIndex)
		kv.mu.Unlock()

		reply.Status = Error	
	}

	kv.logDebug(fmt.Sprintf("Returning Reply %v for Put/Append RPC %v", *reply, newOp))	

	return nil
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.DEBUG = false
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *RaftKV) run(){
	for {
		//Read apply messages off the apply channel and process them
		appliedMsg := <-kv.applyCh

		//Check if this applyMsg has a snapshot, if so load it
		if appliedMsg.UseSnapshot {
			kv.logDebug(fmt.Sprintf("Loading Snapshot..."))

			kv.loadSnapshot(appliedMsg.Snapshot)
			continue
		}

		appliedIndex := appliedMsg.Index
		//Type inference to cast back from interface{}
		appliedOp, ok := appliedMsg.Command.(Op)
		if !ok { continue }
		
		kv.mu.Lock()

		lastAppliedId, ok := kv.LastApplied[appliedOp.ClientId]

		kv.logDebug(fmt.Sprintf("Applied Op %v to Raft", appliedOp))

		//If this client doesn't have a lastAppliedOp on this server or
		//This appliedOp is after the lastAppliedOp, from this client on this server  
		if !ok || (appliedOp.RequestId > lastAppliedId) {
			//Update this kv's database accordingly
			switch appliedOp.Type {
			case Put:
				kv.Database[appliedOp.Key] = appliedOp.Value

				kv.logDebug(fmt.Sprintf("New Database %v", kv.Database))
			case Append:
				_, ok := kv.Database[appliedOp.Key]
				//If the entry is in the database then Append concatenates the new Ops value to the existing Op
				//Otherwise it just adds the Op to the database
				if ok {
					kv.Database[appliedOp.Key] += appliedOp.Value
				} else {
					kv.Database[appliedOp.Key] = appliedOp.Value
				}

				kv.logDebug(fmt.Sprintf("New Database %v", kv.Database))
			}

			//Update the lastApplied for this client on this server
			kv.LastApplied[appliedOp.ClientId] = appliedOp.RequestId
		}

		//Initiate snapshot if raft log has grown too large
		if kv.SNAPSHOT && kv.persister.RaftStateSize() > kv.maxraftstate {
			kv.logDebug(fmt.Sprintf("RaftStateSize:%d > maxraftstate:%d... Creating Snapshot until index:%d", 
				kv.persister.RaftStateSize(), kv.maxraftstate, appliedIndex))
			kv.createSnapshot(appliedIndex)
		}

		//Notify any pending RPCs
		pendingRpc, ok := kv.pendingRpcs[appliedIndex]

		//If the RPC for this appliedOp is not pending on this server then just move on
		if !ok { 
			kv.mu.Unlock()
			continue 
		}

		//Delete the pending RPC at this index
		delete(kv.pendingRpcs, appliedIndex)
		kv.mu.Unlock()

		//Make sure the Op that was applied at this index was the Op requested by the pendingRpc
		if pendingRpc.op.ClientId == appliedOp.ClientId && pendingRpc.op.RequestId == appliedOp.RequestId {
			//If so, notify the pending RPC that it has succeeded
			pendingRpc.appliedCh <- true
		} else {
			//If not, notify the pending RPC that it has failed
			pendingRpc.appliedCh <- false
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func (kv *RaftKV) StartKVServer(servers []*rpc.Client, me int, persister *raft.Persister, maxraftstate int) {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv.me = me
	kv.persister = persister
	kv.Database = make(map[string]string)
	kv.pendingRpcs = make(map[int]*PendingRpc)
	kv.LastApplied = make(map[int64]int)
	kv.DEBUG = true

	if maxraftstate > 0 {
		kv.maxraftstate = maxraftstate
		kv.SNAPSHOT = true
	} else {
		kv.maxraftstate = 0
		kv.SNAPSHOT = false
	}

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf.Make(servers, me, persister, kv.applyCh)

	kv.logDebug(fmt.Sprintf("Started Up"))
	//Start up this kv server
	go kv.run()
}
