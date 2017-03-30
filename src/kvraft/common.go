package kvraft

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug > 0 {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

const (
	Get = "Get"
	Put = "Put"
	Append = "Append"
	OK    = "OK"
	Error = "Error"
)

type OpType string

type RpcStatus string

type PutAppendArgs struct {
	ClientId  int64
	RequestId int
	Type      OpType
	Key       string
	Value     string
}

type PutAppendReply struct {
	Status RpcStatus
}

type GetArgs struct {
	ClientId  int64
	RequestId int
	Key       string
}

type GetReply struct {
	Status RpcStatus
	Value  string
}
