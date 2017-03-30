package kvraft


import (
	"net"
	"net/rpc"
	"net/http"
	"testing"
	"sync"
	//"runtime"
	"raft"
	"log"
	"math/rand"
	"fmt"
	// "os"
)

// Randomize server handles
func randomize_clients(clientEnds []*rpc.Client) []*rpc.Client {
	sa := make([]*rpc.Client, len(clientEnds))
	copy(sa, clientEnds)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type Config struct {
	mu           sync.Mutex
	num_kvs      int
	ports        []int
	kvservers    []*RaftKV
	saved        []*raft.Persister
	clientEnds   [][]*rpc.Client // names of each server's sending ClientEnds
	clerks       map[*Clerk][]*rpc.Client
	maxraftstate int
	nextClientId int
	tag          string	
	t            *testing.T
}

func MakeConfig(t *testing.T, tag string, num_kvs int, maxraftstate int) *Config {
	//runtime.GOMAXPROCS(4)
	cfg := &Config{}
	cfg.num_kvs = num_kvs
	cfg.t = t
	cfg.tag = tag
	cfg.ports = make([]int, cfg.num_kvs)
	cfg.kvservers = make([]*RaftKV, cfg.num_kvs)
	cfg.saved = make([]*raft.Persister, cfg.num_kvs)
	cfg.clientEnds = make([][]*rpc.Client, cfg.num_kvs)
	cfg.clerks = make(map[*Clerk][]*rpc.Client)
	cfg.nextClientId = cfg.num_kvs + 1000 // client ids start 1000 above the highest serverid
	cfg.maxraftstate = maxraftstate


	for i := 0; i < cfg.num_kvs; i++ {
		cfg.ports[i] = 8080 + i

		if cfg.saved[i] != nil {
			cfg.saved[i] = cfg.saved[i].Copy()
		} else {
			cfg.saved[i] = raft.MakePersister()
		}
	}

	log.Printf("Starting servers\n")
	// create a full set of KV servers.
	for i := 0; i < cfg.num_kvs; i++ {
		cfg.StartServer(i, fmt.Sprintf(":%d", cfg.ports[i]))
	}

	log.Printf("Connecting servers\n")
	for i := 0; i < cfg.num_kvs; i++ {
		cfg.connect(i)
	}

	log.Printf("Starting KV servers\n")
	for i := 0; i < cfg.num_kvs; i++ {
		cfg.kvservers[i].StartKVServer(cfg.clientEnds[i], i, cfg.saved[i], cfg.maxraftstate)
	}

	return cfg
}

func (cfg *Config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			cfg.kvservers[i].Kill()
		}
	}
}

func (cfg *Config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.num_kvs; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

// If restart servers, first call ShutdownServer
func (cfg *Config) StartServer(servNum int, addr string) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	srv := rpc.NewServer()

	kv := &RaftKV{}

	err := srv.Register(kv)
	if err != nil {
		log.Fatalf("Register Error: %s", err)
	}

	kv.rf = &raft.Raft{}
	err = srv.Register(kv.rf)
	if err != nil {
		log.Fatalf("Register Error: %s", err)
	}

	cfg.kvservers[servNum] = kv

	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux

    srv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

    http.DefaultServeMux = oldMux

    //Creates a listener
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen Error: %s", err)
	}

	//Accept connections
	go http.Serve(ln, mux)
}

func (cfg *Config) connect(servNum int) {
	// log.Printf("connect peer %d to %v\n", i, to)
	cfg.clientEnds[servNum] = make([]*rpc.Client, cfg.num_kvs)

	// outgoing socket files
	for i := 0; i < cfg.num_kvs; i++ {
		addr := fmt.Sprintf(":%d", cfg.ports[i])
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Dial Error: %s", err)
		}
		cfg.clientEnds[servNum][i] = client
	}
}


// Create a clerk with clerk specific server names.
// Give it connections to all of the servers, but for
// now enable only connections to servers in to[].
func (cfg *Config) MakeClient() *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh set of ClientEnds.
	clientEnds := make([]*rpc.Client, cfg.num_kvs)

	for i := 0; i < cfg.num_kvs; i++ {
		addr := fmt.Sprintf(":%d", cfg.ports[i])
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Dial Error: %s", err)
		}
		clientEnds[i] = client
	}

	ck := MakeClerk(randomize_clients(clientEnds))
	cfg.clerks[ck] = clientEnds
	cfg.nextClientId++

	return ck
}

func (cfg *Config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// v := cfg.clerks[ck]
	// for i := 0; i < len(v); i++ {
	// 	os.Remove(v[i])
	// }
	delete(cfg.clerks, ck)
}


