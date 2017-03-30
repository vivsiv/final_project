package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/http"
	"sync"
	"testing"
	"time"
	"sync/atomic"
)

type config struct {
	mu        sync.Mutex
	t         *testing.T
	num_rafts int
	done      int32 // tell internal threads to die
	ports     []int
	rafts     []*Raft
	saved     []*Persister
	clientEnds [][]*rpc.Client
	applyErr  []string // from apply channel readers
	logs      []map[int]int // copy of each server's committed entries
	connected []bool
}

func make_config(num_rafts int) *config {
	//runtime.GOMAXPROCS(4)

	cfg := &config{}
	// cfg.t = t
	cfg.num_rafts = num_rafts
	cfg.ports = make([]int, cfg.num_rafts)
	cfg.rafts = make([]*Raft, cfg.num_rafts)
	cfg.saved = make([]*Persister, cfg.num_rafts)
	cfg.clientEnds =  make([][]*rpc.Client, cfg.num_rafts)
	cfg.applyErr = make([]string, cfg.num_rafts)
	cfg.logs = make([]map[int]int, cfg.num_rafts)
	cfg.connected = make([]bool, cfg.num_rafts)


	for i := 0; i < cfg.num_rafts; i++ {
		cfg.ports[i] = 8080 + i
		cfg.logs[i] = map[int]int{}

		if cfg.saved[i] != nil {
			cfg.saved[i] = cfg.saved[i].Copy()
		} else {
			cfg.saved[i] = MakePersister()
		}
	}



	for i := 0; i < num_rafts; i++ {
		cfg.start_server(i, fmt.Sprintf(":%d", cfg.ports[i]))
	}

	for i := 0; i < num_rafts; i++ {
		cfg.connect(i)
	}

	for i := 0; i < num_rafts; i++ {
		applyCh := make(chan ApplyMsg)

		go func(servNum int) {
			for m := range applyCh {
				err_msg := ""
				if m.UseSnapshot {
					// ignore the snapshot
				} else if v, ok := (m.Command).(int); ok {
					cfg.mu.Lock()
					for j := 0; j < len(cfg.logs); j++ {
						if old, oldok := cfg.logs[j][m.Index]; oldok && old != v {
							// some server has already committed a different value for this entry!
							err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
								m.Index, servNum, m.Command, j, old)
						}
					}

					_, prevok := cfg.logs[servNum][m.Index-1]
					cfg.logs[servNum][m.Index] = v
					cfg.mu.Unlock()

					if m.Index > 1 && prevok == false {
						err_msg = fmt.Sprintf("server %v apply out of order %v", servNum, m.Index)
					}
				} else {
					err_msg = fmt.Sprintf("committed command %v is not an int", m.Command)
				}

				if err_msg != "" {
					log.Fatalf("apply error: %v\n", err_msg)
					cfg.applyErr[servNum] = err_msg
					// keep reading after error so that Raft doesn't block
					// holding locks...
				}
			}
		}(i)

		cfg.rafts[i].Make(cfg.clientEnds[i], i, cfg.saved[i], applyCh)
	}

	return cfg
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	atomic.StoreInt32(&cfg.done, 1)
}

func (cfg *config) start_server(servNum int, addr string) {
	raft := &Raft{}	

	cfg.rafts[servNum] = raft

	srv := rpc.NewServer()	

	err := srv.Register(raft)
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
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen Error: %s", err)
	}

	//Accept connections
	go http.Serve(ln, mux)
}

func (cfg *config) connect(servNum int) {
	cfg.clientEnds[servNum] = make([]*rpc.Client, cfg.num_rafts)

	for i := 0; i < cfg.num_rafts; i++ {
		if cfg.connected[i]{
			addr := fmt.Sprintf(":%d", cfg.ports[i])
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				log.Fatalf("Dial Error: %s", err)
			}
			cfg.clientEnds[servNum][i] = client
		}
	}

	cfg.connected[servNum] = true
}

func (cfg *config) disconnect(servNum int){
	clients := cfg.clientEnds[servNum]

	for i := 0; i < len(clients); i++ {
		err := clients[i].Close()
		if err != nil {
			log.Fatalf("Close Error: %s", err)
		}
	}

	cfg.connected[servNum] = false
}

func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		leaders := make(map[int][]int)
		for i := 0; i < cfg.num_rafts; i++ {
			if cfg.connected[i]{
				if t, leader := cfg.rafts[i].GetState(); leader {
					leaders[t] = append(leaders[t], i)
				}
			}
		}

		lastTermWithLeader := -1
		for t, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", t, len(leaders))
			}
			if t > lastTermWithLeader {
				lastTermWithLeader = t
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.num_rafts; i++ {
		if cfg.connected[i]{
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.num_rafts; i++ {
		if cfg.connected[i] {
			_, is_leader := cfg.rafts[i].GetState()
			if is_leader {
				cfg.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i < cfg.num_rafts; i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v\n",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

func (cfg *config) one(cmd int, expectedServers int) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < cfg.num_rafts; si++ {
			starts = (starts + 1) % cfg.num_rafts
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}





