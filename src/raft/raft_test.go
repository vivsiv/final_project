package raft

import (
	"testing"
	"fmt"
	"time"
	"log"
	// "math/rand"
	// "sync/atomic"
	// "sync"	
)

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection(t *testing.T) {
	num_servers := 3
	cfg := make_config(t, num_servers)
	defer cfg.cleanup()

	fmt.Printf("\n")
	log.Printf("InitialElection: Start...\n")

	// is a leader elected?
	cfg.checkOneLeader()

	log.Printf("InitialElection: Checked One Leader ...\n")

	// does the leader+term stay the same there is no failure?
	term1 := cfg.checkTerms()
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	log.Printf("InitialElection: Checked Terms ...\n")

	log.Printf("InitialElection: Passed\n")
	fmt.Printf("\n")
}

func TestBasicAgree(t *testing.T) {
	num_servers := 3
	cfg := make_config(t, num_servers)
	defer cfg.cleanup()

	fmt.Printf("\n")
	log.Printf("BasicAgree: Start...\n")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, num_servers)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	fmt.Printf("BasicAgree: Passed...\n")
	fmt.Printf("\n")
}

func TestReElection(t *testing.T) {
	num_servers := 3
	cfg := make_config(t, num_servers)
	defer cfg.cleanup()

	fmt.Printf("\n")
	log.Printf("ReElection: Start...\n")

	log.Printf("ReElection: Checking One Leader...\n")
	leader1 := cfg.checkOneLeader()
	log.Printf("ReElection: Checked One Leader: %d\n", leader1)


	log.Printf("ReElection: Disconnecting Leader:%d and Checking One Leader\n", leader1)
	cfg.disconnect(leader1)
	newLeader := cfg.checkOneLeader()
	log.Printf("ReElection: Disconnected Leader:%d and Checked One Leader:%d\n", leader1, newLeader)


	log.Printf("ReElection: Reconnecting Leader:%d and Checking One Leader\n", leader1)
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()
	log.Printf("ReElection: Reconnected Leader:%d and Checked One Leader:%d\n", leader1, leader2)


	log.Printf("ReElection: Disconnecting 2 servers:%d,%d and Checking No Leader\n", leader2, ((leader2 + 1) % num_servers))
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % num_servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()
	log.Printf("ReElection: Disconnected 2 servers:%d,%d and Checked No Leader\n", leader2, ((leader2 + 1) % num_servers))

	
	log.Printf("ReElection: Reconnecting 1 server:%d and Checking One Leader\n", ((leader2 + 1) % num_servers))
	cfg.connect((leader2 + 1) % num_servers)
	cfg.checkOneLeader()
	log.Printf("ReElection: Reconnected 1 server:%d and Checked One Leader\n", ((leader2 + 1) % num_servers))

	
	log.Printf("ReElection: Reconnecting 1 more server:%d and Checking One Leader\n", leader2)
	cfg.connect(leader2)
	cfg.checkOneLeader()
	log.Printf("ReElection: Reconnected 1 more server:%d and Checked One Leader\n", leader2)

	
	log.Printf("ReElection: Passed\n")
	fmt.Printf("\n")
}

