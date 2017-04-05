package raft

import (
	"sync"
	"math/rand"
	"time"
	"fmt"
	"math"
	"bytes"
	"encoding/gob"
	"strings"
	"net/rpc"
)

// Various constants 
const ELECTION_TIMEOUT_MIN = 300
const ELECTION_TIMEOUT_MAX = 500
const HEARTBEAT_TIMEOUT = 30
const APPLY_STATE_TIMEOUT = 50

type State int
const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu                 sync.Mutex
	//THREAD-SAFE
	peers              []*rpc.Client //immutable
	persister          *Persister
	me                 int // index into peers[], immutable
	peerNums           []int //the indices of all peers to this raft
	//LOCK BEFORE READ OR WRITE
	state              State //if this Raft thinks its the leader
	CurrentTerm        int //last term this raft has seen (starts at 0)
	VotedFor           int //peer that this raft voted for in last term
	Logs               []LogEntry //log entries (indexed 0 - N)
	LogStartIndex      int //Used when logs have been snapshotted
	LastSnapshotTerm   int
	CommitIndex        int //index of highest log entry known to be commited, (indexed 0 - N, initialized to -1)
	lastAppliedIndex   int //index of highest log entry applied to state machine (indexed 0 - N, initialized to -1)
	VotesFor           int
	heartbeatCh        chan bool
	requestVoteCh      chan bool
	leaderCh           chan bool
	DEBUG              bool //basic debug logging
	LOCK_DEBUG         bool //lock debug logging
	//Leaders only
    NextIndex          []int //for each server, index of next log entry to send to that server (indexed 0-N, initialized to 0)
	matchIndex         []int //for each server, index of highest log entry known to be replicated on server
}

func (rf *Raft) stateToString() string {
	switch rf.state {
	case LEADER: 
		return "Leader"
	case FOLLOWER: 
		return "Follower"
	case CANDIDATE: 
		return "Candidate"
	default:
		return ""
	}
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("<Peer:%d Term:%d State:%s>", rf.me, rf.CurrentTerm, rf.stateToString())
}

func (rf *Raft) getMutex(methodName string) {
	if rf.LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("%s:%s HAS the Lock\n", rf.toString(), methodName) }
	rf.mu.Lock()
}

func (rf *Raft) releaseMutex(methodName string) {
	rf.mu.Unlock()
	if rf.LOCK_DEBUG && rf.state != FOLLOWER { fmt.Printf("%s:%s RELEASES the Lock\n", rf.toString(), methodName) }
}

func (rf *Raft) logDebug(msg string) {
	if rf.DEBUG { fmt.Printf("%s:%s\n", rf.toString(), msg) }
}


/***
GRAB THE LOCK BEFORE CALLING ANY OF THESE 4 METHODS
**/
//Translates a global logIndex to an entry num in the current Logs[] array
func (rf *Raft) getLogEntryNum(logIndex int) int {
	return logIndex - rf.LogStartIndex
}

//Translates a local logEntry num to a global logIndex
func (rf *Raft) getLogIndex(logEntryNum int) int {
	return rf.LogStartIndex + logEntryNum
}

func (rf *Raft) getLogTerm(logIndex int) int {
	if logIndex >= rf.LogStartIndex {
		return rf.Logs[rf.getLogEntryNum(logIndex)].Term 
	} else {
		return rf.LastSnapshotTerm
	}
}

//Gets the global lastLogIndex for this server
func (rf *Raft) getLastLogIndex() int {
	return (rf.LogStartIndex + len(rf.Logs)) - 1
}

//Gets the term of the last LogEntry for this server
func (rf *Raft) getLastLogTerm() int {
	if len(rf.Logs) > 0 {
		return rf.Logs[len(rf.Logs) - 1].Term
	} else {
		return rf.LastSnapshotTerm
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.CurrentTerm
	isLeader := (rf.state == LEADER)
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LogStartIndex)
	e.Encode(rf.LastSnapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.LogStartIndex)
	d.Decode(&rf.LastSnapshotTerm)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.getMutex("RequestVote()")
	defer rf.releaseMutex("RequestVote()")

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	// If the requester's term is less than this peer's term: VoteGranted=false
	if args.Term < rf.CurrentTerm {
		rf.logDebug(fmt.Sprintf("DENIED RequestVote for Requester:<Peer:%d Term:%d> (Requester Behind This Peer's Term)", 
			args.CandidateId, args.Term))

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else if args.Term == rf.CurrentTerm {
	// If the requester's term and this peers term are the same...
		// If this Peer has already voted or the Requester's log is behind this Peer's log VoteGranted=false
		//  A requester's log is behind this peer's log if:
		//   1) The lastLogTerm of this Peer is greater than the lastLogTerm of the Requester
		//   2) The lastLogTerms are equal and the lastLogIndex of this Peer is greater than the lastLogIndex of the Requester
		if rf.state == LEADER || rf.VotedFor > -1 || lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && lastLogIndex > args.LastLogIndex) {
			rf.logDebug(fmt.Sprintf("DENIED RequestVote for Requester:<Peer:%d Term:%d> (This Peer is already the Leader OR this Peer Already Voted OR the Requester's Logs Are Behind)", 
				args.CandidateId, args.Term))

			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false 
		} else {
		// If the Requester's log is not behind this Peer's log VoteGranted=true
			rf.logDebug(fmt.Sprintf("GRANTED RequestVote for Requester:<Peer:%d Term:%d> (Term was the same)", 
				args.CandidateId, args.Term))

			rf.VotedFor = args.CandidateId
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
		}
	} else {
	// If Requester's term is greater than this Peer's current term
		// If the Requester's log is behind this Peer's log VoteGranted=false
		//  A requester's log is behind this peer's log if:
		//   1) The lastLogTerm of the requester is less than the lastLogTerm of this peer
		//   2) The lastLogTerms are equal and the lastLogIndex of the requester is less than the LastLogIndex of this peer		
		if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && lastLogIndex > args.LastLogIndex) {
			rf.logDebug(fmt.Sprintf("DENIED RequestVote for Requester:<Peer:%d Term:%d> (Requesters Logs Are Behind)", 
				args.CandidateId, args.Term))

			reply.VoteGranted = false
			//Whatever votes this server granted in this term are now invalid
			rf.VotedFor = -1
		} else {
		// If the Requester's logs are not behind this Peer's logs VoteGranted=true
			rf.logDebug(fmt.Sprintf("GRANTED RequestVote for Requester:<Peer:%d Term:%d> (Term was greater)", 
				args.CandidateId, args.Term))

			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
		}

		//If a peer discovers its term is out of date it immediately reverts to the Follower state
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		rf.requestVoteCh <- true
	}

	//Persist changes to this raft
	go rf.persist()

	return nil
}

// Send requestVote RPC's to all peers
func (rf *Raft) broadcastRequestVote(){
	rf.logDebug(fmt.Sprintf("Sending RequestVote to Peers:%v", 
		rf.peerNums))

	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		rf.getMutex("broadcastRequestVote()")

		if peerNum != rf.me {
			args := &RequestVoteArgs{}
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.getLastLogTerm()

			reply := &RequestVoteReply{}

			if rf.peers[peerNum] != nil {
				go rf.sendRequestVote(peerNum, *args, reply)
			}
		}

		rf.releaseMutex("broadcastRequestVote()")
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if err == nil {
		rf.getMutex("sendRequestVote()")

		if rf.state == CANDIDATE {
			// If a Server has a higher term than this candidate stand down to follower
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.requestVoteCh <- true
			} else if reply.VoteGranted {
			// If this Peer is still a candidate and got the vote
				rf.VotesFor += 1
				// If this Candidate has a majority of votes in this election
				if (rf.VotesFor * 2) > len(rf.peers) {
					//Let the main thread know that you have enough votes to be leader
					select {
					case rf.leaderCh <- true:
					default:
						// Dont fill the channel if it's already full (leads to deadlock)
					}	
				}
			} 
		}
		
		rf.releaseMutex("sendRequestVote()")
	}
	return err == nil
}

type AppendEntriesArgs struct {
	Term         int //Leaders term
	LeaderId     int //Leaders id in peers[]
	PrevLogIndex int //Index of last log entry 
	PrevLogTerm  int //Term of last log entry
	Entries      []LogEntry //Entries to store (empty for heartbeat)
	LeaderCommit int //Leaders commitIndex
}

type AppendEntriesReply struct {
	Term    int //current term 
	Success bool
	MatchIndex int //index of the lastLogEntry on this server
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.getMutex("AppendEntries()")
	defer rf.releaseMutex("AppendEntries()")

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	// If this Peer's term is greater than the Leader's term then Success=false
	if rf.CurrentTerm > args.Term {
		rf.logDebug(fmt.Sprintf("Got AppendEntry for earlier term from:<Leader:%d Term:%d>", 
			args.LeaderId, args.Term))

		reply.Term = rf.CurrentTerm
		reply.MatchIndex = lastLogIndex
		reply.Success = false
	} else {
		// If this Peer's term is less than the Leader's term, update it
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
		}
		reply.Term = rf.CurrentTerm


		// If this Peer's lastLogIndex and lastLogTerm are the same as the Leader's lastLogIndex and lastLogTerm 
		// Success=true
		if lastLogIndex == args.PrevLogIndex && lastLogTerm == args.PrevLogTerm {
			//If this Peer has fewer comitted entries than the Leader, update its CommitIndex and commit new entries
			if rf.CommitIndex < args.LeaderCommit {
				//Make sure this Peer's new CommitIndex doesn't exceed its lastLogIndex
				rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastLogIndex)))
			}

			//Add all new log entries from the Leader to this Peer
			if len(args.Entries) > 0 {
				rf.logDebug(fmt.Sprintf("Adding Log Entries[%d-%d]:%v", 
					lastLogIndex + 1, lastLogIndex + len(args.Entries), args.Entries))

				rf.Logs = append(rf.Logs, args.Entries...)
			}

			reply.MatchIndex = rf.getLastLogIndex()
			reply.Success = true

		} else {
		// If this Peer's lastLogIndex or lastLogTerm don't match the Leader's lastLogIndex or lastLogTerm 
		// Success=false
			// If this Peer's log has more entries than the Leader's log, delete the extra entries from this Peer
			if lastLogIndex > args.PrevLogIndex {
				//For logging purposes
				origLogLength := len(rf.Logs)
				origLastLogIndex := lastLogIndex

				//Delete extra log entries and set MatchIndex
				rf.Logs = rf.Logs[:rf.getLogEntryNum(args.PrevLogIndex + 1)]
				reply.MatchIndex = rf.getLastLogIndex()

				rf.logDebug(fmt.Sprintf("lastLogIndex:(L:%d/P:%d) is ahead of:<Leader:%d Term:%d>... DELETING %d entries from this Peer's log", 
					args.PrevLogIndex, origLastLogIndex, args.LeaderId, args.Term, origLogLength - len(rf.Logs)))

			} else if lastLogIndex == args.PrevLogIndex && lastLogTerm != args.PrevLogTerm {
			// If the lastLogIndexes are the same length but this Peer's lastLogTerm is not equal to the Leader's lastLogTerm delete 1 entry from this Peer
				rf.logDebug(fmt.Sprintf("lastLogIndexes match:(L:%d/P:%d) but lastTerms don't:(L:%d/P:%d). DELETING 1 entry from this Peer's log", 
					args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, lastLogTerm))

				rf.Logs = rf.Logs[:rf.getLogEntryNum(lastLogIndex)]
				reply.MatchIndex = rf.getLastLogIndex()
			} else {
			// If this Peer's lastLogIndex is behind the Leader, then just let the Leader know
				reply.MatchIndex = lastLogIndex
				rf.logDebug(fmt.Sprintf("lastLogIndex:(Leader:%d/Peer:%d) is behind:<Leader:%d Term:%d>", 
					args.PrevLogIndex, lastLogIndex, args.LeaderId, args.Term))
			}
			reply.Success = false
		}
		
		//Send the heartbeat notice to the main server thread
		rf.heartbeatCh <- true

		//Persist changes to this raft
		go rf.persist()
	}

	return nil
}

// Brooadcast AppendEntries RPCs to all peers
func (rf *Raft) broadcastAppendEntries(){
	msgs := make([]string, 0)
	for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
		if peerNum != rf.me {
			rf.getMutex("broadcastAppendEntries()")

			// Get the index of the next log entry to send to this Peer
			nextLogIdx := rf.NextIndex[peerNum]

			//If this Leader has deleted the log that the Peer needs, then send it a snapshot
			if nextLogIdx < rf.LogStartIndex {
				args := &InstallSnapshotArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.LogStartIndex - 1 
				args.LastIncludedTerm = rf.LastSnapshotTerm
				args.Data = rf.persister.ReadSnapshot()

				reply := &InstallSnapshotReply{}

				go rf.sendInstallSnapshot(peerNum, *args, reply)
			} else {
			// Otherwise just send a regular AppendEntry
				args := &AppendEntriesArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me

				// Set the info of the previous log entry (immediately preceeding nextLogIdx)
				args.PrevLogIndex = nextLogIdx - 1
				args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)

				args.Entries = rf.Logs[rf.getLogEntryNum(nextLogIdx):]
				args.LeaderCommit = rf.CommitIndex

				reply := &AppendEntriesReply{}

				if len(args.Entries) > 0 {
					msgs = append(msgs, 
						fmt.Sprintf("<Peer:%d, Entries[%d-]:%v>", 
							peerNum, args.PrevLogIndex + 1, args.Entries))
				}

				if rf.peers[peerNum] != nil {
					go rf.sendAppendEntries(peerNum, *args, reply)
				}
			}

			rf.releaseMutex("broadcastAppendEntries()")
		}		
	}

	if (len(msgs) > 0){
		rf.logDebug(fmt.Sprintf("Broadcasting new Log Entries to:%v", 
			strings.Join(msgs, ", ")))
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	err := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if err == nil {
		rf.getMutex("sendAppendEntries()")
		defer rf.releaseMutex("sendAppendEntries()")

		if rf.state != LEADER {
			rf.logDebug(fmt.Sprintf("Not a leader, no longer accepting AppendEntry replies"))
			return err == nil
		}

		if !reply.Success {
			// If this Leader's term is less than the Peer's term, update its term and stand down to Follower
			if rf.CurrentTerm < reply.Term {
				rf.CurrentTerm = reply.Term
				rf.heartbeatCh <- true
			} else {
			// Otherwise we need to update this Leader's MatchIndex and NextIndex arrays according to the MatchIndex returned by the Peer
				rf.logDebug(fmt.Sprintf("AppendEntry to <Peer:%d> was Not sucessful, changing MatchIndex[%d] to %d and NextIndex[%d] to %d", 
					server, server, reply.MatchIndex, server, reply.MatchIndex + 1)) 

				rf.matchIndex[server] = reply.MatchIndex
				rf.NextIndex[server] = reply.MatchIndex + 1
			}	
		} else {
		// If the call was successful then update the leaders matchIndex array and NextIndex Array
			rf.matchIndex[server] = reply.MatchIndex
			rf.NextIndex[server] = reply.MatchIndex + 1
		}
	}
	return err == nil
}

func (rf *Raft) commitNewEntries(){
	rf.getMutex("commitNewEntries()")
	defer rf.releaseMutex("commitNewEntries()")

	if rf.state != LEADER || len(rf.Logs) == 0 { return }

	//Go through the leaders logs backwards until the entry at the CommitIndex
	for logEntryNum := len(rf.Logs) - 1; logEntryNum > rf.getLogEntryNum(rf.CommitIndex); logEntryNum-- {
		logEntry := rf.Logs[logEntryNum]
		logTerm := logEntry.Term
		logIndex := rf.getLogIndex(logEntryNum)

		//If we've reached a LogEntry that is of lower term we can just break out
		if logTerm < rf.CurrentTerm { break }
		//Replication count for this LogEntry starts at 1 (its on the leader)
		replicationCount := 1
		//Iterate through all peers
		for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
			//If a peer has this Log replicated on it, increment the replication count
			if peerNum != rf.me && rf.matchIndex[peerNum] >= logIndex {
				replicationCount++
			}
		}

		//If a logEntry is the same term as the leader and on a majority of machines commit it and all entries before it
		// if logTerm == rf.CurrentTerm && replicationCount * 2 > len(rf.peers) {
		if (replicationCount * 2) > len(rf.peers) {
			oldCommitIndex := rf.CommitIndex
			oldCommitEntryNum := rf.getLogEntryNum(oldCommitIndex)
			rf.CommitIndex = logIndex

			rf.logDebug(fmt.Sprintf("Log[%d]=%v is replicated on %d/%d machines", 
				logIndex, logEntry, replicationCount, len(rf.peers)))
			rf.logDebug(fmt.Sprintf("Committed %d new entries:%v ... CommitIndex moving from %d to %d", 
				rf.CommitIndex - oldCommitIndex, rf.Logs[(oldCommitEntryNum + 1):(logEntryNum + 1)], oldCommitIndex, rf.CommitIndex)) 
			rf.logDebug(fmt.Sprintf("New Commit Log:%v", rf.Logs[:(logEntryNum + 1)]))
			
			//Once we've committed an entry we've committed all before it so no need to keep checking
			break
		}
	}

	go rf.persist()
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

func (rf *Raft) applyState(applyCh chan ApplyMsg){
	for {
		rf.commitNewEntries()

		time.Sleep(time.Duration(APPLY_STATE_TIMEOUT) * time.Millisecond)

		rf.getMutex("applyState()")

		//If the last applied index is more than 1 less than the last log index, the log was trimmed for a snapshot.
		if rf.lastAppliedIndex < (rf.LogStartIndex - 1) {
			applyMsg := ApplyMsg{}
			applyMsg.UseSnapshot = true
			applyMsg.Snapshot = rf.persister.ReadSnapshot()
			applyCh <- applyMsg
			rf.lastAppliedIndex = rf.LogStartIndex - 1
		}

		if rf.lastAppliedIndex < rf.CommitIndex {
			for logIndex := rf.lastAppliedIndex + 1; logIndex <= rf.CommitIndex; logIndex++ {
				applyMsg := ApplyMsg{}
				//Adjust to 1 indexed logs for client
				applyMsg.Index = logIndex + 1
				applyMsg.Command = rf.Logs[rf.getLogEntryNum(logIndex)].Command
				applyCh <- applyMsg
			}
			rf.lastAppliedIndex = rf.CommitIndex
		}

		rf.releaseMutex("applyState()")
	}
}

type InstallSnapshotArgs struct {
	Term              int //Leaders term
	LeaderId          int //Leaders id in peers[]
	LastIncludedIndex int //Index of last LogEntry in snapshot 
	LastIncludedTerm  int //Term of last LogEntry in snapshot
	Data              []byte //Raw snapshot data
}

type InstallSnapshotReply struct {
	Term int //Term of server that RPC was sent to 
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.getMutex("InstallSnapshot()")
	defer rf.releaseMutex("InstallSnapshot()")

	//If this rafts term is greater than the term of the leader sending the InstallSnapshot reply immediately
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return nil
	}

	if args.LastIncludedIndex < (rf.LogStartIndex - 1) {
		return nil
	}

	rf.CurrentTerm = args.Term	

	//If this peer has more log entries than the snapshot, keep the extra entries
	if (rf.LogStartIndex <= args.LastIncludedIndex) && (rf.getLastLogIndex() > args.LastIncludedIndex) {
		rf.logDebug(fmt.Sprintf("Checking overlapping entry for lastIncludedIndex:%d, logStartIndex:%d, lastLogIndex:%d", 
				args.LastIncludedIndex, rf.LogStartIndex, rf.getLastLogIndex()))

		overlappingEntry := rf.Logs[rf.getLogEntryNum(args.LastIncludedIndex)]
		//If this peer has an entry with the same index and term as the last included entry as the snapshot
		if overlappingEntry.Term == args.LastIncludedTerm {
			rf.Logs = rf.Logs[rf.getLogEntryNum(args.LastIncludedIndex + 1):]
		} else {
			rf.Logs = make([]LogEntry, 0)
		}
	} else {
		rf.Logs = make([]LogEntry, 0)
	}

	//Save the snapshot data
	rf.persister.SaveSnapshot(args.Data)

	//Adjust the log indices accordingly
	rf.LogStartIndex = args.LastIncludedIndex + 1
	rf.LastSnapshotTerm = args.LastIncludedTerm

	//Valid InstallSnapshots count as heartbeats
	// rf.heartbeatCh <-true	

	go rf.persist()

	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	err := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if err == nil {
		rf.getMutex("sendInstallSnapshot()")
		defer rf.releaseMutex("sendInstallSnapshot()")

		if rf.state != LEADER { 
			rf.logDebug(fmt.Sprintf("Not a leader, no longer accepting InstallSnapshot replies"))
			return err == nil
		}

		if rf.CurrentTerm < reply.Term {
			rf.logDebug(fmt.Sprintf("Got InstallSnapshot Reply from <Peer:%d> with higher term %d... standing down to follower", server, reply.Term))
			rf.CurrentTerm = reply.Term
			//This leader needs to stand down
			rf.heartbeatCh <-true
		} else {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.NextIndex[server] = args.LastIncludedIndex + 1
		}
	}
	return err == nil
}

func (rf *Raft) TrimLog(lastAppliedIndex int){
	rf.getMutex("trimLog()")
	defer rf.releaseMutex("trimLog()")

	//Adjust for 1 based log indexes from client
	newLogStartIndex := lastAppliedIndex

	if newLogStartIndex <= rf.LogStartIndex {
		return
	}

	rf.logDebug(fmt.Sprintf("Raft State Size is %d, Trimming Entries[%d-%d]:%v from Log", 
		rf.persister.RaftStateSize(), rf.LogStartIndex, newLogStartIndex - 1, rf.Logs[rf.getLogEntryNum(rf.LogStartIndex):rf.getLogEntryNum(newLogStartIndex)]))

	//Trim all logs before LogStartIndex 
	lastSnapshotEntry := rf.Logs[rf.getLogEntryNum(newLogStartIndex - 1)]
	rf.Logs = rf.Logs[rf.getLogEntryNum(newLogStartIndex):]

	rf.logDebug(fmt.Sprintf("Logs Trimmed, now:[%d-%d]:%v", 
		newLogStartIndex, newLogStartIndex + len(rf.Logs), rf.Logs))


	rf.LogStartIndex = newLogStartIndex
	rf.LastSnapshotTerm = lastSnapshotEntry.Term

	go rf.persist()
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.getMutex("Start()")
	defer rf.releaseMutex("Start()")

	var index int = 0
	term := rf.CurrentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		rf.logDebug(fmt.Sprintf("Received command %v from Client", 
			command)) 

		newEntry := LogEntry{}
		newEntry.Command = command
		newEntry.Term = term
		rf.Logs = append(rf.Logs, newEntry)
		//Adjust to 1 indexed logs for Client
		index = rf.getLastLogIndex() + 1
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.logDebug(fmt.Sprintf("Killed"))
	rf.DEBUG = false
	rf.LOCK_DEBUG = false
	// Your code here, if desired.
}

//Get a random election timeout (between 10ms and 500ms)
func randTimeoutVal(low int, high int) int {
	return rand.Intn(high - low) + low

}

//Run the server
func (rf *Raft) run() {
	for {
		switch rf.state {
		case LEADER:			
			heartbeatTimeout := time.After(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
			select {
			//This Leader got a valid Heartbeat from a Leader of higher term, it will stand down to Follower
			case <-rf.heartbeatCh:
				rf.getMutex("run() LEADER <-rf.heartbeatCh")

				rf.logDebug(fmt.Sprintf("Got Heartbeat of higher term standing down to Follower"))

				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.releaseMutex("run() LEADER <-rf.heartbeatCh")
			//This leader got a valid RequestVote of higher term, it will stand down to Follower
			case <-rf.requestVoteCh:
				rf.getMutex("run() LEADER <-rf.requestVoteCh")

				rf.logDebug(fmt.Sprintf("Got RequestVote of higher term standing down to Follower"))

				rf.state = FOLLOWER
				rf.VotesFor = 0

				rf.releaseMutex("run() LEADER <-rf.requestVoteCh")
			//Send Heartbeats
			case <-heartbeatTimeout:
				rf.broadcastAppendEntries()
			}
		case FOLLOWER:
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			select {
			//This follower got a valid Heartbeat, it will do nothing
			case <-rf.heartbeatCh:
			//This follower got a valid RequestVote of higher term, it will do nothing
			case <-rf.requestVoteCh:
			//This follower's election timeout expired, it will transition to CANDIDATE
			case <-electionTimeout:
				rf.getMutex("run() FOLLOWER <-electionTimeout")

				rf.logDebug(fmt.Sprintf("Timeout moving to Candidate"))
				rf.state = CANDIDATE

				rf.releaseMutex("run() FOLLOWER <-electionTimeout")
			}
		case CANDIDATE:
			//This server is a new Candidate, it will increment its term and vote for itself
			rf.getMutex("run() CANDIDATE")

			rf.CurrentTerm += 1
			rf.VotedFor = rf.me
			rf.VotesFor += 1

			rf.logDebug(fmt.Sprintf("Starting election for Term:%d", rf.CurrentTerm))

			rf.releaseMutex("run() CANDIDATE")

			//Reset the election timer
			electionTimeoutVal := randTimeoutVal(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
			electionTimeout := time.After(time.Duration(electionTimeoutVal) * time.Millisecond)
			//Send request vote RPCs to all other servers
			rf.broadcastRequestVote()
			select {
			//This Candidate received a valid Heartbeat from a Leader, it will revert to Follower
			case <-rf.heartbeatCh:
				rf.getMutex("run() CANDIDATE <-rf.heartbeatCh")

				rf.logDebug(fmt.Sprintf("Got Heartbeat of higher term standing down to Follower"))

				rf.state = FOLLOWER
				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.releaseMutex("run() CANDIDATE <-rf.heartbeatCh")
			//This Candidate received a RequestVote of higher term, it updated its term and will stand down to Follower
			case <-rf.requestVoteCh:
				rf.getMutex("run() CANDIDATE <-rf.requestVoteCh")

				rf.logDebug(fmt.Sprintf("Got RequestVote of higher term standing down to Follower"))

				rf.state = FOLLOWER
				rf.VotesFor = 0

				rf.releaseMutex("run() CANDIDATE <-rf.requestVoteCh")
			//This Candidate received enough votes, it will transition to Leader
			case <-rf.leaderCh:
				rf.getMutex("run() CANDIDATE <-rf.leaderCh")

				rf.logDebug(fmt.Sprintf("Got enough votes moving to Leader"))

				rf.state = LEADER
				rf.VotedFor = -1
				rf.VotesFor = 0
				//When a leader comes to power initialize NextIndex to be the 1 greater than the last entry in the new leader's log
				for peerNum := 0; peerNum < len(rf.peers); peerNum++ {
					rf.NextIndex[peerNum] = rf.getLastLogIndex() + 1
				}

				rf.releaseMutex("run() CANDIDATE <-rf.leaderCh")
			//If you timeout without winning or losing remain a %d and start the election over
			case <-electionTimeout:
				rf.getMutex("run() CANDIDATE <-electionTimeout")

				rf.logDebug(fmt.Sprintf("Election timeout (Term:%d)", rf.CurrentTerm))

				rf.VotedFor = -1
				rf.VotesFor = 0

				rf.releaseMutex("run() CANDIDATE <-electionTimeout")
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft) Make(peers []*rpc.Client, me int,
	persister *Persister, applyCh chan ApplyMsg) {
	//rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peerNums = make([]int, 0)
	for i := 0; i < len(peers); i++ {
		if i != me { rf.peerNums = append(rf.peerNums, i) }
	}

	//Voting initialization
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.VotesFor = 0
	rf.heartbeatCh = make(chan bool)
	rf.requestVoteCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	
	//Log initialization
	rf.Logs = make([]LogEntry, 0)
	rf.CommitIndex = -1
	rf.lastAppliedIndex = -1
	rf.LogStartIndex = 0
	rf.LastSnapshotTerm = 0


	rf.NextIndex = make([]int, len(peers)) 
	rf.matchIndex = make([]int, len(peers))

	rf.DEBUG = true
	rf.LOCK_DEBUG = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = rf.LogStartIndex - 1

	//Run the main server thread
	rf.logDebug(fmt.Sprintf("Started Up"))
	go rf.run()
	//need to send ApplyMsgs on the applyCh
	go rf.applyState(applyCh)

}
