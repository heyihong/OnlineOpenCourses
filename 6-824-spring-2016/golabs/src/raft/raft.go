package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type ServerState string

const (
	Leader              ServerState = "Leader"
	Follower            ServerState = "Follower"
	Candidate           ServerState = "Candidate"
	MinElectionInterval             = 150 // milliseconds
	MaxElectionInterval             = 300 // milliseconds
	HeartbeartInterval              = 20 * time.Millisecond
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []int
	serverState ServerState

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	hasHeartbeat bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here.
	return rf.currentTerm, rf.serverState == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) updateTerm(term int) {
	if rf.currentTerm < term {
		DPrintf("%s %d update its term to %d, became Follower\n", rf.serverState, rf.me, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.serverState = Follower
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term         int
	VoteGranted  bool
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	if reply.VoteGranted = rf.votedFor == -1 || rf.votedFor == args.CandidateId; reply.VoteGranted {
		DPrintf("%s %d votes for %d", rf.serverState, rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.hasHeartbeat = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%s %d sent request vote to %d, ok = %t\n", rf.serverState, rf.me, server, ok)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("%s %d sent heartbeat to %d, ok = %t\n", rf.serverState, rf.me, server, ok)
	return ok
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	if rf.hasHeartbeat {
		rf.hasHeartbeat = false
		rf.mu.Unlock()
		return
	}
	if rf.serverState == Follower {
		// Convert follower to candidate when not receiving heartbeat
		rf.serverState = Candidate
		DPrintf("Discover Leader at term %d failed, Follower %d change became Candidate\n", rf.currentTerm, rf.me)
	}
	if rf.serverState == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		DPrintf("Candidate %d increased its term to %d, voted for himself\n", rf.me, rf.currentTerm)
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me}
		rf.mu.Unlock()
		numVotes := 1
		numFinished := 0
		elected := false // make sure only one value was sent to done channel
		done := make(chan bool)
		for idx := 0; idx < len(rf.peers); idx++ {
			if idx == rf.me {
				continue
			}
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				rf.mu.Lock()
				if ok {
					rf.updateTerm(reply.Term)
					if reply.VoteGranted {
						if numVotes++; rf.currentTerm == reply.Term && rf.serverState == Candidate && numVotes*2 > len(rf.peers) {
							rf.serverState = Leader
							elected = true
							DPrintf("Candidate %d became Leader at term %d\n", rf.me, rf.currentTerm)
							done <- true
						}
					}
				}
				if numFinished++; numFinished == len(rf.peers)-1 && !elected {
					done <- true
				}
				rf.mu.Unlock()
			}(idx)
		}
		<-done
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	if rf.serverState != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me}
	rf.mu.Unlock()
	numFinished := 0
	done := make(chan bool)
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, args, &reply)
			rf.mu.Lock()
			if ok {
				rf.updateTerm(reply.Term)
			}
			if numFinished += 1; numFinished == len(rf.peers)-1 {
				done <- true
			}
			rf.mu.Unlock()
		}(idx)
	}
	<-done
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
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.serverState = Follower
	rf.hasHeartbeat = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			_, isLeader := rf.GetState()
			if isLeader {
				time.Sleep(HeartbeartInterval)
				rf.heartbeat()
			} else {
				sleepTime := time.Duration(rand.Intn(MaxElectionInterval-MinElectionInterval+1)+MinElectionInterval) * time.Millisecond
				time.Sleep(sleepTime)
				rf.elect()
			}
		}
	}()

	return rf
}
