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
	"sort"
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
	HeartbeartInterval              = 40 * time.Millisecond
	SyncInterval                    = 100 * time.Millisecond
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

type LogEntry struct {
	Term    int
	Command interface{}
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
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedIndex int

	// Volatile state on all servers
	commitIndex  int
	lastApplied  int
	serverState  ServerState
	hasHeartbeat bool
	applyCh      chan ApplyMsg

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here.
	return rf.currentTerm, rf.serverState == Leader
}

func (rf *Raft) Snapshot(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.discardLogs(index)
	rf.persist()
}

func (rf *Raft) discardLogs(index int) {
	if rf.lastIncludedIndex < index {
		if index-rf.lastIncludedIndex < len(rf.log) {
			rf.log = rf.log[index-rf.lastIncludedIndex:]
		} else {
			rf.log = []LogEntry{}
		}
		if rf.commitIndex < index {
			rf.commitIndex = index
		}
		if rf.lastApplied < index {
			rf.lastApplied = index
		}
		rf.lastIncludedIndex = index
	}
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
	e.Encode(rf.lastIncludedIndex)
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
	d.Decode(&rf.lastIncludedIndex)
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastLogTerm < args.LastLogTerm || lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.hasHeartbeat = true
	if args.PrevLogIndex < rf.lastIncludedIndex+len(rf.log) && (args.PrevLogIndex < rf.lastIncludedIndex || rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm) {
		reply.Success = true
		// Skip append when receving heartbeat
		if len(args.Entries) > 0 {
			logIndex := 0
			entryIndex := 0
			if args.PrevLogIndex < rf.lastIncludedIndex {
				entryIndex = rf.lastIncludedIndex - args.PrevLogIndex - 1
			} else {
				logIndex = args.PrevLogIndex - rf.lastIncludedIndex + 1
			}
			for entryIndex < len(args.Entries) && logIndex < len(rf.log) && args.Entries[entryIndex].Term == rf.log[logIndex].Term {
				entryIndex++
				logIndex++
			}
			rf.log = append(rf.log[:logIndex], args.Entries[entryIndex:]...)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
			if rf.commitIndex > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			}
		}
	}
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.persister.SaveSnapshot(args.Data)
	rf.discardLogs(args.LastIncludedIndex)
	if len(rf.log) == 0 || rf.log[0].Term != args.LastIncludedTerm {
		rf.log = []LogEntry{LogEntry{Term: args.LastIncludedTerm}}
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			Index:       -1,
			UseSnapshot: true,
			Snapshot:    args.Data}
	}()
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
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%s %d sent request vote args %v to %d, ok = %t, reply %v\n", rf.serverState, rf.me, args, server, ok, *reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if len(args.Entries) > 0 {
		DPrintf("%s %d sent append entries args %v to %d, ok = %t, reply %v\n", rf.serverState, rf.me, args, server, ok, *reply)
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) elect() {
	for {
		electionInterval := time.Duration(rand.Intn(MaxElectionInterval-MinElectionInterval+1)+MinElectionInterval) * time.Millisecond
		time.Sleep(electionInterval)
		rf.mu.Lock()
		if rf.hasHeartbeat {
			rf.hasHeartbeat = false
			rf.mu.Unlock()
		} else {
			if rf.serverState == Follower {
				// Convert follower to candidate when not receiving heartbeat
				rf.serverState = Candidate
				DPrintf("Discover Leader at term %d failed, Follower %d change became Candidate\n", rf.currentTerm, rf.me)
			}
			if rf.serverState == Candidate {
				rf.currentTerm++
				rf.votedFor = rf.me
				DPrintf("Candidate %d increased its term to %d, voting for himself\n", rf.me, rf.currentTerm)
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIncludedIndex + len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term}
				rf.mu.Unlock()
				numVotes := 1
				for idx := 0; idx < len(rf.peers); idx++ {
					if idx != rf.me {
						go func(server int) {
							var reply RequestVoteReply
							ok := rf.sendRequestVote(server, args, &reply)
							rf.mu.Lock()
							if ok {
								rf.updateTerm(reply.Term)
								if reply.VoteGranted {
									if numVotes++; rf.currentTerm == reply.Term && rf.serverState == Candidate && numVotes*2 > len(rf.peers) {
										rf.serverState = Leader
										go rf.heartbeat(rf.currentTerm)
										for idx := 0; idx < len(rf.peers); idx++ {
											rf.nextIndex[idx] = rf.lastIncludedIndex + len(rf.log)
											rf.matchIndex[idx] = 0
											if idx != rf.me {
												go rf.syncLogs(idx, rf.currentTerm)
											}
										}
										DPrintf("Candidate %d became Leader at term %d\n", rf.me, rf.currentTerm)
									}
								}
							}
							rf.mu.Unlock()
						}(idx)
					}
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) heartbeat(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.currentTerm == term {
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: rf.commitIndex,
			PrevLogTerm:  rf.log[rf.commitIndex-rf.lastIncludedIndex].Term,
			LeaderCommit: rf.commitIndex}
		rf.mu.Unlock()
		for idx := 0; idx < len(rf.peers); idx++ {
			if idx != rf.me {
				go func(server int) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(server, args, &reply)
					rf.mu.Lock()
					if ok {
						rf.updateTerm(reply.Term)
					}
					rf.mu.Unlock()
				}(idx)
			}
		}
		time.Sleep(HeartbeartInterval)
		rf.mu.Lock()
	}
}

func (rf *Raft) syncLogs(server int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	missedLen := 1
	for rf.currentTerm == term {
		if rf.matchIndex[server] < rf.lastIncludedIndex+len(rf.log)-1 {
			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              term,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.ReadSnapshot()}
				var reply InstallSnapshotReply
				rf.mu.Unlock()
				ok := rf.sendInstallSnapshot(server, args, &reply)
				rf.mu.Lock()
				rf.updateTerm(reply.Term)
				if rf.currentTerm != term {
					return
				}
				if ok {
					rf.nextIndex[server] = rf.lastIncludedIndex + len(rf.log)
				}
			} else {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1-rf.lastIncludedIndex].Term,
					Entries:      rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:],
					LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(server, args, &reply)
				rf.mu.Lock()
				if ok {
					rf.updateTerm(reply.Term)
					if rf.currentTerm != term {
						return
					}
					if reply.Success {
						rf.nextIndex[server] += len(args.Entries)
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						// find mid match index
						midx := make([]int, len(rf.peers))
						copy(midx, rf.matchIndex)
						sort.Ints(midx)
						if midx[len(rf.peers)/2] > rf.commitIndex {
							rf.commitIndex = midx[len(rf.peers)/2]
						}
					} else if rf.nextIndex[server] > rf.lastIncludedIndex {
						if rf.nextIndex[server] == rf.lastIncludedIndex+1 {
							// Trigger install snapshot
							rf.nextIndex[server] = rf.lastIncludedIndex
						} else {
							rf.nextIndex[server] -= missedLen
							if rf.nextIndex[server] <= rf.lastIncludedIndex {
								rf.nextIndex[server] = rf.lastIncludedIndex + 1
							}
							missedLen *= 2
						}
					}
				}
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(SyncInterval)
			rf.mu.Lock()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != Leader {
		return -1, rf.currentTerm, false
	}
	entryIndex := rf.lastIncludedIndex + len(rf.log)
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command}
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = entryIndex + 1
	rf.matchIndex[rf.me] = entryIndex
	return entryIndex, rf.currentTerm, rf.serverState == Leader
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
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = 0
	rf.lastIncludedIndex = 0
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.serverState = Follower
	rf.hasHeartbeat = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	go rf.elect()

	go func() {
		for {
			rf.mu.Lock()
			for rf.lastApplied+1 <= rf.commitIndex {
				logEntry := rf.log[rf.lastApplied+1-rf.lastIncludedIndex]
				applyMsg := ApplyMsg{
					Index:   rf.lastApplied + 1,
					Command: logEntry.Command}
				rf.lastApplied++
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			time.Sleep(SyncInterval)
		}
	}()

	return rf
}
