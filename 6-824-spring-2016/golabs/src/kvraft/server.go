package raftkv

import (
	"../labrpc"
	"../raft"
	"encoding/gob"
	"log"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	NoOp bool
	Val  interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientIdToSeq map[int64]int
	kv            map[string]string
	appliedIndex  int
}

func (kv *RaftKV) waitForApplied(op Op) bool {
	index, term, ok := kv.rf.Start(op)
	if !ok {
		return false
	}
	sleepTime := 10 * time.Millisecond
	for {
		currentTerm, isLeader := kv.rf.GetState()
		if term != currentTerm || !isLeader {
			return false
		}
		kv.mu.Lock()
		if index <= kv.appliedIndex {
			kv.mu.Unlock()
			return true
		}
		kv.mu.Unlock()
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (kv *RaftKV) apply() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		op := msg.Command.(Op)
		DPrintf("Msg: %v\n", msg)
		if !op.NoOp {
			switch op.Val.(type) {
			case PutAppendArgs:
				args := op.Val.(PutAppendArgs)
				if kv.clientIdToSeq[args.ClientId]+1 == args.SeqNum {
					value, hasKey := kv.kv[args.Key]
					if hasKey && args.Op == "Append" {
						value += args.Value
					} else {
						value = args.Value
					}
					kv.kv[args.Key] = value
					DPrintf("%s -> %v\n", args.Key, value)
					kv.clientIdToSeq[args.ClientId]++
				}
			}
		}
		kv.appliedIndex = msg.Index
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{NoOp: true}
	ok := kv.waitForApplied(op)
	reply.WrongLeader = !ok
	if ok {
		kv.mu.Lock()
		value, hasKey := kv.kv[args.Key]
		if hasKey {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}
	DPrintf("%d args = %v, reply = %v\n", kv.me, args, *reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	reply.WrongLeader = !kv.waitForApplied(op)
	DPrintf("%d args = %v, reply = %v\n", kv.me, *args, *reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientIdToSeq = make(map[int64]int)
	kv.kv = make(map[string]string)
	kv.appliedIndex = 0
	go kv.apply()
	return kv
}
