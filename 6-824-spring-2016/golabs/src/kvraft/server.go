package raftkv

import (
	"../labrpc"
	"../raft"
	"bytes"
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
	mu        sync.Mutex
	persister *raft.Persister
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientIdToSeq map[int64]int
	kv            map[string]string
	appliedIndex  int
}

func (kv *RaftKV) snapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.appliedIndex)
	e.Encode(kv.kv)
	e.Encode(kv.clientIdToSeq)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.appliedIndex)
	d.Decode(&kv.kv)
	d.Decode(&kv.clientIdToSeq)
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
		DPrintf("Server %d is applying msg %v\n", kv.me, msg)
		if msg.UseSnapshot {
			kv.readSnapshot(msg.Snapshot)
			DPrintf("Server %d used snapshot (index = %d, snapshot size = %d, raft state size = %d)\n", kv.me, msg.Index, len(msg.Snapshot), len(kv.persister.ReadRaftState()))
		} else {
			op := msg.Command.(Op)
			if !op.NoOp {
				args := op.Val.(PutAppendArgs)
				if kv.clientIdToSeq[args.ClientId]+1 == args.SeqNum {
					value, hasKey := kv.kv[args.Key]
					if hasKey && args.Op == "Append" {
						value += args.Value
					} else {
						value = args.Value
					}
					kv.kv[args.Key] = value
					kv.clientIdToSeq[args.ClientId]++
				}
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot()
				kv.rf.Snapshot(msg.Index)
				DPrintf("Server %d make a snapshot (index = %d, snapshot size = %d, raft state size = %d)\n", kv.me, msg.Index, len(kv.persister.ReadSnapshot()), len(kv.persister.ReadRaftState()))
			}
			kv.appliedIndex = msg.Index
		}
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
	//DPrintf("%d args = %v, reply = %v\n", kv.me, args, *reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	reply.WrongLeader = !kv.waitForApplied(op)
	//DPrintf("%d args = %v, reply = %v\n", kv.me, *args, *reply)
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
	kv.persister = persister
	kv.maxraftstate = maxraftstate
	// Your initialization code here.
	kv.appliedIndex = 0
	kv.clientIdToSeq = make(map[int64]int)
	kv.kv = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())
	kv.rf.Snapshot(kv.appliedIndex)
	DPrintf("Server %d recovered from a snapshot (index = %d, snapshot size = %d, raft state size = %d)\n", kv.me, kv.appliedIndex, len(persister.ReadSnapshot()), len(persister.ReadRaftState()))

	go kv.apply()

	return kv
}
