package shardkv

// import "shardmaster"
import (
	"../labrpc"
	"../raft"
	"../shardmaster"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	// Snapshot state on each server
	appliedIndex        int
	configNums          [shardmaster.NShards]int
	isAvailable         [shardmaster.NShards]bool
	moveOutArgs         [shardmaster.NShards]MoveOutArgs
	kvShards            [shardmaster.NShards]map[string]string
	clientIdToSeqShards [shardmaster.NShards]map[int64]int

	// Volatile state on leader
	config      shardmaster.Config
	firstConfig shardmaster.Config
}

func (kv *ShardKV) snapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.configNums)
	e.Encode(kv.appliedIndex)
	e.Encode(kv.isAvailable)
	e.Encode(kv.moveOutArgs)
	e.Encode(kv.kvShards)
	e.Encode(kv.clientIdToSeqShards)
	kv.persister.SaveSnapshot(w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.configNums)
	d.Decode(&kv.appliedIndex)
	d.Decode(&kv.isAvailable)
	d.Decode(&kv.moveOutArgs)
	d.Decode(&kv.kvShards)
	d.Decode(&kv.clientIdToSeqShards)
}

func (kv *ShardKV) waitForApplied(op Op) bool {
	index, term, ok := kv.rf.Start(op)
	if !ok {
		return false
	}
	sleepTime := 10 * time.Millisecond
	defer kv.mu.Unlock()
	for {
		kv.mu.Lock()
		currentTerm, isLeader := kv.rf.GetState()
		if currentTerm != term || !isLeader {
			return false
		}
		if index <= kv.appliedIndex {
			return true
		}
		kv.mu.Unlock()
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (kv *ShardKV) apply() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		DPrintf("Server (%d, %d) is applying msg %v\n", kv.gid, kv.me, msg)
		if msg.UseSnapshot {
			kv.readSnapshot(msg.Snapshot)
			kv.rf.Snapshot(kv.appliedIndex)
		} else {
			op := msg.Command.(Op)
			if !op.NoOp {
				switch op.Val.(type) {
				case PutAppendArgs:
					args := op.Val.(PutAppendArgs)
					shardId := key2shard(args.Key)
					if kv.isAvailable[shardId] && args.SeqNum > kv.clientIdToSeqShards[shardId][args.ClientId] {
						var reply PutAppendReply
						value, ok := kv.kvShards[shardId][args.Key]
						if args.Op == "Put" || !ok {
							value = args.Value
						} else {
							value += args.Value
						}
						reply.Err = OK
						kv.kvShards[shardId][args.Key] = value
						kv.clientIdToSeqShards[shardId][args.ClientId] = args.SeqNum
					}
				case MoveInArgs:
					args := op.Val.(MoveInArgs)
					if args.ConfigNum > kv.configNums[args.ShardId] {
						kv.configNums[args.ShardId] = args.ConfigNum
						kv.kvShards[args.ShardId] = args.Kv
						kv.clientIdToSeqShards[args.ShardId] = args.ClientIdToSeq
						kv.isAvailable[args.ShardId] = args.IsAvailable
					}
				case MoveOutArgs:
					args := op.Val.(MoveOutArgs)
					if kv.isAvailable[args.ShardId] && args.ConfigNum > kv.configNums[args.ShardId] {
						kv.isAvailable[args.ShardId] = false
						kv.moveOutArgs[args.ShardId] = args
					}
				}
			}
			kv.appliedIndex = msg.Index
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.snapshot()
				kv.rf.Snapshot(kv.appliedIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{NoOp: true}
	ok := kv.waitForApplied(op)
	reply.WrongLeader = !ok
	if ok {
		shardId := key2shard(args.Key)
		kv.mu.Lock()
		if kv.isAvailable[shardId] {
			kvShard := kv.kvShards[shardId]
			value, hasKey := kvShard[args.Key]
			if hasKey {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()
	}
	//DPrintf("Server (%d, %d) args = %v, reply = %v\n", kv.gid, kv.me, *args, *reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	ok := kv.waitForApplied(op)
	reply.WrongLeader = !ok
	if ok {
		shardId := key2shard(args.Key)
		kv.mu.Lock()
		if kv.isAvailable[shardId] && kv.clientIdToSeqShards[shardId][args.ClientId] == args.SeqNum {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()
	}
	//DPrintf("Server (%d, %d) args = %v, reply = %v\n", kv.gid, kv.me, *args, *reply)
}

func (kv *ShardKV) MoveIn(args *MoveInArgs, reply *MoveInReply) {
	op := Op{
		NoOp: false,
		Val:  *args}
	ok := kv.waitForApplied(op)
	reply.WrongLeader = !ok
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) updateConfig(sm *shardmaster.Clerk) {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			newConfig := sm.Query(-1)
			kv.mu.Lock()
			kv.config = newConfig
			if kv.firstConfig.Num == 0 && newConfig.Num > 0 {
				kv.firstConfig = sm.Query(1)
			}
			kv.mu.Unlock()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *ShardKV) manageShard(shardId int) {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			if kv.moveOutArgs[shardId].ConfigNum > kv.configNums[shardId] {
				moveOutArgs := kv.moveOutArgs[shardId]
				moveInArgs := MoveInArgs{
					ShardId:       shardId,
					ConfigNum:     moveOutArgs.ConfigNum,
					IsAvailable:   true,
					Kv:            kv.kvShards[shardId],
					ClientIdToSeq: kv.clientIdToSeqShards[shardId]}
				kv.mu.Unlock()
				for idx := 0; ; idx = (idx + 1) % len(moveOutArgs.Servers) {
					var moveInReply MoveInReply
					srv := kv.make_end(moveOutArgs.Servers[idx])
					if srv.Call("ShardKV.MoveIn", &moveInArgs, &moveInReply) && !moveInReply.WrongLeader {
						moveInArgs.IsAvailable = false
						moveInArgs.Kv = make(map[string]string)
						moveInArgs.ClientIdToSeq = make(map[int64]int)
						kv.MoveIn(&moveInArgs, &moveInReply)
						break
					}
				}
			} else if kv.config.Num > kv.configNums[shardId] {
				op := Op{NoOp: true}
				if kv.configNums[shardId] == 0 {
					args := MoveInArgs{
						ShardId:       shardId,
						ConfigNum:     1,
						IsAvailable:   kv.firstConfig.Shards[shardId] == kv.gid,
						Kv:            make(map[string]string),
						ClientIdToSeq: make(map[int64]int)}
					op = Op{
						NoOp: false,
						Val:  args}
				} else if kv.isAvailable[shardId] && kv.config.Shards[shardId] != kv.gid {
					args := MoveOutArgs{
						ConfigNum: kv.config.Num,
						ShardId:   shardId,
						Servers:   kv.config.Groups[kv.config.Shards[shardId]]}
					op = Op{
						NoOp: false,
						Val:  args}
				}
				kv.mu.Unlock()
				if !op.NoOp {
					kv.waitForApplied(op)
				}
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(MoveInArgs{})
	gob.Register(MoveOutArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	// Your initialization code here.
	kv.persister = persister

	kv.appliedIndex = 0
	for shardId := 0; shardId < shardmaster.NShards; shardId++ {
		kv.isAvailable[shardId] = false
		kv.configNums[shardId] = 0
		kv.moveOutArgs[shardId].ConfigNum = 0
		kv.kvShards[shardId] = make(map[string]string)
		kv.clientIdToSeqShards[shardId] = make(map[int64]int)
	}

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())
	kv.rf.Snapshot(kv.appliedIndex)

	DPrintf("Server (%d, %d) recovered to %v %v\n", kv.gid, kv.me, kv.kvShards, kv.clientIdToSeqShards)

	go kv.updateConfig(shardmaster.MakeClerk(masters))

	for shardId := 0; shardId < shardmaster.NShards; shardId++ {
		go kv.manageShard(shardId)
	}

	go kv.apply()

	return kv
}
