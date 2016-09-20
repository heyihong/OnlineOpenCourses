package shardmaster

import "../raft"
import "../labrpc"
import "sync"
import "encoding/gob"
import "time"
import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	appliedIndex int
	configs      []Config // indexed by config num
}

type Op struct {
	// Your data here.
	NoOp bool
	Val  interface{}
}

func (config *Config) Clone() Config {
	c := Config{}
	c.Num = config.Num
	c.Groups = make(map[int][]string)
	for idx, shard := range config.Shards {
		c.Shards[idx] = shard
	}
	for key, value := range config.Groups {
		c.Groups[key] = value
	}
	return c
}

func (config *Config) Rebalance() {
	numAve := (NShards + len(config.Groups) - 1) / len(config.Groups)
	remain := NShards % len(config.Groups)
	count := make(map[int]int)
	for idx := 0; idx < len(config.Shards); idx += 1 {
		shard := config.Shards[idx]
		if shard != 0 {
			if count[shard] < numAve {
				count[shard] += 1
				if count[shard] == numAve && remain > 0 {
					remain -= 1
					if remain == 0 {
						numAve -= 1
					}
				}
			} else {
				config.Shards[idx] = 0
			}
		}
	}
	idx := 0
	for key, _ := range config.Groups {
		cnt := count[key]
		if cnt < numAve {
			for ; cnt < numAve; cnt += 1 {
				for ; config.Shards[idx] != 0; idx += 1 {
				}
				config.Shards[idx] = key
			}
			if remain > 0 {
				remain -= 1
				if remain == 0 {
					numAve -= 1
				}
			}
		}
	}
}

func (sm *ShardMaster) waitForApplied(op Op) bool {
	index, term, ok := sm.rf.Start(op)
	if !ok {
		return false
	}
	sleepTime := 10 * time.Millisecond
	defer sm.mu.Unlock()
	for {
		sm.mu.Lock()
		currentTerm, isLeader := sm.rf.GetState()
		if currentTerm != term || !isLeader {
			return false
		}
		if index <= sm.appliedIndex {
			return true
		}
		sm.mu.Unlock()
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (sm *ShardMaster) apply() {
	for msg := range sm.applyCh {
		op := msg.Command.(Op)
		sm.mu.Lock()
		DPrintf("Server %d is applying msg %v\n", sm.me, msg)
		if !op.NoOp {
			config := sm.configs[len(sm.configs)-1].Clone()
			config.Num += 1
			switch op.Val.(type) {
			case JoinArgs:
				args := op.Val.(JoinArgs)
				for gid, servers := range args.Servers {
					config.Groups[gid] = servers
				}
				config.Rebalance()
				sm.configs = append(sm.configs, config)
			case LeaveArgs:
				args := op.Val.(LeaveArgs)
				for _, gid := range args.GIDs {
					delete(config.Groups, gid)
				}
				for idx := 0; idx < len(config.Shards); idx += 1 {
					_, ok := config.Groups[config.Shards[idx]]
					if !ok {
						config.Shards[idx] = 0
					}
				}
				config.Rebalance()
				sm.configs = append(sm.configs, config)
			case MoveArgs:
				args := op.Val.(MoveArgs)
				config.Shards[args.Shard] = args.GID
				sm.configs = append(sm.configs, config)
			}
		}
		sm.appliedIndex = msg.Index
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	reply.WrongLeader = !sm.waitForApplied(op)
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	reply.WrongLeader = !sm.waitForApplied(op)
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		NoOp: false,
		Val:  *args}
	reply.WrongLeader = !sm.waitForApplied(op)
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{NoOp: true}
	reply.WrongLeader = !sm.waitForApplied(op)
	reply.Err = OK
	if !reply.WrongLeader {
		sm.mu.Lock()
		num := args.Num
		if num == -1 {
			num = len(sm.configs) - 1
		}
		reply.Config = sm.configs[num]
		sm.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.appliedIndex = 0

	go sm.apply()
	return sm
}
