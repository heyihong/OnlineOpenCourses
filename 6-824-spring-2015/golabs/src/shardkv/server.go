package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "../paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "../shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Me   int
	Id   int
	Args interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	isInit      bool
	seq         int
	nextOpId    int
	configNums  [shardmaster.NShards]int
	kvShards    [shardmaster.NShards]map[string]string
	replyShards [shardmaster.NShards]map[int64]PutAppendReply
}

// Non-thread safe
// Prove all logs whose seq <= sync seq are executed
// return reply of last executed log in the function
func (kv *ShardKV) SyncPaxosLogs(seq int) interface{} {
	var reply interface{}
	for ; kv.seq <= seq; kv.seq += 1 {
		_, o := kv.px.Status(kv.seq)
		args := o.(Op).Args
		switch args.(type) {
		case GetArgs:
			getArgs := args.(GetArgs)
			shardId := key2shard(getArgs.Key)
			if kv.kvShards[shardId] != nil {
				value, ok := kv.kvShards[shardId][getArgs.Key]
				if ok {
					reply = GetReply{
						Err:   OK,
						Value: value}
				} else {
					reply = GetReply{
						Err: ErrNoKey}
				}
			} else {
				reply = GetReply{
					Err: ErrWrongGroup}
			}
		case PutAppendArgs:
			putAppendArgs := args.(PutAppendArgs)
			shardId := key2shard(putAppendArgs.Key)
			if kv.kvShards[shardId] != nil {
				rep, ok := kv.replyShards[shardId][putAppendArgs.Id]
				if !ok {
					value, ok := kv.kvShards[shardId][putAppendArgs.Key]
					if putAppendArgs.Op == "Put" || !ok {
						value = putAppendArgs.Value
					} else {
						value += putAppendArgs.Value
					}
					reply = PutAppendReply{Err: OK}
					kv.kvShards[shardId][putAppendArgs.Key] = value
					kv.replyShards[shardId][putAppendArgs.Id] = reply.(PutAppendReply)
				} else {
					reply = rep
				}
			} else {
				reply = PutAppendReply{
					Err: ErrWrongGroup}
			}
		case MoveInArgs:
			moveInArgs := args.(MoveInArgs)
			kv.configNums[moveInArgs.ShardId] = moveInArgs.ConfigNum
			kv.kvShards[moveInArgs.ShardId] = moveInArgs.Shard
			kv.replyShards[moveInArgs.ShardId] = moveInArgs.Reply
		case MoveOutArgs:
			moveOutArgs := args.(MoveOutArgs)
			moveInArgs := MoveInArgs{
				ConfigNum: moveOutArgs.ConfigNum,
				ShardId:   moveOutArgs.ShardId,
				Shard:     kv.kvShards[moveOutArgs.ShardId],
				Reply:     kv.replyShards[moveOutArgs.ShardId]}
			go func(servers []string, args MoveInArgs) {
				var reply MoveInReply
				for {
					for _, server := range servers {
						if call(server, "ShardKV.MoveIn", args, &reply) {
							return
						}
					}
				}
			}(moveOutArgs.Servers, moveInArgs)
			kv.kvShards[moveOutArgs.ShardId] = nil
			kv.replyShards[moveOutArgs.ShardId] = nil
		}
	}
	kv.px.Done(seq)
	return reply
}

// Non-thread safe
// if seq == -1 then append until success
// else append to specific position
// return append position, if value is -1, it means failure append
// failure append only occurs when seq != -1
func (kv *ShardKV) AppendPaxosLog(appendSeq int, args interface{}) int {
	op := Op{
		Me:   kv.me,
		Id:   kv.nextOpId,
		Args: args}
	kv.nextOpId += 1
	for {
		var seq int
		if appendSeq == -1 {
			seq = kv.px.Max() + 1
		} else {
			seq = appendSeq
		}
		kv.px.Start(seq, op)
		to := 10 * time.Millisecond
		for {
			status, v := kv.px.Status(seq)
			if status == paxos.Decided {
				if v.(Op).Me == kv.me && v.(Op).Id == op.Id {
					return seq
				}
				if appendSeq != -1 {
					return -1
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := kv.SyncPaxosLogs(kv.AppendPaxosLog(-1, *args))
	reply.Err = res.(GetReply).Err
	reply.Value = res.(GetReply).Value
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := kv.SyncPaxosLogs(kv.AppendPaxosLog(-1, *args))
	reply.Err = res.(PutAppendReply).Err
	return nil
}

func (kv *ShardKV) MoveIn(args *MoveInArgs, reply *MoveInReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		appendSeq := kv.px.Max() + 1
		kv.SyncPaxosLogs(appendSeq - 1)
		// The move in args is out dated
		if kv.configNums[args.ShardId] >= args.ConfigNum {
			break
		}
		// The move in args has been appended to the paxos log
		if kv.AppendPaxosLog(appendSeq, *args) != -1 {
			break
		}
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.sm.Query(-1)
	if config.Num > 0 && !kv.isInit {
		config = kv.sm.Query(1)
		kv.isInit = true
	}
	ok := false
	for !ok {
		ok = true
		appendSeq := kv.px.Max() + 1
		kv.SyncPaxosLogs(appendSeq - 1)
		if config.Num == 1 {
			for shardId := 0; shardId < shardmaster.NShards; shardId += 1 {
				if config.Shards[shardId] == kv.gid && kv.kvShards[shardId] == nil && kv.configNums[shardId] < config.Num {
					ok = false
					args := MoveInArgs{
						ConfigNum: 1,
						ShardId:   shardId,
						Shard:     make(map[string]string),
						Reply:     make(map[int64]PutAppendReply)}
					kv.AppendPaxosLog(appendSeq, args)
					break
				}
			}
		} else {
			for shardId := 0; shardId < shardmaster.NShards; shardId += 1 {
				if kv.kvShards[shardId] != nil && config.Shards[shardId] != kv.gid && kv.configNums[shardId] < config.Num {
					ok = false
					args := MoveOutArgs{
						ShardId:   shardId,
						ConfigNum: config.Num,
						Servers:   config.Groups[config.Shards[shardId]]}
					kv.AppendPaxosLog(appendSeq, args)
					break
				}
			}
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(MoveInArgs{})
	gob.Register(MoveOutArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.seq = 0
	kv.nextOpId = 0
	kv.isInit = false

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
