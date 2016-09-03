package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "../paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	seq      int
	nextOpId int
}

type Op struct {
	// Your data here.
	Me   int
	Id   int
	Args interface{}
}

func (config *Config) Rebalance() {
	numAve := (NShards + len(config.Groups) - 1) / len(config.Groups)
	remain := NShards % len(config.Groups)
	count := make(map[int64]int)
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

func (config *Config) Clone() Config {
	c := Config{}
	c.Num = config.Num
	c.Groups = make(map[int64][]string)
	for idx, shard := range config.Shards {
		c.Shards[idx] = shard
	}
	for key, value := range config.Groups {
		c.Groups[key] = value
	}
	return c
}

func (sm *ShardMaster) Update(args interface{}) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{
		Me:   sm.me,
		Id:   sm.nextOpId,
		Args: args}
	sm.nextOpId += 1
	for {
		seq := sm.px.Max() + 1
		sm.px.Start(seq, op)
		to := 10 * time.Millisecond
		for {
			status, v := sm.px.Status(seq)
			if status == paxos.Decided {
				var reply interface{}
				for ; sm.seq <= seq; sm.seq += 1 {
					_, o := sm.px.Status(sm.seq)
					args := o.(Op).Args
					config := sm.configs[len(sm.configs)-1].Clone()
					config.Num += 1
					switch args.(type) {
					case JoinArgs:
						joinArgs := args.(JoinArgs)
						config.Groups[joinArgs.GID] = joinArgs.Servers
						config.Rebalance()
						sm.configs = append(sm.configs, config)
					case LeaveArgs:
						leaveArgs := args.(LeaveArgs)
						for idx := 0; idx < len(config.Shards); idx += 1 {
							if config.Shards[idx] == leaveArgs.GID {
								config.Shards[idx] = 0
							}
						}
						delete(config.Groups, leaveArgs.GID)
						config.Rebalance()
						sm.configs = append(sm.configs, config)
					case MoveArgs:
						moveArgs := args.(MoveArgs)
						config.Shards[moveArgs.Shard] = moveArgs.GID
						sm.configs = append(sm.configs, config)
					case QueryArgs:
						queryArgs := args.(QueryArgs)
						if queryArgs.Num == -1 {
							queryArgs.Num = len(sm.configs) - 1
						}
						reply = QueryReply{sm.configs[queryArgs.Num]}
					}
				}
				sm.px.Done(seq)
				if v.(Op).Me == sm.me && v.(Op).Id == op.Id {
					return reply
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

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.Update(*args)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.Update(*args)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.Update(*args)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	rep := sm.Update(*args).(QueryReply)
	reply.Config = rep.Config
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
