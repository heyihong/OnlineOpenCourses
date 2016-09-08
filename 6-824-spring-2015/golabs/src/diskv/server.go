package diskv

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
import "encoding/base32"
import "math/rand"
import "../shardmaster"
import "io/ioutil"
import "strconv"

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

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	isInit      bool
	seq         int
	nextOpId    int
	configNums  [shardmaster.NShards]int
	kvShards    [shardmaster.NShards]map[string]string
	replyShards [shardmaster.NShards]map[int64]PutAppendReply
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

// Non-thread safe
// Prove all logs whose seq <= sync seq are executed
// return reply of last executed log in the function
func (kv *DisKV) SyncPaxosLogs(seq int) interface{} {
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
						if call(server, "DisKV.MoveIn", args, &reply) {
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
func (kv *DisKV) AppendPaxosLog(appendSeq int, args interface{}) int {
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

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := kv.SyncPaxosLogs(kv.AppendPaxosLog(-1, *args))
	reply.Err = res.(GetReply).Err
	reply.Value = res.(GetReply).Value
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := kv.SyncPaxosLogs(kv.AppendPaxosLog(-1, *args))
	reply.Err = res.(PutAppendReply).Err
	return nil
}

func (kv *DisKV) MoveIn(args *MoveInArgs, reply *MoveInReply) error {
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
func (kv *DisKV) tick() {
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
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	// Don't call Join().
	kv.seq = 0
	kv.nextOpId = 0
	kv.isInit = false

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(MoveInArgs{})
	gob.Register(MoveOutArgs{})
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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
