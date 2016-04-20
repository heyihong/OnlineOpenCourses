package kvpaxos

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq       int
	idToValue map[int64]*string
	kv        map[string]string
}

func (kv *KVPaxos) UpdateByPaxosLog() {
	kv.mu.Lock()
	maxSeq := kv.px.Max()
	for ; kv.seq+1 <= maxSeq; kv.seq++ {
		_, v := kv.px.Status(kv.seq + 1)
		switch v.(type) {
		case GetArgs:
			args := v.(GetArgs)
			value, containsKey := kv.kv[args.Key]
			if containsKey {
				kv.idToValue[args.Id] = &value
			} else {
				kv.idToValue[args.Id] = nil
			}
		case PutAppendArgs:
			args := v.(PutAppendArgs)
			switch args.Op {
			case "Put":
				kv.kv[args.Key] = args.Value
				kv.idToValue[args.Id] = nil
			case "Append":
				value, containsKey := kv.kv[args.Key]
				if containsKey {
					kv.kv[args.Key] = value + args.Value
				} else {
					kv.kv[args.Key] = args.Value
				}
				kv.idToValue[args.Id] = nil
			}
		}

	}
	kv.px.Done(kv.seq)
	kv.mu.Unlock()
}

func (kv *KVPaxos) HandleOp(op interface{}) {
	// It's possible that a kv server handles a bunch of Get and PutAppend requests.
	// However, in order to reduce useless rpc connection, the program only allow
	// only one request to start Paxos instance at a time.
	kv.mu.Lock()
	availSeq := kv.seq + 1
	kv.px.Start(availSeq, op)
	to := 10 * time.Millisecond
	for {
		status, _ := kv.px.Status(availSeq)
		if status == paxos.Decided {
			kv.mu.Unlock()
			return
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	for {
		kv.UpdateByPaxosLog()
		kv.mu.Lock()
		result, ok := kv.idToValue[args.Id]
		kv.mu.Unlock()
		if !ok {
			kv.HandleOp(*args)
		} else {
			if result == nil {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Err = OK
				reply.Value = *result
			}
			break
		}
	}
	delete(kv.idToValue, args.Id)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	for {
		kv.UpdateByPaxosLog()
		kv.mu.Lock()
		_, ok := kv.idToValue[args.Id]
		kv.mu.Unlock()
		if !ok {
			kv.HandleOp(*args)
		} else {
			reply.Err = OK
			break
		}
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = -1
	kv.idToValue = make(map[int64]*string)
	kv.kv = make(map[string]string)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
