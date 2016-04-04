package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "../viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	kv                 map[string]string
	idToGetReply       map[int64]GetReply
	idToPutAppendReply map[int64]PutAppendReply

	currentView viewservice.View
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	pb.kv = args.KeyValueMap
	pb.idToGetReply = args.IdToGetReply
	pb.idToPutAppendReply = args.IdToPutAppendReply
	reply.Err = OK
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.currentView.Primary == pb.me {
		getReply, containsId := pb.idToGetReply[args.Id]
		if containsId {
			reply.Value, reply.Err = getReply.Value, getReply.Err
		} else {
			value, containsKey := pb.kv[args.Key]
			if containsKey {
				reply.Value, reply.Err = value, OK
			} else {
				reply.Value, reply.Err = "", ErrNoKey
			}
			pb.idToGetReply[args.Id] = *reply
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.currentView.Primary == pb.me || args.IsBackup {
		putAppendReply, containsId := pb.idToPutAppendReply[args.Id]
		if containsId {
			reply.Err = putAppendReply.Err
		} else {
			ok := true
			if !args.IsBackup {
				args.IsBackup = true
				if pb.currentView.Primary == pb.me && pb.currentView.Backup != "" {
					var backupReply PutAppendReply
					ok = call(pb.currentView.Backup, "PBServer.PutAppend", args, &backupReply)
				}
			}
			if ok {
				if args.Op == "Put" {
					pb.kv[args.Key] = args.Value
				} else if args.Op == "Append" {
					value, containsKey := pb.kv[args.Key]
					if containsKey {
						pb.kv[args.Key] = value + args.Value
					} else {
						pb.kv[args.Key] = args.Value
					}
				}
				reply.Err = OK
				pb.idToPutAppendReply[args.Id] = *reply
			} else {
				reply.Err = ErrBackupFailed
			}
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	view, _ := pb.vs.Ping(pb.currentView.Viewnum)
	if pb.me == view.Primary && pb.currentView.Backup != view.Backup {
		args := &TransferArgs{}
		args.KeyValueMap = pb.kv
		args.IdToGetReply = pb.idToGetReply
		args.IdToPutAppendReply = pb.idToPutAppendReply
		var reply TransferReply
		call(view.Backup, "PBServer.Transfer", args, &reply)
	}
	pb.currentView = view
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kv = make(map[string]string)
	pb.idToGetReply = make(map[int64]GetReply)
	pb.idToPutAppendReply = make(map[int64]PutAppendReply)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
