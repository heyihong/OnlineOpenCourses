package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	primaryPing   time.Time
	backupPing    time.Time
	currentView   View
	isPrimayAcked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	switch args.Me {
	case vs.currentView.Primary:
		if args.Viewnum == 0 && vs.isPrimayAcked {
			// Primary crashed and rebooted
			vs.currentView.Viewnum += 1
			vs.currentView.Primary = vs.currentView.Backup
			vs.primaryPing = vs.backupPing
			vs.currentView.Backup = args.Me
			vs.backupPing = time.Now()
			vs.isPrimayAcked = false
		} else {
			vs.primaryPing = time.Now()
			if args.Viewnum == vs.currentView.Viewnum {
				vs.isPrimayAcked = true
			}
		}
	case vs.currentView.Backup:
		vs.backupPing = time.Now()
	default:
		if vs.isPrimayAcked || vs.currentView.Viewnum == 0 {
			if vs.currentView.Primary == "" {
				vs.currentView.Viewnum += 1
				vs.currentView.Primary = args.Me
				vs.primaryPing = time.Now()
				vs.isPrimayAcked = false
			} else if vs.currentView.Backup == "" {
				vs.currentView.Viewnum += 1
				vs.currentView.Backup = args.Me
				vs.backupPing = time.Now()
				vs.isPrimayAcked = false
			}
		}
	}
	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	if vs.isPrimayAcked {
		if time.Now().Sub(vs.primaryPing) > DeadPings*PingInterval {
			vs.currentView.Viewnum += 1
			vs.currentView.Primary = vs.currentView.Backup
			vs.primaryPing = vs.backupPing
			vs.currentView.Backup = ""
			vs.isPrimayAcked = false
		} else if vs.currentView.Backup != "" && time.Now().Sub(vs.backupPing) > DeadPings*PingInterval {
			vs.currentView.Viewnum += 1
			vs.currentView.Backup = ""
			vs.isPrimayAcked = false

		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{
		Viewnum: 0,
		Primary: "",
		Backup:  ""}
	vs.isPrimayAcked = false
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
