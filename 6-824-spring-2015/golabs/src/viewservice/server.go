package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "errors"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	recentPing  map[string]time.Time
	currentView View
	nextView    View
	isDown      bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	if vs.isDown {
		vs.mu.Unlock()
		return errors.New("PB service is down.")
	}
	_, ok := vs.recentPing[args.Me]
	if args.Viewnum == 0 {
		if ok {
			// Server failed and restart
			vs.nextView.ServerDied(args.Me)
			vs.nextView.ServerAlive(args.Me)
			if args.Me == vs.nextView.Primary {
				vs.isDown = true
				vs.mu.Unlock()
				return errors.New("PB service is down.")
			}
		} else {
			vs.nextView.ServerAlive(args.Me)
		}
		if vs.nextView.Viewnum == 0 {
			vs.nextView.Viewnum = 1
		}
	} else if args.Viewnum == vs.currentView.Viewnum && args.Viewnum == vs.nextView.Viewnum && args.Me == vs.currentView.Primary {
		vs.nextView.Viewnum++
	}
	// WHen primary acked and some changes happened, nextView will replace currentView
	if vs.currentView.Viewnum+1 == vs.nextView.Viewnum &&
		(vs.currentView.Primary != vs.nextView.Primary || vs.currentView.Backup != vs.nextView.Backup) &&
		(vs.currentView.Viewnum == 0 || vs.nextView.Primary == vs.currentView.Primary || vs.nextView.Primary == vs.currentView.Backup) {
		vs.currentView = vs.nextView
	}
	if ok || args.Viewnum == 0 {
		vs.recentPing[args.Me] = time.Now()
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
	if vs.isDown {
		vs.mu.Unlock()
		return errors.New("PB service is down.")
	}
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
	for key, value := range vs.recentPing {
		du := time.Now().Sub(value)
		if du > DeadPings*PingInterval {
			delete(vs.recentPing, key)
			vs.nextView.ServerDied(key)
		} else {
			vs.nextView.ServerAlive(key)
		}
	}
	if vs.currentView.Viewnum+1 == vs.nextView.Viewnum &&
		(vs.currentView.Primary != vs.nextView.Primary || vs.currentView.Backup != vs.nextView.Backup) &&
		(vs.currentView.Viewnum == 0 || vs.nextView.Primary == vs.currentView.Primary || vs.nextView.Primary == vs.currentView.Backup) {
		vs.currentView = vs.nextView
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
	vs.recentPing = make(map[string]time.Time)
	vs.isDown = false
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
