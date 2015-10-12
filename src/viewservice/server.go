package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

import "strings"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
    curView  View
    newView  View
    recentTime map[string]time.Time // most recent time receive ping
    state    bool // the primary server acknowledge the view or not
}

func initView(view *View) {
    view.Viewnum = 0
    view.Primary = ""
    view.Backup = ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
    vs.mu.Lock()

    // no primary server
    if strings.Compare(vs.curView.Primary, "") == 0 {
        vs.newView.Primary = args.Me
        vs.newView.Viewnum = vs.oldView.Viewnum + 1
        vs.state = false

        reply.View = vs.newView
        vs.recentTime[args.Me] = time.Now()
        vs.mu.Unlock()
        return nil
    }

    // no backup server
    if strings.Compare(vs.curView.Backup, "") == 0 {
        vs.newView.Backup = args.me
        vs.newView.Viewnum = vs.oldView.Viewnum + 1

        reply.View = vs.curView

        vs.recentTime[args.Me] = time.Now()
        vs.mu.Unlock()
        return nil
    }

    // primary server acknowledge
    if strings.Compare(vs.newView.primary, args.Me) == 0 && vs.stat == false {
        vs.curView = vs.newView
        initView(&vs.newView)
        vs.stat = true
        reply.View = curView
        vs.recentTime[args.Me] = time.Now()
        vs.mu.Unlock()
        return nil
    }

    // other situation
    vs.recentTime[args.Me] = time.Now()
    reply.View = vs.curView
    vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    vs.mu.Lock()
    reply.curView
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
    initView(&vs.curView)
    initView(&vs.newView)
    vs.recentTime = make(map[string]time.Time)
    vs.state = false

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
