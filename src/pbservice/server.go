package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
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
    data       map[string]string
    view       viewservice.View
    init       bool
    opid       map[string]int64
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    pb.mu.Lock()
    if pb.view.Primary != pb.me {
        reply.Err = ErrWrongServer
        reply.Value = ""
    } else {
        key := args.Key
        if value, exist := pb.data[key]; exist {
            reply.Err = OK
            reply.Value = value
        } else {
            reply.Err = ErrNoKey
            reply.Value = ""
        }
    }
    pb.mu.Unlock()
	return nil
}

// server do put operation
func (pb *PBServer) doPutOperation(args *PutAppendArgs, reply *PutAppendReply) {
    switch {
    case args.Op == Put:
        pb.data[args.Key] = args.Value
        pb.opid[args.Key] = args.Opid
        reply.Err = OK
    case args.Op == Append:
        _, exist := pb.data[args.Key]
        if exist {
            if pb.opid[args.Key] != args.Opid {
                pb.data[args.Key] += args.Value
                pb.opid[args.Key] = args.Opid
            }
            reply.Err = OK
        } else {
            pb.data[args.Key] = args.Value
            pb.opid[args.Key] = args.Opid
            reply.Err = OK
        }
    }
}

// primary server forward update to backup server
func (pb *PBServer) forwardToBackup(args *PutAppendArgs) {
    if pb.view.Backup == "" {
        return
    }
    args.Client = pb.me
    var reply PutAppendReply
    for {
        ok := call(pb.view.Backup, "PBServer.PutAppend", args, &reply)
        if ok && reply.Err == OK {
            break;
        } else {
            time.Sleep(viewservice.PingInterval)
            pb.view, _ = pb.vs.Get()
            if pb.view.Backup == "" {
                break
            }
        }
    }
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    pb.mu.Lock()
    if pb.view.Primary == pb.me {
        pb.doPutOperation(args, reply)
        pb.forwardToBackup(args)
    } else {
        if args.Client == pb.view.Primary {
            pb.doPutOperation(args, reply)
        }
    }
    pb.mu.Unlock()
	return nil
}

func (pb *PBServer) SynData(args *SynArgs, reply *SynReply) error {
    pb.mu.Lock()
    pb.data = args.Data
    reply.Err = OK
    pb.mu.Unlock()
    return nil
}


func (pb *PBServer) synPrimaryToBackup(backup string) {
    Args := &SynArgs{pb.data}
    var reply SynReply
    for {
        ok := call(backup, "PBServer.SynData", Args, &reply)
        if ok {
            break
        }
    }
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
    if view, err := pb.vs.Ping(pb.view.Viewnum); err == nil {
        if view.Primary == pb.me && view.Backup != "" && pb.view.Backup == "" {
            pb.synPrimaryToBackup(view.Backup)
        }
        pb.view = view
    } else {
        pb.view = viewservice.View{}
    }
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
    pb.data = make(map[string]string)
    pb.opid = make(map[string]int64)
    pb.init = false
    pb.view, _= pb.vs.Get()


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
