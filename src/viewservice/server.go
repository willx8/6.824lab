package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "container/list"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	lastHeard map[string]time.Time
	alive     map[string]bool
	v         View
	tempv     View // temp view
	pAcked    bool // whether current primary has acknowledged
	bAcked    bool
	idles     *list.List
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	prev := vs.v
	defer func() {
		vs.alive[args.Me] = true
		if prev != vs.v {
			vs.pAcked = false
			vs.bAcked = false
		}
		reply.View = vs.v
	}()
	vs.lastHeard[args.Me] = time.Now()

	if args.Me == vs.v.Primary {
		if args.Viewnum == vs.v.Viewnum {
			vs.pAcked = true
		} else {
			if args.Viewnum != 0 {
				// consider it an invalid call. return current view.
				return nil
			}
			if vs.bAcked {
				vs.v = View{vs.v.Viewnum + 1, vs.v.Backup, args.Me}
			} else {
				// TODO implement this
				if i := vs.idleServer(); i != "" {
					vs.v = View{vs.v.Viewnum + 1, i, args.Me}
				}
			}
		}
	} else if args.Me == vs.v.Backup {
		if args.Viewnum == vs.v.Viewnum {
			vs.bAcked = true
		} else {
			if args.Viewnum != 0 {
				// consider it an invalid call. return current view.
				return nil
			}
			if vs.pAcked {
				vs.v.Viewnum = vs.v.Viewnum + 1
			}
		}
	} else {
		if vs.v.Primary == "" {
			// central meeting decision: you be primary.
			// 中央已经开会决定了，你来当primary
			vs.v = View{vs.v.Viewnum + 1, args.Me, ""}
			return nil
		} else if vs.v.Backup == "" {
			vs.v = View{vs.v.Viewnum + 1, vs.v.Primary, args.Me}
		} else {
			l, ok := vs.alive[args.Me]
			if ok == false || l == false { // not exist or dead
				vs.idles.PushBack(args.Me)
			}
		}
	}
	// the primary from the current view acknowledges that
	// it is operating in the current view
	if vs.pAcked && vs.tempv.Viewnum > vs.v.Viewnum {
		if vs.tempv.Viewnum != vs.v.Viewnum+1 {
			return fmt.Errorf("incorrect temp viewnum %v", vs.tempv.Viewnum)
		}
		vs.v = vs.tempv
	}
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if vs.dead {
		return fmt.Errorf("viewservice is dead")
	}
	reply.View = vs.v
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	t := time.Now()
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.v.Primary != "" &&
		t.Sub(vs.lastHeard[vs.v.Primary]) > DeadPings*PingInterval {
		vs.alive[vs.v.Primary] = false
		if vs.bAcked { // promote backup
			vs.tempv = View{vs.v.Viewnum + 1, vs.v.Backup, vs.idleServer()}
		} else { // no backup
			vs.tempv = View{vs.v.Viewnum + 1, vs.idleServer(), vs.idleServer()}
		}
	} else if vs.v.Backup != "" &&
		t.Sub(vs.lastHeard[vs.v.Backup]) > DeadPings*PingInterval {
		vs.alive[vs.v.Backup] = false
		vs.tempv = View{vs.v.Viewnum + 1, vs.v.Primary, vs.idleServer()}
	}
	// maintain the active idle servers.
	for i := vs.idles.Front(); i != nil; i = i.Next() {
		if t.Sub(vs.lastHeard[i.Value.(string)]) > DeadPings*PingInterval {
			vs.alive[i.Value.(string)] = false
			vs.idles.Remove(i)
		}
	}
}

func (vs *ViewServer) idleServer() string {
	i := vs.idles.Front()
	if i == nil {
		return ""
	}
	vs.idles.Remove(i)
	return i.Value.(string)
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.v = View{0, "", ""}
	vs.tempv = View{0, "", ""}
	vs.idles = list.New()
	vs.lastHeard = make(map[string]time.Time)
	vs.alive = make(map[string]bool)

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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
