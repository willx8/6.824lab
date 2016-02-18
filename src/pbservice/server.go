package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	view  viewservice.View
	kv    map[string]string
	isP   bool
	raw   bool               //whether server has been initialized
	putId map[int64]PutReply // all ids we have seen
	getId map[int64]GetReply // all ids we have seen
	mu    sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	DPrintf("Put: %s RPC received %v\n", pb.me, args.Id)
	if r, ok := pb.putId[args.Id]; ok {
		*reply = r
		DPrintf("Duplicate put! Previous reply:{%s, %v}\n",
			reply.Err, reply.PreviousValue)
		return nil
	}
	if args.FromPrimary || pb.isP {
		// Deal with put.
		if args.DoHash {
			reply.PreviousValue = ""
			if _, ok := pb.kv[args.Key]; ok {
				reply.PreviousValue = pb.kv[args.Key]
			}
			pb.kv[args.Key] = strconv.Itoa(
				int(hash(reply.PreviousValue + args.Value)))
		} else {
			pb.kv[args.Key] = args.Value
		}
		reply.Err = OK

		defer func() {
			pb.putId[args.Id] = *reply
		}()
	}

	if args.FromPrimary {
		// Backup deal with get. If heard a new view, reject it.
		if pb.isP {
			reply.Err = ErrWrongServer
		}
		return nil
	} else if pb.isP {
		if pb.view.Backup != "" {
			myReply := PutReply{}
			args.FromPrimary = true

			// call until success
			ok := false
			for ok == false {
				ok = call(pb.view.Backup, "PBServer.Put", args, &myReply)
				time.Sleep(viewservice.PingInterval)
				if ok && (myReply == PutReply{}) {
					// empty reply due to unreliable network
					log.Printf("%v: Miss put reply, require again.", args.Id)
					ok = false
				}
			}

			if myReply.Err == ErrWrongServer {
				pb.isP = false
				return fmt.Errorf("nolonger primary")
			}
		}
		return nil
	} else {
		// A case which is not regarded as duplicate.
		return fmt.Errorf("Unexpected put request!")
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	DPrintf("Get: %s RPC received %v\n", pb.me, args.Id)

	// Check for duplicates.
	if r, ok := pb.getId[args.Id]; ok {
		*reply = r
		DPrintf("Duplicate get request!\n")
		return nil
	}
	if args.FromPrimary || pb.isP {
		defer func() {
			pb.getId[args.Id] = *reply
		}()
	}

	if args.FromPrimary {
		// Backup deal with get. If heard a new view, reject it.
		if pb.isP {
			reply.Err = ErrWrongServer
			reply.Value = pb.kv[args.Key]
		} else {
			reply.Err = OK
		}
		return nil
	} else if pb.isP {
		if pb.view.Backup != "" {
			// Just as Put, Get needs to be forwarded to backup. Thus,
			// if backup has heard a newer view, it can reject this request.
			myReply := GetReply{}
			args.FromPrimary = true
			DPrintf("Form backup %s.\n", pb.view.Backup)

			// call until success
			ok := false
			for ok == false {
				ok = call(pb.view.Backup, "PBServer.Get", args, &myReply)
				time.Sleep(viewservice.PingInterval)
				if ok && (myReply == GetReply{}) {
					// empty reply due to unreliable network
					DPrintf("%v: Miss get reply, require again.\n", args.Id)
					ok = false
				}
			}

			DPrintf("Reply from backup: %s.\n", myReply.Err)
			if myReply.Err == ErrWrongServer {
				log.Printf("No longer primary, new value:%d", myReply.Value)
				reply.Err = ErrWrongServer
				return nil
			}
		}
		var ok bool
		reply.Value, ok = pb.kv[args.Key]
		if ok {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		return nil
	} else {
		// If a server is not primary nor did it get a request from primary,
		// then an error occurs
		return fmt.Errorf("Unexpected get request!")
	}
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.

	v, e := pb.vs.Ping(pb.view.Viewnum)
	if e != nil {
		fmt.Errorf("erro when ping the server:%v", e)
	}
	if pb.raw && (pb.me == v.Backup) {
		// initialization
		e := pb.SetNewBackup(v)
		if e != nil {
			log.Print(e)
		} else {
			pb.raw = false
		}
	}
	pb.view = v
	pb.isP = (pb.view.Primary == pb.me)
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

// new functions: send/receive backup RPC
func (pb *PBServer) SetNewBackup(v viewservice.View) error {
	args := &NewBackupArgs{v}
	var reply NewBackupReply
	ok := call(args.View.Primary, "PBServer.NewBackup", args, &reply)
	if ok == false {
		return fmt.Errorf("Can't setup new backup %s\n", pb.me)
	} else {
		log.Printf("New backup set.")
		pb.kv = reply.Data
		pb.putId = reply.PutId
		pb.getId = reply.GetId
		return nil
	}
}

func (pb *PBServer) NewBackup(args *NewBackupArgs, reply *NewBackupReply) error {
	if pb.view.Viewnum < args.View.Viewnum {
		pb.view = args.View
	}
	reply.Data = make(map[string]string)
	reply.PutId = make(map[int64]PutReply)
	reply.GetId = make(map[int64]GetReply)

	for k, v := range pb.kv {
		reply.Data[k] = v
	}
	for k, v := range pb.putId {
		reply.PutId[k] = v
	}
	for k, v := range pb.getId {
		reply.GetId[k] = v
	}
	return nil
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.kv = make(map[string]string)
	pb.putId = make(map[int64]PutReply)
	pb.getId = make(map[int64]GetReply)
	pb.raw = true
	pb.view = viewservice.View{0, "", ""}

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
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
