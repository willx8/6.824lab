package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "log"

// You'll probably need to uncomment these:
import "time"
import "crypto/rand"
import "math/big"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
	id      int64
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.id = nrand() // For debug.
	ck.setNewPrimary()

	return ck
}

func (ck *Clerk) setNewPrimary() {
	ok := false
	var v viewservice.View
	for ok == false {
		//log.Printf("%v: asking for primary", ck.id)
		v, ok = ck.vs.Get()
		time.Sleep(viewservice.PingInterval)
	}
	log.Printf("%v: Primary set: %s", ck.id, v.Primary)
	ck.primary = v.Primary
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if ck.primary == "" {
		ck.setNewPrimary()
	}
	args := &GetArgs{key,
		false, /* not from primary but client */
		nrand()}
	var reply GetReply
	ok := false
	fails := 0
	for ok == false {
		ok = call(ck.primary, "PBServer.Get", args, &reply)
		//log.Printf("Get [%s] from %s..%s..ok:%b", key, ck.primary, reply.Err, ok)
		time.Sleep(viewservice.PingInterval)
		if ok && (reply == GetReply{}) {
			// empty reply due to unreliable network
			DPrintf("%v: Miss get reply, require again.\n", args.Id)
			ok = false
		}
		fails++
		if fails == viewservice.DeadPings {
			// unable to reach primary for such a long time, we assume it
			// to be dead and thus require the server for the new primary.
			ck.setNewPrimary()
			fails = 0
		}
	}
	DPrintf("Got {%s:%s}, err:%s\n", key, reply.Value, reply.Err)
	if reply.Err == OK {
		return reply.Value
	} else if reply.Err == ErrNoKey {
		return ""
	} else {
		fmt.Errorf("Get error")
		return "???"
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// Your code here.
	DPrintf("%v: Put {%s,%s}\n", ck.id, key, value)
	if ck.primary == "" {
		log.Printf("Put: primary null, ask for a new primary")
		ck.setNewPrimary()
	}
	args := &PutArgs{key, value, dohash,
		false, /* not from primary but client */
		nrand()}
	var reply PutReply
	ok := false
	fails := 0
	for ok == false {
		ok = call(ck.primary, "PBServer.Put", args, &reply)
		time.Sleep(viewservice.PingInterval)
		if ok && (reply == PutReply{}) {
			// empty reply due to unreliable network
			log.Printf("%v: Miss put reply, require again.", args.Id)
			ok = false
		}
		fails++
		if fails == viewservice.DeadPings {
			// unable to reach primary for such a long time, we assume it
			// to be dead and thus require the server for the new primary.
			ck.setNewPrimary()
			fails = 0
		}
	}
	if reply.Err == OK {
		return reply.PreviousValue
	} else {
		fmt.Printf("Put error %s", reply.Err)
		return "???"
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
