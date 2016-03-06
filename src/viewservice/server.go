package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import (
	"sync/atomic"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	primary string
	backup string
	primaryAck uint
	backupAck uint
	primaryTick uint
	backupTick uint
	viewnum uint
	curTick uint
}

type Server struct {
	id string
	heartBeat int8
}

func (vs *ViewServer) promoteBackup() {
	if vs.backup == "" {
		return
	}

	vs.primary = vs.backup
	vs.backup = ""
	vs.viewnum++
	vs.primaryAck = vs.backupAck
	vs.primaryTick = vs.backupTick
}

func (vs *ViewServer) acked() bool {
	return vs.primaryAck == vs.viewnum
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()

	switch {
	case vs.primary == "":
		vs.primary = args.Me
		vs.primaryTick = vs.curTick
		vs.primaryAck = 0
		vs.viewnum = args.Viewnum + 1
	case args.Me == vs.primary:
		if args.Viewnum == 0 && vs.acked() {
			vs.promoteBackup()
		} else {
			vs.primaryAck = args.Viewnum
			vs.primaryTick = vs.curTick
		}
	case vs.backup == "" && args.Viewnum == 0 && vs.acked():
		vs.backup = args.Me
		vs.viewnum++
		vs.backupTick = vs.curTick
	case vs.backup == args.Me:
    vs.backupTick = vs.curTick
	}

	reply.View = View{vs.viewnum, vs.primary, vs.backup}
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = View{vs.viewnum, vs.primary, vs.backup}
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
	vs.curTick++
	if vs.primary != "" && vs.curTick - vs.primaryTick >= DeadPings && vs.acked() {
		vs.promoteBackup()
	}

	if vs.backup != "" && vs.curTick - vs.backupTick >= DeadPings && vs.acked() {
		vs.backup = ""
		vs.viewnum++
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
	vs.viewnum = 0
	vs.primary = ""
	vs.backup = ""

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
