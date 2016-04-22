package pbservice

import "net"
import "fmt"
import "net/rpc"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import (
	"math/rand"
  "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("pbservice")

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	view       viewservice.View
	db         map[string]string
	handled    map[int64]bool
}

type MigrateArgs struct {
  Db      map[string]string
  Handled map[int64]bool
}

type MigrateReply struct { }

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me != pb.view.Primary {
    reply.Err = ErrWrongServer
    return nil
  }

  if pb.view.Backup != "" {
    ok := call(pb.view.Backup, "PBServer.FGet", args, reply)
    if !ok || reply.Err != OK {
      return nil
    }
  }

  reply.Value = pb.db[args.Key]
  reply.Err = OK

	return nil
}

func (pb *PBServer) FGet(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me != pb.view.Backup {
    reply.Err = ErrWrongServer
  } else {
    reply.Err = OK
  }

  return nil
}

func (pb *PBServer) FPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
	} else {
		if args.Op == "Put" {
			pb.db[args.Key] = args.Value
			pb.handled[args.Id] = true
			reply.Err = OK
		} else if args.Op == "Append" {
			pb.db[args.Key] = pb.db[args.Key] + args.Value
			pb.handled[args.Id] = true
			reply.Err = OK
		} else {
			logger.Error("unknow operation ", args.Op)
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.handled[args.Id] {
		reply.Err = OK
		return nil
	}

	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	// Primary will only make changes after backup being updated
	if pb.view.Backup != "" {
    ok := call(pb.view.Backup, "PBServer.FPutAppend", args, reply)
		if !ok || reply.Err != OK {
			return nil
		}
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
		pb.handled[args.Id] = true
		reply.Err = OK
	} else if args.Op == "Append" {
		pb.db[args.Key] = pb.db[args.Key] + args.Value
		pb.handled[args.Id] = true
		reply.Err = OK
	} else {
		logger.Error("unknow operation ", args.Op)
	}

	return nil
}

func (pb *PBServer) Migrate(args *MigrateArgs, reply *MigrateReply) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	pb.db = args.Db
  pb.handled = args.Handled

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	var viewnum uint
	if pb.me == pb.view.Primary || pb.me == pb.view.Backup {
		viewnum = pb.view.Viewnum
	} else {
		viewnum = 0
	}

	view, err := pb.vs.Ping(viewnum)
	if err != nil {
		logger.Debug(err)
	}

	// migrate the data if new backup appears
	if view.Backup != "" && pb.view.Backup != view.Backup && pb.me == view.Primary {
    args := MigrateArgs{Db: pb.db, Handled: pb.handled}
    reply := MigrateReply{}

    call(view.Backup, "PBServer.Migrate", &args, &reply)
	}

	pb.view = view
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
	var formatter = logging.MustStringFormatter(`%{color} %{shortfunc} : %{message} %{color:reset}`)

	logging.SetLevel(logging.CRITICAL, "pbservice")
	logging.SetFormatter(formatter)

	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	// Your pb.* initializations here.
	pb.db = make(map[string]string)
	pb.handled = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		logger.Debug("listen error: ", e)
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
