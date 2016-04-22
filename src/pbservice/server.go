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
	ready      bool
	handled    map[int64]bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	reply.Value = pb.db[args.Key]
	reply.Err = OK

	return nil
}

func (pb *PBServer) Forward(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	logger.Debug(pb.me, "Start handling forwarded request")

	if pb.me != pb.view.Backup || !pb.ready {
		logger.Debug(pb.me, "Forwarding request being sent to a non-backup node")

		reply.Err = ErrWrongServer
	} else {
		logger.Debug(pb.me, "Start updating the database")

		if args.Op == "Put" {
			pb.db[args.Key] = args.Value
			pb.handled[args.Id] = true
			reply.Err = OK
		} else if args.Op == "Append" {
			pb.db[args.Key] = pb.db[args.Key] + args.Value
			pb.handled[args.Id] = true
			reply.Err = OK
		} else {
			logger.Error("Unknow operation ", args.Op)
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.handled[args.Id] {
		logger.Debug(pb.me, "Request already been handled")
		reply.Err = OK
		return nil
	}

	if pb.me != pb.view.Primary {
		logger.Debug(pb.me, "PutAppend being sent to a non-primary node")
		reply.Err = ErrWrongServer
		return nil
	}

	// Primary will only make changes after backup being updated
	if pb.view.Backup != "" {
		logger.Debug(pb.me, "Start forwarding PutAppend to the backup node", pb.view.Backup)

    ok := call(pb.view.Backup, "PBServer.Forward", args, reply)
		if !ok || reply.Err != OK {
			logger.Debug(pb.me, "Failed in forwarding the request", pb.view.Backup)
			return nil
		}
	}

	logger.Debug(pb.me, "Start updating the database")
	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
		pb.handled[args.Id] = true
		reply.Err = OK
	} else if args.Op == "Append" {
		pb.db[args.Key] = pb.db[args.Key] + args.Value
		pb.handled[args.Id] = true
		reply.Err = OK
	} else {
		logger.Error("Unknow operation ", args.Op)
	}

	return nil
}

func (pb *PBServer) MigrateDB(args bool, reply *map[string]string) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	logger.Debug(pb.me, "Start handling migration request")
	*reply = pb.db

	// Update the view
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		logger.Error(err)
	}

	pb.view = view

	return nil
}

func (pb *PBServer) MigrateHandled(args bool, reply *map[int64]bool) error {
	pb.mu.Lock()
  defer pb.mu.Unlock()

	logger.Debug(pb.me, "Start handling migration request")
	*reply = pb.handled

	return nil
}

func retry(fn func() bool) {
	for {
		ok := fn()
		if ok {
			break
		} else {
			time.Sleep(viewservice.PingInterval)
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
		logger.Error(err)
	}

	logger.Debug(pb.me, "tick", view.Primary, view.Backup)

	// pb who isn't the backup in last view has become the backup in current view
	if pb.me != pb.view.Backup && pb.me == view.Backup {
		logger.Debug(pb.me, "Start migration", view.Primary, view.Backup, pb.me)
		retry(func () bool {
			return call(view.Primary, "PBServer.MigrateDB", true, &pb.db)
		})
		retry(func() bool {
			return call(view.Primary, "PBServer.MigrateHandled", true, &pb.handled)
		})
    pb.ready = true

		logger.Debug(pb.me, "Migration finished", pb.db, pb.handled)
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
	pb.ready = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		logger.Error("listen error: ", e)
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
