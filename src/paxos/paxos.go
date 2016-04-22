package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import (
  "math/rand"
  "time"
  "github.com/op/go-logging"
)

var log = logging.MustGetLogger("paxos")

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided Fate = iota + 1
	Pending
	Forgotten // decided but forgotten.
)

type Result int

const (
  Ok Result = iota + 1
  Fail
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances  map[int]*PaxosInstance
}

type Proposal struct {
  Num   string
  Value interface{}
}

type PaxosInstance struct {
  decided Fate
  maxNum string
  accepted Proposal
}

type PrepareArgs struct {
  Seq int    // instance number
  Num string // proposal number
}

type PrepareReply struct {
  Result   Result
  Accepted Proposal
}

type AcceptArgs struct {
  Seq      int
  Proposal Proposal
}

type AcceptReply struct {
  Result Result
}

type DecideArgs struct {
  Seq      int
  Proposal Proposal
}

type DecideReply struct { }

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			log.Debug("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
  go func() {
    px.propose(seq, v)
  }()
}

func (px *Paxos) propose(seq int, v interface{}) {
  num := time.Now().Format(time.ANSIC) + px.peers[px.me]

  for {
    preparedOk := 0
    maxAccepted := Proposal{Num: "0"}

    // send prepare requests to all acceptors
    for i, peer := range px.peers {
      args := PrepareArgs{Seq: seq, Num: num}
      reply := PrepareReply{Result: Fail}

      if i == px.me {
        log.Debug(px.peers[px.me], "send prepare to", peer, args)

        px.ProcessPrepare(&args, &reply)
      } else {
        log.Debug(px.peers[px.me], "send prepare to", peer, args)

        call(peer, "Paxos.ProcessPrepare", &args, &reply)
      }

      if reply.Result == Ok {
        if reply.Accepted.Num > maxAccepted.Num {
          maxAccepted = reply.Accepted
        }
        preparedOk++

        log.Notice(peer, "prepare ok")
      }
    }

    // if prepared number doesn't suffice
    if preparedOk <= len(px.peers) / 2 {
      continue
    }

    acceptedOk := 0

    // send accept requests to all acceptors
    for i, peer := range px.peers {
      args := AcceptArgs{Seq: seq, Proposal: Proposal{Num: num, Value: maxAccepted.Value}}
      reply := AcceptReply{Result: Fail}

      if i == px.me {
        px.ProcessAccept(&args, &reply)
      } else {
        call(peer, "Paxos.ProcessAccept", &args, &reply)
      }

      if reply.Result == Ok {
        acceptedOk++
      }
    }

    // if accepted number doesn't suffice
    if acceptedOk <= len(px.peers) / 2 {
      continue
    }

    for i, peer := range px.peers {
      if i == px.me {
        px.instances[seq].decided = Decided
      } else {
        args := DecideArgs{Seq: seq, Proposal: Proposal{Num: num, Value: maxAccepted.Value}}
        reply := DecideReply{}

        call(peer, "Paxos.ProcessDecide", &args, &reply)
      }
    }
  }
}

func (px *Paxos) ProcessDecide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if _, exist := px.instances[args.Seq]; !exist {
    px.instances[args.Seq] = &PaxosInstance{
      decided: Decided,
      accepted: args.Proposal,
    }
  }

  px.instances[args.Seq].decided = Decided
  px.instances[args.Seq].accepted = args.Proposal

  return nil
}

func (px *Paxos) ProcessAccept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  instance, exist := px.instances[args.Seq]

  if !exist {
    px.instances[args.Seq] = &PaxosInstance{
      decided: Pending,
      maxNum: args.Proposal.Num,
      accepted: Proposal{Num: "0"},
    }

    reply.Result = Ok
  } else if args.Proposal.Num >= instance.maxNum {
    instance.maxNum = args.Proposal.Num
    instance.accepted = args.Proposal

    reply.Result = Ok
  }

  return nil
}

func (px *Paxos) ProcessPrepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  instance, exist := px.instances[args.Seq]

  if !exist {
    // make a new instance and return ok
    px.instances[args.Seq] = &PaxosInstance{
      decided: Pending,
      maxNum: args.Num,
      accepted: Proposal{Num: "0"},
    }

    log.Info("instance created on", px.peers[px.me], px.instances[args.Seq])

    reply.Result = Ok
    reply.Accepted = px.instances[args.Seq].accepted
  } else if args.Num > instance.maxNum {
    // update max_num of the instance
    instance.maxNum = args.Num

    log.Info("instance updated on", px.peers[px.me], px.instances[args.Seq])

    // return ok and accepted proposal if there is one
    reply.Result = Ok
    reply.Accepted = instance.accepted
  }

  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
  max := 0

  for seq, _ := range px.instances {
    if (seq > max) {
      max = seq
    }
  }

	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
  if _, exist := px.instances[seq]; !exist {
    return Pending, nil
  }

  return px.instances[seq].decided, px.instances[seq].accepted
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  var formatter = logging.MustStringFormatter(`%{color} %{shortfunc} : %{message} %{color:reset}`)

  logging.SetLevel(logging.INFO, "paxos")
  logging.SetFormatter(formatter)

	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
  px.instances = make(map[int]*PaxosInstance)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Debug("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
