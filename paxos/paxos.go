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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
//import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  totalPeers int
  quorum int
  instances []*PaxosInstance
  startSeq []int
  minStartSeq int
  insLock sync.RWMutex
  startSeqLock sync.Mutex
}

type PaxosInstance struct {
    lock sync.Mutex
    maxPropose int
    maxPrepare int
    maxAccept int
    decided bool
    value interface{}
}

type PrepareArgs struct {
    Seq int
    Num int
    Peer int
    StartSeq int
}

type AcceptArgs struct {
    Seq int
    Num int
    Value interface{}
}

type DecideArgs struct {
    Seq int
    Value interface{}
}

type PaxosReply struct {
    Ok bool
    Decided bool
    MaxNum int
    StartSeq int
    Value interface{}
}

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
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  //c, err := rpc.Dial("unix", srv)
  c, err := rpc.Dial("tcp", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
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
  //
  //
  fmt.Printf("> %v\n", seq)
  instance := px.getInstance(seq)
  go func() {
    for !px.propose(instance, seq, v) {
        if px.dead {
            break
        }
    }
  }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  //
  px.peerDone(px.me, seq + 1)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.minStartSeq + len(px.instances) - 1
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
  return px.minStartSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (decided bool, v interface{}) {
  // Your code here.
  instance := px.getInstance(seq)
  if instance == nil {
    return true, 0
  }
  instance.lock.Lock()
  decided, v = instance.decided, instance.value
  instance.lock.Unlock()
  return
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

func (px *Paxos) peerDone(peer int, peerStartSeq int) {
    px.mu.Lock()
    if peerStartSeq > px.startSeq[peer] {
        if px.startSeq[peer] == px.minStartSeq {
            px.startSeq[peer] = peerStartSeq
            oldMin := px.minStartSeq
            px.minStartSeq = peerStartSeq
            for i := 0; i < px.totalPeers; i++ {
                if px.startSeq[i] < px.minStartSeq {
                    px.minStartSeq = px.startSeq[i]
                }
            }
            if px.minStartSeq > oldMin {
                for i := 0; i < px.minStartSeq - oldMin; i++ {
                    px.instances[i] = nil
                }
                px.instances = px.instances[px.minStartSeq - oldMin:]
            }
        } else {
            px.startSeq[peer] = peerStartSeq
        }
    }
    px.mu.Unlock()
}

func (px *Paxos) getInstance(seq int) (ret *PaxosInstance) {
    px.mu.Lock()
    defer px.mu.Unlock()
    index := seq - px.minStartSeq
    l := len(px.instances)
    switch {
    case index < 0:
        //
        fmt.Printf("Requesting an forgotten instance !!! %v %v\n", seq, px.minStartSeq)
        return nil
    case index < l:
        return px.instances[index]
    default:
        px.instances = append(px.instances, make([]*PaxosInstance, index + 1 - l)...)
        for i := l; i <= index; i++ {
            px.instances[i] = new(PaxosInstance)
            ins := px.instances[i]
            ins.maxPropose, ins.maxPrepare, ins.maxAccept, ins.decided, ins.value = px.me, -1, -1, false, 0
        }
        return px.instances[index]
    }
}

func (px *Paxos) Prepare(args PrepareArgs, reply *PaxosReply) error {
    instance := px.getInstance(args.Seq)
    if instance == nil {
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, false, -1, px.minStartSeq, 0
        return nil
    }
    instance.lock.Lock()
    go px.peerDone(args.Peer, args.StartSeq)
    switch {
    case instance.decided:
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, true, -1, px.startSeq[px.me], instance.value
    case instance.maxPrepare > args.Num:
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, false, instance.maxPrepare, px.startSeq[px.me], 0
    default:
        instance.maxPrepare = args.Num
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = true, false, instance.maxAccept, px.startSeq[px.me], instance.value
    }
    instance.lock.Unlock()
    return nil
}

func (px *Paxos) Accept(args AcceptArgs, reply *PaxosReply) error {
    instance := px.getInstance(args.Seq)
    if instance == nil {
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, false, -1, px.minStartSeq, 0
        return nil
    }
    instance.lock.Lock()
    switch {
    case instance.decided:
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, true, -1, 0, instance.value
    case instance.maxPrepare > args.Num:
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, false, instance.maxPrepare, 0, 0
    case instance.maxAccept > args.Num:
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = false, false, -1, 0, 0
    default:
        instance.maxAccept = args.Num
        instance.value = args.Value
        reply.Ok, reply.Decided, reply.MaxNum, reply.StartSeq, reply.Value = true, false, -1, 0, 0
    }
    instance.lock.Unlock()
    return nil
}

func (px *Paxos) Decide(args DecideArgs, dummy *interface{}) error {
    instance := px.getInstance(args.Seq)
    if instance == nil {
        return nil
    }
    instance.lock.Lock()
    instance.value = args.Value
    instance.decided = true
    instance.lock.Unlock()
    return nil
}

func (px *Paxos) sendPrepare(instance *PaxosInstance, seq int, newNum int) bool {
    var group sync.WaitGroup
    successCount := 0
    failCount := 0
    success := false
    fail := false
    args := PrepareArgs{seq, newNum, px.me, px.startSeq[px.me]}
    group.Add(px.totalPeers)
    for i := 0; i < px.totalPeers; i++ {
        go func(peer int) {
            defer group.Done()
            reply := new(PaxosReply)
            var ok bool
            if peer == px.me {
                ok = (px.Prepare(args, reply) == nil)
            } else {
                ok = call(px.peers[peer], "Paxos.Prepare", args, reply)
            }
            if ok {
                go px.peerDone(peer, reply.StartSeq)
            }
            instance.lock.Lock()
            switch {
            case fail || success:
                instance.lock.Unlock()
                return
            case reply.StartSeq > seq:
                success = true
            case instance.decided:
                fail = true
            case !ok:
                failCount++
                if failCount > px.totalPeers - px.quorum {
                    fail = true
                }
            case reply.Ok:
                successCount++
                if reply.MaxNum > instance.maxAccept {
                    instance.maxAccept = reply.MaxNum
                    instance.value = reply.Value
                }
                if successCount >= px.quorum {
                    success = true
                }
            case reply.Decided:
                instance.decided = true
                instance.value = reply.Value
                fail = true
            default:
                if reply.MaxNum > instance.maxPrepare {
                    instance.maxPrepare = reply.MaxNum
                }
                fail = true
            }
            instance.lock.Unlock()
        }(i)
    }
    group.Wait()
    return success
}

func (px *Paxos) sendAccept(instance *PaxosInstance, seq int, newNum int, v interface{}) (bool, interface{}) {
    var group sync.WaitGroup
    successCount := 0
    failCount := 0
    success := false
    fail := false
    group.Add(px.totalPeers)
    var pv interface{}
    if instance.maxAccept == -1 {
        pv = v
    } else {
        pv = instance.value
    }
    args := AcceptArgs{seq, newNum, pv}
    for i := 0; i < px.totalPeers; i++ {
        go func(peer int) {
            defer group.Done()
            reply := new(PaxosReply)
            var ok bool
            if peer == px.me {
                ok = (px.Accept(args, reply) == nil)
            } else {
                ok = call(px.peers[peer], "Paxos.Accept", args, reply)
            }
            instance.lock.Lock()
            switch {
            case fail || success:
                instance.lock.Unlock()
                return
            case reply.StartSeq > seq:
                success = true
            case instance.decided:
                fail = true
            case !ok:
                failCount++
                if failCount > px.totalPeers - px.quorum {
                    fail = true
                }
            case reply.Ok:
                successCount++
                if successCount >= px.quorum {
                    success = true
                }
            case reply.Decided:
                instance.decided = true
                instance.value = reply.Value
                fail = true
            default:
                if reply.MaxNum > instance.maxPrepare {
                    instance.maxPrepare = reply.MaxNum
                }
                fail = true
            }
            instance.lock.Unlock()
        }(i)
    }
    group.Wait()
    return success, pv
}

func (px *Paxos) sendDecide(instance *PaxosInstance, seq int, v interface{}) {
    var group sync.WaitGroup
    args := DecideArgs{seq, v}
    group.Add(px.totalPeers)
    for i := 0; i < px.totalPeers; i++ {
        go func(peer int) {
            var dummy *interface{}
            if peer == px.me {
                px.Decide(args, dummy)
            } else {
                call(px.peers[peer], "Paxos.Decide", args, dummy)
            }
            group.Done()
        }(i)
    }
    group.Wait()
}

func (px *Paxos) propose(instance *PaxosInstance, seq int, v interface{}) bool {
    //
    //fmt.Printf("*")
    instance.lock.Lock()
    if instance.decided {
        pv := instance.value
        instance.lock.Unlock()
        px.sendDecide(instance, seq, pv)
        return true
    }
    instance.maxPropose += px.totalPeers
    baseNum := instance.maxPropose
    if instance.maxPrepare > baseNum {
        baseNum = instance.maxPrepare
    }
    newNum := px.me + (baseNum / px.totalPeers + 1) * px.totalPeers
    instance.lock.Unlock()
    if px.dead {
        return false
    }
    
    //
    //fmt.Printf("> %v %v\n", seq, newNum)
    if !px.sendPrepare(instance, seq, newNum) {
        return false
    }
    if px.dead {
        return false
    }
    
    ok, pv := px.sendAccept(instance, seq, newNum, v)
    if !ok {
        return false
    }
    if px.dead {
        return false
    }
    
    px.sendDecide(instance, seq, pv)
    return true
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.totalPeers = len(peers)
  px.quorum = (px.totalPeers + 2) / 2
  px.startSeq = make([]int, px.totalPeers)
  for i := 0; i < px.totalPeers; i++ {
    px.startSeq[i] = 0
  }
  px.minStartSeq = 0
  

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    /*
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    */
    ///*
    _, port, _ := net.SplitHostPort(peers[me])
    l, e := net.Listen("tcp", ":" + port)
    //*/
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
