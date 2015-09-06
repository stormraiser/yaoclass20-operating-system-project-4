package kvpaxos

import (
    "../paxos"
    "../kvutil"
    "net/http"
    "sync"
    "time"
    "encoding/json"
    "strconv"
    "encoding/gob"
)

const (
    MethodNop = iota
    MethodGet = iota
    MethodInsert = iota
    MethodUpdate = iota
    MethodDelete = iota
    MethodCountKey = iota
    MethodDump = iota
)

type Request struct {
    method int
    key string
    value string
    responseChannel chan []byte
}

type LogEntry struct {
    Method int
    Key string
    Value string
    Peer int
    Worker int
}

type KVPaxos struct {
    paxosLock sync.Mutex
    commitLock sync.Mutex
    px *paxos.Paxos
    peers []string
    kvMap map[string]string
    me int
    catchupSec int
    nextCommit int
    requestQueue chan Request
    responseChannels []chan []byte
    commitSignal chan int
}

func (kv *KVPaxos) propose(seq int, entry LogEntry) bool {
    timeOut := 10 * time.Millisecond
    kv.px.Start(seq, entry)
    for {
        decided, value := kv.px.Status(seq)
        if decided {
            return (value == entry)
        }
        time.Sleep(timeOut)
        if timeOut < time.Second {
            timeOut = timeOut * 3 / 2
        }
    }
}

func (kv *KVPaxos) worker(id int) {
    for {
        request := <- kv.requestQueue
        for !kv.propose(kv.px.Max() + 1, LogEntry{request.method, request.key, request.value, kv.me, id}) {}
        select {
        case kv.commitSignal <- 0:
            break
        default:
            break
        }
        data := <- kv.responseChannels[id]
        request.responseChannel <- data
    }
}

func (kv *KVPaxos) autoCatchup() {
    for {
        for !kv.propose(kv.px.Max() + 1, LogEntry{MethodNop, "", "", -1, -1}) {}
        select {
        case kv.commitSignal <- 0:
            break
        default:
            break
        }
        time.Sleep(time.Duration(kv.catchupSec) * time.Second)
    }
}

func (kv *KVPaxos) autoCommit() {
    for {
        <- kv.commitSignal
        for kv.nextCommit <= kv.px.Max() {
            kv.propose(kv.nextCommit, LogEntry{MethodNop, "", "", -1, -1})
            _, value := kv.px.Status(kv.nextCommit)
            kv.commit(value.(LogEntry))
            kv.nextCommit++
        }
        kv.px.Done(kv.nextCommit - 1)
    }
}

func (kv *KVPaxos) commit(entry LogEntry) {
    var data []byte
    switch entry.Method {
    case MethodNop:
        break
    case MethodGet:
        if entry.Peer == kv.me {
            value, ok := kv.kvMap[entry.Key]
            if ok {
                data, _ = json.Marshal(kvutil.SuccessValueJSON{"true", value, ""})
            } else {
                data, _ = json.Marshal(kvutil.SuccessValueJSON{"false", "", "Key does not exist"})
            }
        }
    case MethodInsert:
        _, ok := kv.kvMap[entry.Key]
        if ok {
            data, _ = json.Marshal(kvutil.SuccessJSON{"false", "Key already existed"})
        } else {
            kv.kvMap[entry.Key] = entry.Value
            data, _ = json.Marshal(kvutil.SuccessJSON{"true", ""})
        }
    case MethodUpdate:
        _, ok := kv.kvMap[entry.Key]
        if ok {
            kv.kvMap[entry.Key] = entry.Value
            data, _ = json.Marshal(kvutil.SuccessJSON{"true", ""})
        } else {
            data, _ = json.Marshal(kvutil.SuccessJSON{"false", "Key does not exist"})
        }
    case MethodDelete:
        value, ok := kv.kvMap[entry.Key]
        if ok {
            delete(kv.kvMap, entry.Key)
            data, _ = json.Marshal(kvutil.SuccessValueJSON{"true", value, ""})
        } else {
            data, _ = json.Marshal(kvutil.SuccessValueJSON{"false", "", "Key does not exist"})
        }
    case MethodCountKey:
        data, _ = json.Marshal(kvutil.CountKeyJSON{strconv.Itoa(len(kv.kvMap))})
    case MethodDump:
        var kvArray [][]string
        for key, value := range kv.kvMap {
            kvArray = append(kvArray, []string{key, value})
        }
        data, _ = json.Marshal(kvArray)
    }
    if (entry.Peer == kv.me) && (entry.Worker != -1) {
        kv.responseChannels[entry.Worker] <- data
    }
}

func (kv *KVPaxos) HandleReadWrite(method int, writer http.ResponseWriter, request *http.Request) {
    q := request.URL.Query()
    key := q.Get("key")
    value := ""
    isRead := (method == MethodGet) || (method == MethodDelete)
    if !isRead {
        value = q.Get("value")
    }
    c := make(chan []byte)
    select {
    case kv.requestQueue <- Request{method, key, value, c}:
        writer.Write(<- c)
    default:
        var data []byte
        if isRead {
            data, _ = json.Marshal(kvutil.SuccessValueJSON{"false", "", "Server overload"})
        } else {
            data, _ = json.Marshal(kvutil.SuccessJSON{"false", "Server overload"})
        }
        writer.Write(data)
    }
}

func (kv *KVPaxos) HandleMaintenance(method int, writer http.ResponseWriter) {
    c := make(chan []byte)
    kv.requestQueue <- Request{method, "", "", c}
    writer.Write(<- c)
}

func Make(peers []string, queueLength, poolSize, catchupSec, me int) *KVPaxos {
    kv := new(KVPaxos)
    kv.peers = peers
    kv.me = me
    kv.px = paxos.Make(peers, me, nil)
    kv.catchupSec = catchupSec
    kv.kvMap = make(map[string]string)
    kv.requestQueue = make(chan Request, queueLength)
    kv.nextCommit = 0
    kv.responseChannels = make([]chan []byte, poolSize)
    kv.commitSignal = make(chan int, 1)
    go kv.autoCommit()
    go kv.autoCatchup()
    for i := 0; i < poolSize; i++ {
        kv.responseChannels[i] = make(chan []byte)
        go kv.worker(i)
    }
    return kv
}

func init() {
    gob.RegisterName("LogEntry", LogEntry{})
}
