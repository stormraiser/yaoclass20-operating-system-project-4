package main

import (
    "../kvpaxos"
    "../kvutil"
    "os"
    "net"
    "net/http"
    "fmt"
)

var kv *kvpaxos.KVPaxos

func handleGet(writer http.ResponseWriter, request *http.Request) {
    kv.HandleReadWrite(kvpaxos.MethodGet, writer, request)
}

func handleInsert(writer http.ResponseWriter, request *http.Request) {
    kv.HandleReadWrite(kvpaxos.MethodInsert, writer, request)
}

func handleUpdate(writer http.ResponseWriter, request *http.Request) {
    kv.HandleReadWrite(kvpaxos.MethodUpdate, writer, request)
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
    kv.HandleReadWrite(kvpaxos.MethodDelete, writer, request)
}

func handleCountKey(writer http.ResponseWriter, request *http.Request) {
    kv.HandleMaintenance(kvpaxos.MethodCountKey, writer)
}

func handleDump(writer http.ResponseWriter, request *http.Request) {
    kv.HandleMaintenance(kvpaxos.MethodDump, writer)
}

func handleShutdown(writer http.ResponseWriter, request *http.Request) {
    os.Exit(0)
}

func main() {
    var me int
    fmt.Sscanf(os.Args[1], "n%d", &me)
    me--
    paxosPeers, httpPeers, ok := kvutil.LoadConfig("conf/settings.conf")
    if !ok {
        return
    }
    _, myPort, _ := net.SplitHostPort(httpPeers[me])
    kv = kvpaxos.Make(paxosPeers, 100, 10, 10, me)
    
    http.HandleFunc("/kv/insert", handleInsert)
    http.HandleFunc("/kv/delete", handleDelete)
    http.HandleFunc("/kv/update", handleUpdate)
    http.HandleFunc("/kv/get", handleGet)
    http.HandleFunc("/kvman/countkey", handleCountKey)
    http.HandleFunc("/kvman/dump", handleDump)
    http.HandleFunc("/kvman/shutdown", handleShutdown)
    
    http.ListenAndServe(":" + myPort, nil)
}
