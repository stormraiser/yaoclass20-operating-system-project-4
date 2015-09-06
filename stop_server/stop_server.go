package main

import (
    "os"
    "../kvutil"
    "fmt"
)

func main() {
    _, peers, ok := kvutil.LoadConfig("conf/settings.conf")
    if !ok {
        return
    }
    
    var peer int
    fmt.Sscanf(os.Args[1], "n%d", &peer)
    kvutil.IssueShutdown(peers[peer - 1])
}
