package main

import (
    "fmt"
    "strconv"
    "os"
    "bufio"
    "strings"
    "../kvutil"
)

func main() {
    _, peers, ok := kvutil.LoadConfig("conf/settings.conf")
    if !ok {
        return
    }
    
    reader := bufio.NewReader(os.Stdin)
    for {
        str, _ := reader.ReadString('\n')
        fields := strings.Fields(str)
        if fields[0] == "exit" {
            os.Exit(0)
        }
        peer, _ := strconv.Atoi(fields[0])
        peer -= 1
        switch fields[1] {
        case "insert":
            _, ok, _ := kvutil.IssueWrite(peers[peer], "insert", fields[2], fields[3])
            if ok {
                fmt.Printf("Success\n")
            } else {
                fmt.Printf("Fail\n")
            }
        case "delete":
            ret, ok, _ := kvutil.IssueWrite(peers[peer], "delete", fields[2], "")
            if ok {
                fmt.Printf("Success: %v\n", ret)
            } else {
                fmt.Printf("Fail\n")
            }
        case "update":
            _, ok, _ := kvutil.IssueWrite(peers[peer], "update", fields[2], fields[3])
            if ok {
                fmt.Printf("Success\n")
            } else {
                fmt.Printf("Fail\n")
            }
        case "get":
            ret, ok, _ := kvutil.IssueGet(peers[peer], fields[2])
            if ok {
                fmt.Printf("Success: %v\n", ret)
            } else {
                fmt.Printf("Fail\n")
            }
        case "dump":
            ret, ok := kvutil.IssueDump(peers[peer])
            if ok {
                fmt.Printf("Success: ")
                os.Stdout.Write(ret)
                fmt.Printf("\n")
            } else {
                fmt.Printf("Fail\n")
            }
        case "countkey":
            ret, ok := kvutil.IssueCountKey(peers[peer])
            if ok {
                fmt.Printf("Success: %v\n", ret)
            } else {
                fmt.Printf("Fail\n")
            }
        case "shutdown":
            kvutil.IssueShutdown(peers[peer])
        default:
            fmt.Println("unkown command")
        }
    }
}
