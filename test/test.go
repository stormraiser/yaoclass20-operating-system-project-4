package main

import (
    "../kvutil"
    "sort"
    "time"
    "strconv"
    "fmt"
    "sync"
    "math/rand"
)

var totalInsert, successInsert int
var totalGet, successGet int
var insertTime, getTime int64
var insertTimeRec, getTimeRec []int

var insertLock, getLock sync.Mutex
var testGroup sync.WaitGroup

var success bool

var peers []string

func logInsert(ok bool, latency time.Duration) {
    insertLock.Lock()
    totalInsert++
    if ok {
        successInsert++
    }
    var musec int
    musec = int((latency.Nanoseconds() + 500) / 1000)
    insertTime += latency.Nanoseconds()
    insertTimeRec = append(insertTimeRec, musec)
    insertLock.Unlock()
}

func logGet(ok bool, latency time.Duration) {
    getLock.Lock()
    totalGet++
    if ok {
        successGet++
    }
    var musec int
    musec = int((latency.Nanoseconds() + 500) / 1000)
    getTime += latency.Nanoseconds()
    getTimeRec = append(getTimeRec, musec)
    getLock.Unlock()
}

func test1(round, num, offset int) {
    defer testGroup.Done()
    var ret string
    var ok bool
    var latency time.Duration
    for i := 0; i < round; i++ {
        for j := 0; j < num; j++ {
            k := offset + i * num + j
            peer := rand.Int() % len(peers)
            _, ok, latency = kvutil.IssueWrite(peers[peer], "insert", strconv.Itoa(k), strconv.Itoa(k))
            logInsert(ok, latency)
        }
        for j := 0; j < num; j++ {
            k := offset + i * num + j
            peer := rand.Int() % len(peers)
            ret, ok, latency = kvutil.IssueGet(peers[peer], strconv.Itoa(k))
            logGet(ok, latency)
            if ok && (ret != strconv.Itoa(k)) {
                success = false
            }
        }
        for j := 0; j < num; j++ {
            k := offset + i * num + j
            peer := rand.Int() % len(peers)
            kvutil.IssueWrite(peers[peer], "update", strconv.Itoa(k), strconv.Itoa(k + 1))
        }
        for j := 0; j < num; j++ {
            k := offset + i * num + j
            peer := rand.Int() % len(peers)
            ret, ok, _ = kvutil.IssueWrite(peers[peer], "delete", strconv.Itoa(k), "")
            if ok && (ret != strconv.Itoa(k + 1)) {
                success = false
            }
        }
    }
}

func report(ok bool) {
    sort.Ints(insertTimeRec)
    sort.Ints(getTimeRec)
    fmt.Print("Result: ")
    if ok {
        fmt.Println("success")
    } else {
        fmt.Println("fail")
    }
    fmt.Printf("Insertion: %v / %v\n", successInsert, totalInsert)
    fmt.Printf("Average latency: %vus / %vus\n", (insertTime + int64(totalInsert) / 2) / int64(totalInsert) / 1000, (getTime + int64(totalGet) / 2) / int64(totalGet) / 1000)
    fmt.Printf("Percentile latency: %vus / %vus, %vus / %vus, %vus / %vus, %vus / %vus\n",
                insertTimeRec[int(float32(totalInsert) * 0.2)], getTimeRec[int(float32(totalGet) * 0.2)],
                insertTimeRec[int(float32(totalInsert) * 0.5)], getTimeRec[int(float32(totalGet) * 0.5)],
                insertTimeRec[int(float32(totalInsert) * 0.7)], getTimeRec[int(float32(totalGet) * 0.7)],
                insertTimeRec[int(float32(totalInsert) * 0.9)], getTimeRec[int(float32(totalGet) * 0.9)])
}

func main() {
    var ok bool
    _, peers, ok = kvutil.LoadConfig("conf/settings.conf")
    if !ok {
        return
    }
    
    totalInsert = 0
    successInsert = 0
    totalGet = 0
    successGet = 0
    insertTime = 0
    getTime = 0
    success = true
    
    /*
    testGroup.Add(10)
    go test1(1000, 1, 0)
    go test1(500, 2, 1000)
    go test1(200, 5, 2000)
    go test1(100, 10, 3000)
    go test1(50, 20, 4000)
    go test1(20, 50, 5000)
    go test1(10, 100, 6000)
    go test1(5, 200, 7000)
    go test1(2, 500, 8000)
    go test1(1, 1000, 9000)
    testGroup.Wait()
    */
    
    testGroup.Add(7)
    go test1(100, 1, 0)
    go test1(50, 2, 100)
    go test1(20, 5, 200)
    go test1(10, 10, 300)
    go test1(5, 20, 400)
    go test1(2, 50, 500)
    go test1(1, 100, 600)
    testGroup.Wait()
    
    report(success)
}
