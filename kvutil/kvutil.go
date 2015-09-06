package kvutil

import (
    "encoding/json"
    "net"
    "net/http"
    "net/url"
    "io"
    "io/ioutil"
    "time"
    "strconv"
)

type KeyJSON struct {
    Key string `json:"key"`
}

type KeyValueJSON struct {
    Key string `json:"key"`
    Value string `json:"value"`
}

type SuccessJSON struct {
    Success string `json:"success"`
    Message string `json:"message"`
}

type SuccessValueJSON struct {
    Success string `json:"success"`
    Value string `json:"value"`
    Message string `json:"error_message"`
}

type CountKeyJSON struct {
    Result string `json:"result"`
}

func DecodeJSONMessage(body io.ReadCloser, messageJSON interface {}) bool { 
    if body == nil {
        return false
    }
    defer body.Close()
    dec := json.NewDecoder(body)
    err := dec.Decode(&messageJSON)
    if err != io.EOF && err != nil {
        return false
    }
    return true
}

func LoadConfig(configFile string) (paxos []string, http []string, ok bool) {
    data, err := ioutil.ReadFile(configFile)
    if err != nil {
        ok = false
        return
    }
    config := make(map[string]string)
    err = json.Unmarshal(data, &config)
    if err != nil {
        ok = false
        return
    }
    var str string
    l := len(config) - 1
    paxos = make([]string, l)
    http = make([]string, l)
    port := config["port"]
    for i := 0; i < l; i++ {
        if i <= 8 {
            str = "n0" + strconv.Itoa(i + 1)
        } else {
            str = "n" + strconv.Itoa(i + 1)
        }
        paxos[i] = config[str]
        host, _, _ := net.SplitHostPort(paxos[i])
        http[i] = net.JoinHostPort(host, port)
    }
    ok = true
    return
}

func IssueWrite(addr, method, key, value string) (ret string, ok bool, latency time.Duration){
    u, _ := url.Parse("http://" + addr + "/kv/" + method)
    q := u.Query()
    q.Set("key", key)
    if method != "delete" {
        q.Set("value", value)
    }
    u.RawQuery = q.Encode()
    startTime := time.Now()
    response, err1 := http.Post(u.String(), "", nil)
    latency = time.Since(startTime)
    if err1 != nil {
        ok = false
        return
    }
    defer response.Body.Close()
    if method == "delete" {
        var message SuccessValueJSON
        if !DecodeJSONMessage(response.Body, &message) {
            ok = false
            return
        }
        if message.Success != "true" {
            ok = false
            return
        }
        ret = message.Value
        ok = true
        return
    } else {
        var message SuccessJSON
        if !DecodeJSONMessage(response.Body, &message) {
            ok = false
            return
        }
        if message.Success != "true" {
            ok = false
            return
        }
        ok = true
        return
    }
}

func IssueGet(addr, key string) (ret string, ok bool, latency time.Duration) {
    u, _ := url.Parse("http://" + addr + "/kv/get")
    q := u.Query()
    q.Set("key", key)
    u.RawQuery = q.Encode()
    startTime := time.Now()
    response, err1 := http.Get(u.String())
    latency = time.Since(startTime)
    if err1 != nil {
        ok = false
        return
    }
    defer response.Body.Close()
    var message SuccessValueJSON
    if !DecodeJSONMessage(response.Body, &message) {
        ok = false
        return
    }
    if message.Success != "true" {
        ok = false
        return
    }
    ret = message.Value
    ok = true
    return
}

func IssueDump(addr string) (ret []byte, ok bool) {
    u, _ := url.Parse("http://" + addr + "/kvman/dump")
    response, err1 := http.Get(u.String())
    if err1 != nil {
        ok = false
        return
    }
    defer response.Body.Close()
    ret, _ = ioutil.ReadAll(response.Body)
    ok = true
    return
}

func IssueCountKey(addr string) (ret int, ok bool) {
    u, _ := url.Parse("http://" + addr + "/kvman/countkey")
    response, err1 := http.Get(u.String())
    if err1 != nil {
        ok = false
        return
    }
    defer response.Body.Close()
    var message CountKeyJSON
    if !DecodeJSONMessage(response.Body, &message) {
        ok = false
        return
    }
    ret, err1 = strconv.Atoi(message.Result)
    if err1 != nil {
        ok = false
        return
    }
    ok = true
    return
}

func IssueShutdown(addr string) {
    u, _ := url.Parse("http://" + addr + "/kvman/shutdown")
    http.Get(u.String())
}
