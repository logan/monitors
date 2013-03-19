package tally

import (
    "fmt"
    "log"
    "net/http"
    "net/url"
    "strings"
    "time"
)

type Harold struct {
    host string
    port int
    secret string
    timeout time.Duration
}

func HaroldFromConfig(config Config) (harold *Harold, err error) {
    harold = &Harold{timeout: time.Duration(3) * time.Second}
    if harold.host, err = config.GetValue("harold", "host"); err != nil { return }
    if harold.secret, err = config.GetValue("harold", "secret"); err != nil { return }
    harold.port, err = config.GetInt("harold", "port")
    return
}

func (harold *Harold) post(path []string, data map[string] string) (*http.Response, error) {
    uri := fmt.Sprintf("http://%s:%d/harold/%s/%s", harold.host, harold.port,
                 strings.Join(path, "/"), harold.secret)
    post_data := make(map[string] []string)
    for name, value := range(data) {
        post_data[name] = []string{value}
    }
    return http.PostForm(uri, url.Values(post_data))
}

func (harold *Harold) Heartbeat(tag string, interval time.Duration) (*http.Response, error) {
    data := map[string] string{"tag": tag, "interval": fmt.Sprintf("%f", interval.Seconds())}
    return harold.post([]string{"heartbeat"}, data)
}

func (harold *Harold) AsyncHeartbeat(tag string, interval time.Duration,
                                     response chan *http.Response, err chan error) {
    r, e := harold.Heartbeat(tag, interval)
    if e == nil {
        response <- r
    } else {
        err <- e
    }
}

func (harold *Harold) HeartMonitor(tag string, intervals chan time.Duration) {
    var alive *time.Duration
    ready := true
    response := make(chan *http.Response)
    err := make(chan error)
    for {
        select {
            case interval := <-intervals:
                alive = &interval
            case _ = <-response:
                ready = true
            case e := <-err:
                log.Printf("ERROR: harold heartbeat failed: %#v", e)
                ready = true
        }
        if alive != nil && ready {
            log.Printf("sending heartbeat to harold")
            go harold.AsyncHeartbeat(tag, *alive, response, err)
            ready = false
            alive = nil
        }
    }
}
