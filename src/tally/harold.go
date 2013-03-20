package tally

import (
    "fmt"
    "log"
    "net/http"
    "net/url"
    "strings"
    "time"
)

// Harold is a monitoring service used by reddit. We post heartbeat messages to
// harold to let it know we're alive.
type Harold struct {
    host string
    port int
    secret string
}

func HaroldFromConfig(config Config) (harold *Harold, err error) {
    harold = &Harold{}
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

// Heartbeat sends a heartbeat message to harold, blocking until acknowledged.
func (harold *Harold) Heartbeat(tag string, interval time.Duration) (*http.Response, error) {
    log.Printf("HeartBeat")
    data := map[string] string{"tag": tag, "interval": fmt.Sprintf("%f", interval.Seconds())}
    log.Printf("posting")
    return harold.post([]string{"heartbeat"}, data)
}

// HeartMonitor returns a channel for the caller to send harold heartbeats to.
// It spins off a goroutine so the heartbeat channel never blocks, even if the
// harold service is not responding.
func (harold *Harold) HeartMonitor(tag string) (intervals chan time.Duration) {
    intervals = make(chan time.Duration)
    go func() {
        var alive *time.Duration // most recent interval pending to be sent
        waiting := false // whether we're waiting on a previous heartbeat send

        // channel for notifying end of asynchronous heartbeat RPC
        err := make(chan error)

        for {
            select {
            case interval := <-intervals:
                alive = &interval
            case e := <-err:
                if e != nil {
                    log.Printf("ERROR: harold heartbeat failed: %#v", e)
                }
                waiting = false
            }
            if alive != nil && !waiting {
                log.Printf("sending heartbeat to harold")
                go func(i time.Duration) {
                    _, x := harold.Heartbeat(tag, i)
                    err <- x
                }(*alive)
                waiting = true
                alive = nil
            }
        }
    }()
    return
}
