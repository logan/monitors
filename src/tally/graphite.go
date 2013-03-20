package tally

import (
    "net"
    "strings"
)

// Graphite is a client for sending stat reports to a graphite (carbon) server.
type Graphite struct {
    addr *net.TCPAddr
}

func GraphiteFromConfig(config Config) (client *Graphite, err error) {
    var addr string
    addr, err = config.GetValue("graphite", "graphite_addr")
    if err != nil { return }
    client = &Graphite{}
    client.addr, err = net.ResolveTCPAddr("tcp", addr)
    return
}

// SendReport takes a snapshot and submits all its stats to graphite.
func (client *Graphite) SendReport(snapshot *Snapshot) (err error) {
    var conn *net.TCPConn
    conn, err = net.DialTCP("tcp", nil, client.addr)
    if err != nil { return }
    defer conn.Close()
    msg := strings.Join(snapshot.GraphiteReport(), "")
    _, err = conn.Write([]byte(msg))
    return
}
