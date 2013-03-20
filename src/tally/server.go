package tally

import (
    "fmt"
    "log"
    "net"
    "runtime"
    "time"
)

type Server struct {
    receiverPort int
    numWorkers int
    flushInterval time.Duration

    conn *net.UDPConn
    harold *Harold
    graphite *Graphite
}

func ServerFromConfig(config Config) (server *Server, err error) {
    server = &Server{}
    server.receiverPort, err = config.GetInt("tallier", "port")
    if err != nil { return }
    server.numWorkers, err = config.GetInt("tallier", "num_workers")
    if err != nil { return }
    server.flushInterval, err = config.GetSeconds("tallier", "flush_interval")
    if err != nil { return }
    server.graphite, err = GraphiteFromConfig(config)
    if err != nil { return }
    var enableHeartbeat bool
    enableHeartbeat, err = config.GetBoolean("tallier", "enable_heartbeat")
    if err == nil && enableHeartbeat {
        server.harold, err = HaroldFromConfig(config)
    }
    return
}

func (server *Server) Setup() error {
    runtime.GOMAXPROCS(server.numWorkers + 1)
    receiver_addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d",
                               server.receiverPort))
    if err != nil { return err }
    server.conn, err = net.ListenUDP("udp", receiver_addr)
    return err
}

func (server *Server) Loop() {
    intervals := make(chan time.Duration)
    log.Printf("setting up server")
    server.Setup()
    if server.harold != nil {
        go server.harold.HeartMonitor("tallier", intervals)
    }
    snapchan := Aggregate(server.conn, server.numWorkers, server.flushInterval)
    log.Printf("running")
    for {
        log.Printf("waiting for snapshot")
        snapshot := <-snapchan
        for {
            log.Printf("sending snapshot with %d stats to graphite",
                    snapshot.NumStats())
            var err error
            if err = server.graphite.SendReport(snapshot); err == nil { break }
            log.Printf("ERROR: failed to send graphite report: %s", err)
            time.Sleep(time.Second)
        }
        if server.harold != nil {
            log.Printf("sending interval to heart monitor")
            intervals <- 3 * server.flushInterval
        }
    }
}
