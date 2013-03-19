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
    receivers []*Receiver
    aggregator *Aggregator
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
    if err != nil { return err }
    server.receivers = make([]*Receiver, server.numWorkers)
    for i := range(server.receivers) {
        server.receivers[i] = NewReceiver(fmt.Sprintf("%d", i), server.conn)
    }
    server.aggregator = NewAggregator(server.receivers, server.flushInterval)
    return err
}

func (server *Server) Loop() {
    intervals := make(chan time.Duration)
    log.Printf("setting up server")
    server.Setup()
    for _, receiver := range(server.receivers) {
        go receiver.Loop()
    }
    go server.aggregator.Loop()
    if server.harold != nil {
        go server.harold.HeartMonitor("tallier", intervals)
    }
    log.Printf("running")
    for {
        log.Printf("waiting for snapshot")
        snapshot := <-server.aggregator.flush
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
