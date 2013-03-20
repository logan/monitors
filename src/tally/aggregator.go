package tally

import (
    "log"
    "net"
    "time"
)

// Spin off receivers and a goroutine to manage them. Returns a channel by
// which aggregated snapshots will be shared at the given interval.
func Aggregate(conn *net.UDPConn, numReceivers int, flushInterval time.Duration) (snapchan chan *Snapshot) {
    snapchan = make(chan *Snapshot)
    receivers := make([]*Receiver, numReceivers)
    for i := range(receivers) {
        receivers[i] = NewReceiver(string(i), conn)
        go receivers[i].Loop()
    }
    go func() {
        var numStats int64 = 0
        var snapshot *Snapshot
        for {
            if snapshot != nil { snapchan <- snapshot }
            snapshot = NewSnapshot()
            snapshot.start = time.Now()
            log.Printf("aggregator sleeping for %s", flushInterval)
            time.Sleep(flushInterval)
            log.Printf("aggregator sending flush command to receivers")
            for _, receiver := range(receivers) {
                receiver.flush <- nil
            }
            log.Printf("aggregator collecting flush responses")
            snapshot.duration = time.Now().Sub(snapshot.start)
            for _, receiver := range(receivers) {
                snapshot.Aggregate(<-receiver.flush)
            }
            numStats += int64(snapshot.NumStats())
            snapshot.totalStats = numStats
            log.Printf("aggregator returning snapshot")
        }
    }()
    return
}
