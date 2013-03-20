package tally

import (
    "log"
    "net"
    "time"
)

const STATGRAM_CHANNEL_BUFSIZE = 1024

// Receivers share the work of listening on a UDP port and accumulating stats.
type Receiver struct {
    id string // child identifier for collecting internal stats
    conn *net.UDPConn
    snapshot *Snapshot // current snapshot we're collecting into
    lastMessageCount int64
    messageCount int64
    lastByteCount int64
    byteCount int64
}

// ReadOnce blocks on the listening connection until a statgram arrives. It
// takes care of parsing it and returns it. Any parse errors are ignored, so
// it's possible an empty statgram will be returned.
func (receiver *Receiver) ReadOnce() (statgram Statgram, err error) {
    buf := make([]byte, 1024)
    var size int
    size, err = receiver.conn.Read(buf)
    if err == nil {
        receiver.messageCount += 1
        receiver.byteCount += int64(size)
        statgram = Parse(string(buf[:size]))
    }
    return
}

// ReceiveStatgrams spins off a goroutine to read statgrams off the UDP port.
// Returns a buffered channel that will receive statgrams as they arrive.
func (receiver *Receiver) ReceiveStatgrams() (statgrams chan Statgram) {
    statgrams = make(chan Statgram, STATGRAM_CHANNEL_BUFSIZE)
    go func() {
        for {
            statgram, err := receiver.ReadOnce()
            if err != nil { break }
            statgrams <- statgram
        }
    }()
    return
}

// RunReceiver spins off a goroutine to receive and process statgrams. Returns a
// bidirectional control channel, which provides a snapshot each time it's given
// a nil value.
func RunReceiver(id string, conn *net.UDPConn) (controlChannel chan *Snapshot) {
    receiver := &Receiver{
        id: id,
        conn: conn,
        snapshot: NewSnapshot(),
    }
    controlChannel = make(chan *Snapshot)
    statgrams := receiver.ReceiveStatgrams()
    go func() {
        for {
            select {
            case statgram := <-statgrams:
                receiver.snapshot.ProcessStatgram(statgram)
            case _ = <-controlChannel:
                snapshot := receiver.snapshot
                snapshot.Count("tallier.messages.child_" + receiver.id,
                        float64(receiver.messageCount - receiver.lastMessageCount))
                snapshot.Count("tallier.bytes.child_" + receiver.id,
                        float64(receiver.byteCount - receiver.lastByteCount))
                receiver.lastMessageCount = receiver.messageCount
                receiver.lastByteCount = receiver.byteCount
                receiver.snapshot = NewSnapshot()
                controlChannel <- snapshot
            }
        }
    }()
    return
}

// Aggregate spins off receivers and a goroutine to manage them. Returns a
// channel by which aggregated snapshots will be shared at the given interval.
func Aggregate(conn *net.UDPConn, numReceivers int, flushInterval time.Duration) (snapchan chan *Snapshot) {
    snapchan = make(chan *Snapshot)
    var controlChannels []chan *Snapshot
    for i := 0; i < numReceivers; i++ {
        controlChannels = append(controlChannels, RunReceiver(string(i), conn))
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
            for _, controlChannel := range(controlChannels) {
                controlChannel <- nil
            }
            log.Printf("aggregator collecting flush responses")
            snapshot.duration = time.Now().Sub(snapshot.start)
            for _, controlChannel := range(controlChannels) {
                snapshot.Aggregate(<-controlChannel)
            }
            numStats += int64(snapshot.NumStats())
            snapshot.totalStats = numStats
            log.Printf("aggregator returning snapshot")
        }
    }()
    return
}
