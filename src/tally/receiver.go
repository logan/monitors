package tally

import (
    "log"
    "net"
    "time"
)

const STATGRAM_CHANNEL_BUFSIZE = 1024

type Receiver struct {
    id string
    conn *net.UDPConn
    snapshot *Snapshot
    statgrams chan Statgram
    flush chan *Snapshot
    quit chan int
    lastMessageCount int64
    messageCount int64
    lastByteCount int64
    byteCount int64
}

func NewReceiver(id string, conn *net.UDPConn) *Receiver {
    return &Receiver{
        id: id,
        conn: conn,
        snapshot: NewSnapshot(),
        statgrams: make(chan Statgram, STATGRAM_CHANNEL_BUFSIZE),
        flush: make(chan *Snapshot),
        quit: make(chan int),
    }
}

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

func (receiver *Receiver) ReceiveStatgrams() {
    for {
        statgram, err := receiver.ReadOnce()
        if err != nil { break }
        receiver.statgrams <- statgram
    }
}

func (receiver *Receiver) Process(statgram Statgram) {
    for _, sample := range(statgram) {
        switch (sample.valueType) {
            case COUNTER:
                receiver.snapshot.Count(sample.key, sample.value / sample.sampleRate)
            case TIMER:
                receiver.snapshot.Time(sample.key, sample.value)
        }
    }
}

func (receiver *Receiver) Flush() (snapshot *Snapshot) {
    snapshot = receiver.snapshot
    snapshot.Count("tallier.messages.child_" + receiver.id,
            float64(receiver.messageCount - receiver.lastMessageCount))
    snapshot.Count("tallier.bytes.child_" + receiver.id,
            float64(receiver.byteCount - receiver.lastByteCount))
    receiver.lastMessageCount = receiver.messageCount
    receiver.lastByteCount = receiver.byteCount
    receiver.snapshot = NewSnapshot()
    log.Printf("receiver returning snapshot")
    return
}

func (receiver *Receiver) Loop() {
    go receiver.ReceiveStatgrams()
    for {
        select {
            case statgram := <-receiver.statgrams:
                receiver.Process(statgram)
            case _ = <-receiver.quit:
                break
            case _ = <-receiver.flush:
                receiver.flush <- receiver.Flush()
        }
    }
}

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
