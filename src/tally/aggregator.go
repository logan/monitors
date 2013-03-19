package tally

import (
    "log"
    "time"
)

type Aggregator struct {
    receivers []*Receiver
    flushDelay time.Duration
    snapshot *Snapshot
    flush chan *Snapshot
}

func NewAggregator(receivers []*Receiver, flushDelay time.Duration) *Aggregator {
    return &Aggregator{
        receivers: receivers,
        flushDelay: flushDelay,
        flush: make(chan *Snapshot),
    }
}

func (aggregator *Aggregator) Loop() {
    var numStats int64
    for {
        aggregator.snapshot = NewSnapshot()
        aggregator.snapshot.start = time.Now()
        log.Printf("aggregator sleeping for %s", aggregator.flushDelay)
        time.Sleep(aggregator.flushDelay)
        log.Printf("aggregator sending flush command to receivers")
        for _, receiver := range(aggregator.receivers) {
            receiver.flush <- nil
        }
        log.Printf("aggregator collecting flush responses")
        aggregator.snapshot.duration = time.Now().Sub(aggregator.snapshot.start)
        for _, receiver := range(aggregator.receivers) {
            aggregator.snapshot.Aggregate(<-receiver.flush)
        }
        numStats += int64(aggregator.snapshot.NumStats())
        aggregator.snapshot.totalStats = numStats
        log.Printf("aggregator returning snapshot")
        aggregator.flush <- aggregator.snapshot
    }
}
