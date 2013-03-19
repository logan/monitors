package main

import (
    "../tally"
)

func main() {
    cfg := tally.NewConfig()
    cfg.Parse("production.ini")
    server, err := tally.ServerFromConfig(*cfg)
    if err != nil {
        panic(err)
    }
    server.Loop()
}
