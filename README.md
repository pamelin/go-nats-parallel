# go-nats-parallel

Utils library for [go-nats](https://github.com/nats-io/go-nats).

This library provides a bunch of helper subscribe functions which will invoke nats handlers in the separate goroutines.
  
Use this library if you require parallel processing of the incoming messages in the non-blocking fashion.

## Basic Usage

```go

ec, _ := natsll.NewEncodedJsonConn(nats.DefaultURL)
defer nc.Close()


natsll.SubscribeEc(ec, "foo", func(s string){
    fmt.Printf("Received a message: %s\n", s)
    time.Sleep(5 * time.Second) // let's dance
})

ec.Publish("foo", "Hello World")
ec.Publish("foo", "Hello World")
ec.Publish("foo", "Hello World")

```