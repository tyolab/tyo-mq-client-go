# tyo-mq-client-go

A Go client for **[tyo-mq](https://github.com/tyolab/tyo-mq)** — the
distributed pub/sub messaging service with durable delivery (ACK / retry /
dead-letter queue), MQTT-style topic wildcards, consumer groups, and
multi-tenant auth realms.

The client is a dependency-light Socket.IO v4 implementation (only
`gorilla/websocket`) with typed protocol structs and a small convenience layer.

## Install

```bash
go get github.com/tyolab/tyo-mq-client-go
```

Requires Go 1.22+ and a running tyo-mq server
(`npm install tyo-mq && npx tyo-mq-server`, or Docker — see the
[server repo](https://github.com/tyolab/tyo-mq)).

## Quick start

```go
import tyomq "github.com/tyolab/tyo-mq-client-go"

c := tyomq.NewClient("http://localhost:17352", nil)
ready := make(chan struct{})
go c.Connect(ctx, ready)
<-ready

// with auth enabled on the server:
// err := c.Authenticate(ctx, "my-token")

// produce
c.RegisterProducer("order-service")
c.Produce("order-service", "order-placed", map[string]any{"orderId": 1001})

// subscribe (durable + auto-ACK)
c2 := tyomq.NewClient("http://localhost:17352", nil)
// ... Connect as above ...
c2.RegisterConsumer("email-service")
c2.Subscribe(tyomq.SubscribeReq{
    Producer: "order-service",
    Event:    "order-placed",
    Consumer: "email-service",
    Durable:  true,
    Ack:      true,
    Retry:    &tyomq.RetryPolicy{MaxAttempts: 3, Delay: "5s", Backoff: "exponential"},
}, func(msg tyomq.ConsumedMessage) {
    fmt.Printf("order event: %s\n", msg.Message)
})
```

Run the complete example against a local server:

```bash
go run ./examples/pubsub -server http://localhost:17352
```

## Feature coverage

| Feature | How |
|---|---|
| Fire-and-forget pub/sub | `Produce` / `Subscribe` |
| Durable delivery + ACK/retry | `SubscribeReq{Durable, Ack, ManualAck, AckTimeout, Retry}`; auto-ACK or `c.Ack(msgID)` |
| Topic wildcards (`+`, `#`) | `SubscribeReq{Mode: "topic", Event: "orders/#"}` |
| Consumer groups | `SubscribeReq{Group: "workers"}` |
| Broadcast / TTL / guaranteed | emit a full `ProduceReq` |
| Authentication (tokens) | `Authenticate(ctx, token)` |
| Authorization request flow | `AuthorizationRequest` + signed `NewAuthNextReq` / `NewAuthDecideReq` |
| Signed manager commands | `CreateAdminProof` (HMAC-SHA256, matches the server exactly) |
| Custom namespaces (e.g. `/remote`) | `NewClientNS(url, "/remote", log)` |

Anything not covered by a helper is one `c.Emit(event, payload)` +
`c.On(event, handler)` away — the full wire protocol is documented in the
[server repo](https://github.com/tyolab/tyo-mq).

## Reconnection

`Connect` runs one connection and returns when it drops; reconnection policy
belongs to the caller (a simple retry loop) so that services control their own
backoff. Durable subscriptions survive: reconnect with the same consumer name,
re-`Subscribe`, and queued messages replay.

## Other clients

Node.js (and browsers) ships with the [server package](https://github.com/tyolab/tyo-mq);
see also [Python](https://github.com/tyolab/tyo-mq-client-python),
[Rust](https://github.com/tyolab/tyo-mq-client-rust),
[C/C++](https://github.com/tyolab/tyo-mq-client-cpp),
[Java](https://github.com/tyolab/tyo-mq-client-java), and
[C#](https://github.com/tyolab/tyo-mq-client-csharp).

## License

Apache-2.0. Built by [TYO Lab](https://tyo.com.au).
