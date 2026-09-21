# Quash

⚠️ **Work in progress** — Quash is an experimental gRPC-based service written in Go that combines an in-memory key-value store with a topic-based publish/subscribe system, plus a live telemetry dashboard over WebSocket.

## Features

- **Key-value store** — set, get, and delete keys with a per-key TTL
- **Publish/Subscribe** — create named topics, add and remove subscribers, and publish values that queue up per subscriber until they're consumed
- **Live telemetry dashboard** — an admin web page that streams KV and topic/subscriber metrics in real time over WebSocket
- **gRPC API** — language-agnostic surface defined in Protocol Buffers, including bidirectional streaming for `Publish` and `Consume`
- **Hot reload for development** — configured via [Air](https://github.com/air-verse/air) (`.air.toml`)

> **Note:** This is an early-stage, single-node, in-memory project meant for learning and experimentation — not production use. See [Known limitations](#known-limitations).

## Architecture

```
                        ┌─────────────────────┐
                        │        main.go       │
                        │  (signal handling,    │
                        │   graceful shutdown)  │
                        └──────────┬────────────┘
                                   │
                          go server.Serve()
                                   │
              ┌────────────────────┴────────────────────┐
              │                                          │
   gRPC server  :6300                        go admin.AdminServer()
   (server/quashserver)                                  │
   - SetKV / GetKV / DeleteKV                   HTTP + WebSocket  :6301
   - CreateTopic / RemoveTopic                  (admin package)
   - AddSubscriber / RemoveSubscriber           - "/"        → dashboard HTML
   - Publish (bidi stream)                      - "/ws/admin"→ live KV + topic metrics feed
   - Consume (bidi stream)                          │
              │                          quashserver.Snapshot() (in-process,
    server/queue (linked-list queue,      lock-protected point-in-time copy)
    one per subscriber — each
    subscriber's own mailbox)
```

Each topic holds a map of `subscriber ID → *queue.Queue`. `Publish` pushes a value onto **every** subscriber's queue for that topic (broadcast). `Consume` is a pull: a client sends its topic + subscriber ID once, and the server drains and streams back whatever is currently queued for that subscriber, then closes — it does not block waiting for future messages, so a consumer calls it again (e.g. on a timer) to keep polling. The admin dashboard doesn't touch server state directly — it calls `quashserver.Snapshot()` in-process (which takes the same locks the RPC handlers use) and pushes the result over WebSocket to the browser.

## Project structure

```
.
├── main.go                       # Entry point, signal handling, graceful shutdown
├── utils/
│   ├── utils.go                    # Listen addresses (QuashDb :6300, QuashTelemetry :6301), SubscriptionID()
│   └── utils_test.go
├── structs/
│   └── structs.go                  # Shared types: KVValue, Topic, SubscriptionID, SrvData (admin snapshot)
├── admin/
│   └── admin.go                    # Admin dashboard: HTTP + WebSocket telemetry bridge
├── client/
│   ├── client.go                   # Go client wrapper around the gRPC service
│   └── client_test.go
├── proto/
│   ├── quash_proto.proto           # Service and message definitions
│   ├── quash_proto.pb.go           # Generated message code
│   └── quash_proto_grpc.pb.go      # Generated gRPC code
├── server/
│   ├── server.go                   # gRPC server bootstrap, starts admin server too
│   ├── quashserver/
│   │   └── quashserver.go            # Core RPC handlers, KV store, topic broker, TTL garbage collection
│   └── queue/
│       └── queue.go                   # Singly linked-list queue (one per subscriber)
├── .air.toml                       # Hot-reload config for local development
└── go.mod
```

## Installation

### Prerequisites

- Go 1.25 or later

### Setup

```bash
git clone https://github.com/mddfaisal/quash.git
cd quash
go mod download
```

### Build & run

```bash
go build -o quash
./quash
```

This starts:
- the gRPC API on `0.0.0.0:6300`
- the admin dashboard on `:6301` — open `http://localhost:6301` in a browser to see live metrics

### Development with hot reload

```bash
go install github.com/air-verse/air@latest
air
```

Air rebuilds and restarts the binary whenever a `.go`, `.html`, `.tpl`, or `.tmpl` file changes.

## Running tests

```bash
go test ./...
```

## API reference

Full definitions live in `proto/quash_proto.proto`.

| RPC | Type | Description |
|---|---|---|
| `SetKV` | unary | Store a key/value with a TTL (`time_out_duration`, in seconds) |
| `GetKV` | unary | Retrieve a value by key; errors if missing or expired |
| `DeleteKV` | unary | Delete a key |
| `CreateTopic` | unary | Create a named topic |
| `RemoveTopic` | unary | Delete a topic |
| `AddSubscriber` | unary | Register a new subscriber on an existing topic; returns a `subscriber_id` |
| `RemoveSubscriber` | unary | Unregister a subscriber from a topic |
| `Publish` | bidi-streaming | Send values to a topic; every current subscriber's queue gets a copy |
| `Consume` | bidi-streaming | Send a topic + `subscriber_id` once; server drains and streams back everything currently queued for that subscriber, then closes |

## Usage example

```go
package main

import (
    "context"
    "fmt"

    "github.com/mddfaisal/quash/client"
    "github.com/mddfaisal/quash/proto"
)

func main() {
    ctx := context.Background()

    // Set a key with a 5 minute TTL (time_out_duration is in seconds)
    resp, err := client.SetKV(ctx, &proto.SetKVRequest{
        Key:             "user:123",
        Value:           "John Doe",
        TimeOutDuration: 300,
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("SetKV response:", resp)

    // Create a topic and add a subscriber
    if _, err := client.CreateTopic(ctx, &proto.CreateTopicRequest{TopicName: "orders"}); err != nil {
        panic(err)
    }
    sub, err := client.AddSubscriber(ctx, &proto.AddSubscriberRequest{TopicName: "orders"})
    if err != nil {
        panic(err)
    }
    fmt.Println("Subscriber ID:", sub.SubscriberId)

    // Publish a value (Publish/Consume use channel-driven streaming helpers)
    pubReq, pubResp := make(chan string), make(chan string)
    go client.Publish("orders", pubReq, pubResp)
    pubReq <- "process_order_42"
    fmt.Println("Publish ack:", <-pubResp)
    close(pubReq)

    // Consume whatever is queued for this subscriber right now
    consumed := make(chan string)
    if err := client.Consume("orders", sub.SubscriberId, consumed); err != nil {
        fmt.Println("Consume error:", err)
    }
    for msg := range consumed {
        fmt.Println("Consumed:", msg)
    }
}
```

## Regenerating protobuf code

Requires `protoc` and the Go protobuf/gRPC plugins:

```bash
protoc --go_out=. --go-grpc_out=. proto/quash_proto.proto
```

## Known limitations

- Single-node, in-memory only — no persistence or durability across restarts
- No authentication/authorization or multi-tenancy
- `Consume` is pull-and-drain, not push: a consumer only sees messages queued up to the moment it calls `Consume`, and has to call it again (e.g. on a timer) to pick up anything published afterward — there's no long-lived, server-pushed delivery yet
- A subscriber only receives messages published after it was added; there's no replay of earlier messages
- `client.Publish`'s ack channel is never closed by the client helper, so a goroutine reading from it will block forever after `Publish` returns unless the caller handles that itself

## Contributing

Contributions are welcome — please open issues or pull requests.

## License

See the [LICENSE](LICENSE) file.
