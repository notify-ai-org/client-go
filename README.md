<p align="center">
  <img src="assets/notify-ai-logo.svg" alt="Notify.ai" width="96" />
</p>

<h1 align="center">Notify.ai</h1>

<p align="center"><b>Go Client SDK</b> — event capture and metadata transmission for Go services</p>

---

## 📖 Overview

The Go counterpart of the Java client SDK (`dev.notify-ai:notify-ai-agent-client`). It speaks the same protocol to acp-server — registration, vocabulary, rules and event captures over HTTP or Kafka — so a Go service and a Java service look the same to the control plane.

Go has no annotations or AOP, so each Java annotation becomes a registration call:

| Java | Go |
|---|---|
| `@EnableNotify` + `notify.ai.properties.*` | `notify.New(cfg)` + `client.Start()` (or `notify.ConfigFromEnv()`) |
| `@Model` / `@Vocabulary` | struct tags `notify:"name" notifyDesc:"..."`, `notify.RegisterModel[T]`, optional `NotifyModelDescription()` |
| `@Event` | `notify.Event(client, spec, fn)` — returns a wrapped function with the same signature |
| `@Rule` | `notify.Rule(client, notify.RuleSpec{...}, fn)` |
| `@SubjectSupplier` | `notify.SubjectSupplier(client, event, description, fn)` |
| `@VocabularySupplier` | `notify.VocabularySupplier(client, event, description, fn)` |
| `@Callback(when = BEFORE/AFTER)` | `notify.Callback(client, event, notify.Before/After, fn)` |
| `EmailSubject`, `SmsSubject`, … | `notify.NewEmailSubject(...)`, `notify.NewSmsSubject(...)`, … (same JSON) |

## 🚀 Quick start

```bash
go get github.com/notify-ai-org/client-go
```

```go
type OrderPayload struct {
    OrderID    string  `json:"orderId"    notify:"orderId"    notifyDesc:"Unique order identifier"`
    CustomerID string  `json:"customerId" notify:"customerId" notifyDesc:"Customer who placed the order"`
    Amount     float64 `json:"amount"     notify:"amount"     notifyDesc:"Total order amount in USD"`
}

func (OrderPayload) NotifyModelDescription() string { return "Payload for order placement events" }

client := notify.New(notify.Config{
    ACPServerURL:    "http://localhost:8080",
    ApplicationName: "orders-service",
    ClientToken:     "client-...",
})

// @Event: call placeOrder wherever you called svc.PlaceOrder.
placeOrder := notify.Event(client, notify.EventSpec{
    Key: "ORDER_PLACED", Description: "Customer placed an order",
    EventType: "static", ScheduleIntent: "immediate", PreferredTimeWindow: "09:00-18:00", Priority: 2,
}, svc.PlaceOrder) // func(context.Context, OrderPayload) (OrderPayload, error)

notify.SubjectSupplier(client, "ORDER_PLACED", "Order customer", func(ctx context.Context, p OrderPayload) ([]notify.Subject, error) {
    c := customers.Get(p.CustomerID)
    return []notify.Subject{notify.NewEmailSubject(c.Email, "", "", "", map[string]string{"firstName": c.FirstName})}, nil
})

notify.Rule(client, notify.RuleSpec{Name: "fraud-check", Event: "ORDER_PLACED"},
    func(ctx context.Context, p OrderPayload) (bool, error) { return p.Amount < 1000, nil })

if err := client.Start(); err != nil { log.Fatal(err) }
defer client.Close(context.Background()) // flushes buffered captures
```

`notify.Emitter[P](client, spec)` registers an event with no business logic and returns `func(ctx, P) error`, for call sites that just need to fire the event.

A complete port of the e-commerce example (every hook type) is in [examples/ecommerce](examples/ecommerce/main.go).

## 🔁 What a wrapped call does

Same sequence as the Java `EventListener` aspect:

1. `Before` callbacks (errors and panics are logged and ignored)
2. Your function. Errors are returned unchanged; panics are recorded and re-raised.
3. `After` callbacks
4. The subject supplier (subjects are validated: each needs a non-blank address) and every rule for the event, plus rules registered without an event (`"*"`)
5. An `EventCapture` is buffered with the flattened payload, the call stack (5 application frames), duration, result or exception, subjects and rule results

A background dispatcher sends buffered records to acp-server.

## ⚙️ Configuration

| Field | Env var (`ConfigFromEnv`) | Default |
|---|---|---|
| `ACPServerURL` | `NOTIFY_AI_ACP_SERVER_URL` | `https://app.notify-ai.dev` |
| `ApplicationName` | `NOTIFY_AI_APPLICATION_NAME` | `notify-client` |
| `ClientToken` | `NOTIFY_AI_CLIENT_TOKEN` | — |
| `BasePackage` | `NOTIFY_AI_BASE_PACKAGE` | — |
| `BufferBatchSize` | `NOTIFY_AI_BUFFER_BATCH_SIZE` | 100 |
| `FlushInterval` | `NOTIFY_AI_FLUSH_INTERVAL_MS` | 100ms |
| `MaxBufferSize` | `NOTIFY_AI_MAX_BUFFER_SIZE` | 10000 (negative = unbounded) |
| `KafkaEnabled` | `NOTIFY_AI_KAFKA_ENABLED` | false |
| `HTTPClient`, `Logger` | — | `http.Client{}`, `slog.Default()` |

## 📨 Kafka

The core `notify` package has no dependencies. Kafka support is in the `kafka` subpackage (franz-go):

```go
import notifykafka "github.com/notify-ai-org/client-go/kafka"

client := notify.New(notify.Config{..., KafkaEnabled: true},
    notify.WithKafka(notifykafka.Factory(notifykafka.Config{Brokers: []string{"broker:9092"}})))
```

Behavior matches the Java dispatcher:
- Captures go to `notify-v1-events`: one record per subject with key `tenantId:event:subjectId`, partitioned by `abs(subjectId.hashCode()) % partitions` using Java's `String.hashCode`. The same subject therefore lands on the same partition from either SDK.
- The `Authorization: Bearer …` header carries the Kafka header token from registration (falling back to the access token). `tenantId` is read from that JWT.
- When the access token has `profile: basic`, captures use HTTP even if Kafka is enabled.
- An API key and secret returned at registration switch the connection to SASL_SSL / PLAIN.
- Scheduled triggers are consumed from `notify-v1-scheduled-events` (group `vocab-agent-group`). They become `SCHEDULED` captures whose payload comes from the event's `VocabularySupplier`; inside it, `notify.ScheduleFromContext(ctx)` returns the trigger.
- Captures whose payload contains credential-like fields (`password`, `apiKey`, `*secret`, private keys, …) are rejected before publishing. This is the Java `CredentialPayloadPolicy`, also exported as `notify.ValidateCredentialFree`.

## ↔️ Differences from the Java SDK

Wire format and endpoints are identical. These runtime behaviors differ on purpose:

| | Java | Go |
|---|---|---|
| Registration failure | Logs and leaves the SDK inactive; captures are never sent | Retries with backoff (1s→60s); captures buffer meanwhile |
| Buffer | Unbounded | Capped at `MaxBufferSize`; drops the oldest capture when full (`Metrics().DroppedCaptureCount`) |
| Failed sends | Retried after 1s; the batch can be reprocessed | Unsent records are requeued in order; backoff 1s→30s |
| Shutdown | Stops the dispatcher; pending records are lost | `Close(ctx)` flushes within the deadline |
| Token refresh | Only on 401, and sends a null `clientId` | Also before expiry; sends the registered client id |
| Kafka capture with no subjects | Not sent | Sent as one tenant-keyed record |
| `Integer.MIN_VALUE` subject hash | Negative partition (send fails) | Uses the absolute value |
| Scheduled-event offsets | Committed only on rebalance or shutdown | Committed after each poll |
| Hook registration errors | Exception at startup | Panic at registration (blank key, priority outside 1–5, blank rule name) |
| Payload type for a scheduled `VocabularySupplier` | `null` | Zero value of `P` |
| `bufferFlushTimeoutMs` | Present, but the dispatcher polls every 100ms anyway | Replaced by `FlushInterval` |

## 🧪 Development

```bash
go test -race ./...
```

`kafka/integration_test.go` runs the client against an in-process Kafka cluster (franz-go `kfake`), so no broker is needed. It checks partitions, keys, headers and scheduled-event consumption. `notify/unit_test.go` checks the hash function against values produced by a real JVM.
