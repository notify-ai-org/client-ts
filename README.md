<p align="center">
  <img src="assets/notify-ai-logo.svg" alt="Notify.ai" width="96" />
</p>

<h1 align="center">Notify.ai</h1>

<p align="center"><b>TypeScript Client SDK</b> — event capture and metadata transmission for Node.js services</p>

---

## 📖 Overview

The TypeScript/Node.js counterpart of the Java client SDK (`dev.notify-ai:notify-ai-agent-client`) and the Go and Python SDKs. It speaks the same protocol to acp-server (registration, vocabulary, rules and event captures over HTTP or Kafka), so services in any of these languages look the same to the control plane.

It needs Node ≥ 18 and has no runtime dependencies (it uses the built-in `fetch`). Kafka support is in an optional subpath built on `kafkajs`.

| Java | TypeScript |
|---|---|
| `@EnableNotify` + `notify.ai.properties.*` | `new NotifyClient({...})` + `client.start()` (or `configFromEnv()`) |
| `@Model` / `@Vocabulary` | `defineModel<T>("Name", { description, fields: { key: field.string("…") } })` |
| `@Event` | `client.event(spec, fn)` or the `@client.Event(spec)` method decorator |
| `@Rule` | `client.rule({ name, event }, fn)` |
| `@SubjectSupplier` | `client.subjectSupplier(event, fn)` |
| `@VocabularySupplier` | `client.vocabularySupplier(event, (schedule) => payload)` |
| `@Callback(when = BEFORE/AFTER)` | `client.callback(event, BEFORE, fn)` |
| `EmailSubject`, `SmsSubject`, … | `new EmailSubject(...)`, `new SmsSubject(...)`, … (same JSON) |

## 🚀 Quick start

```bash
npm install @notify-ai/client
npm install kafkajs   # only for the Kafka transport
```

```ts
import { NotifyClient, defineModel, field, EmailSubject } from "@notify-ai/client";

interface OrderPayload { orderId: string; customerId: string; amount: number }

// TypeScript types are erased at runtime, so vocabulary is declared.
// Field keys are checked against OrderPayload.
const OrderModel = defineModel<OrderPayload>("OrderPayload", {
  description: "Payload for order placement events",
  fields: {
    orderId: field.string("Unique order identifier"),
    customerId: field.string("Customer who placed the order"),
    amount: field.double("Total order amount in USD"),
  },
});

const client = new NotifyClient({ acpServerUrl: "http://localhost:8080", applicationName: "orders-service", clientToken: "client-..." });

class OrderService {
  @client.Event<OrderPayload>({ key: "ORDER_PLACED", description: "Customer placed an order", eventType: "static",
    scheduleIntent: "immediate", preferredTimeWindow: "09:00-18:00", priority: 2, payload: OrderModel })
  async placeOrder(p: OrderPayload) { /* ... */ }
}

// Or without decorators:
const placeOrder = client.event({ key: "ORDER_PLACED", priority: 2, payload: OrderModel }, (p: OrderPayload) => p);

client.subjectSupplier<OrderPayload>("ORDER_PLACED", async (p) => {
  const c = await customers.get(p.customerId);
  return [new EmailSubject(c.email, { attributes: { firstName: c.firstName } })];
});

client.rule<OrderPayload>({ name: "fraud-check", event: "ORDER_PLACED" }, (p) => p.amount < 1000);

client.start(); // non-blocking; captures are flushed before exit (or await client.close())
```

**The payload** is the event function's first argument. Without a `payload` model, an object payload is flattened by its own keys. The decorator works with standard TypeScript 5 decorators and with `experimentalDecorators`.

**Sync and async:** a wrapped sync function stays sync, and a wrapped async function returns its promise. Hooks may be async in both cases, because the capture is built in the background. Async `BEFORE` callbacks are awaited before native `async` functions run.

`client.emitter(spec)` returns `emit(payload)` for call sites that just need to fire an event.

A complete port of the e-commerce example (every hook type) is in [examples/ecommerce.ts](examples/ecommerce.ts).

## 🔁 What a wrapped call does

Same sequence as the Java `EventListener` aspect:

1. `BEFORE` callbacks (errors are logged and ignored)
2. Your function. Errors are recorded and rethrown unchanged.
3. `AFTER` callbacks
4. The subject supplier and every rule for the event, plus rules registered without an event (`"*"`). Each subject must have a non-blank address.
5. A capture is buffered with the flattened payload, the call stack (5 application frames), duration, result or exception, subjects and rule results

A background loop sends buffered records to acp-server. Its timers are `unref`'d, so they never keep the process alive.

## ⚙️ Configuration

| Option | Env var (`configFromEnv()`) | Default |
|---|---|---|
| `acpServerUrl` | `NOTIFY_AI_ACP_SERVER_URL` | `https://app.notify-ai.dev` |
| `applicationName` | `NOTIFY_AI_APPLICATION_NAME` | `notify-client` |
| `clientToken` | `NOTIFY_AI_CLIENT_TOKEN` | — |
| `basePackage` | `NOTIFY_AI_BASE_PACKAGE` | — |
| `bufferBatchSize` | `NOTIFY_AI_BUFFER_BATCH_SIZE` | 100 |
| `flushIntervalMs` | `NOTIFY_AI_FLUSH_INTERVAL_MS` | 100 |
| `maxBufferSize` | `NOTIFY_AI_MAX_BUFFER_SIZE` | 10000 (negative = unbounded) |
| `kafkaEnabled` + `kafka` | `NOTIFY_AI_KAFKA_ENABLED` | false |
| `flushOnExit` | — | true (flushes on `beforeExit`) |
| `logger` | — | `console` |

## 📨 Kafka

```ts
import { kafkaFactory } from "@notify-ai/client/kafka";

const client = new NotifyClient({ ...config, kafkaEnabled: true, kafka: kafkaFactory({ brokers: ["broker:9092"] }) });
```

Behavior matches the Java dispatcher and the Go and Python SDKs:
- Captures go to `notify-v1-events`: one record per subject with key `tenantId:event:subjectId`, partitioned by Java's `String.hashCode` so a subject lands on the same partition from any SDK.
- The `Authorization: Bearer …` header carries the Kafka header token from registration.
- An access token with `profile: basic` forces HTTP.
- An API key and secret returned at registration switch the connection to SSL + SASL PLAIN.
- Scheduled triggers from `notify-v1-scheduled-events` become `SCHEDULED` captures. The event's vocabulary supplier receives the trigger and returns the payload.
- Payloads with credential-like fields are rejected before publishing (`validateCredentialFree`).

kafkajs only bundles GZIP compression, so records are sent uncompressed (the Java SDK uses snappy). Brokers accept both.

## ↔️ Differences from the Java SDK

Wire format and endpoints are identical. Runtime behavior follows the Go SDK:
- Registration is retried with backoff instead of disabling the SDK.
- The buffer is capped (oldest capture dropped first).
- Failed sends are requeued in order with backoff.
- Token refresh happens before expiry as well as on 401, and sends the registered client id.
- On Kafka, a capture with no subjects is sent as one tenant-keyed record.
- `close()` and process exit flush pending captures.

See the [Go SDK README](../client-go/README.md#️-differences-from-the-java-sdk) for the full table.

## 🧪 Development

```bash
npm install
npm run typecheck && npm test && npm run build
# Optional, against a real broker with notify-v1-events (12 partitions) and notify-v1-scheduled-events:
NOTIFY_TEST_KAFKA_BROKERS=localhost:9092 npm test
```
