import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  AFTER, BEFORE, defineModel, EmailSubject, EVENTS_TOPIC, field, NotifyClient, SmsSubject,
  type KafkaRecord, type KafkaTransport, type NotifyConfig,
} from "../src/index.js";
import { FakeACP, fakeJwt, quietLogger, until } from "./helpers.js";

interface OrderPayload { orderId: string; customerId?: string; amount?: number }
const OrderModel = defineModel<OrderPayload>("OrderPayload", {
  description: "Payload for order placement events",
  fields: {
    orderId: field.string("Unique order identifier"),
    customerId: field.string("Customer who placed the order"),
    amount: field.double("Total order amount in USD"),
  },
});

let acp: FakeACP;
const clients: NotifyClient[] = [];
beforeEach(async () => {
  acp = await new FakeACP().start();
});
afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.close().catch(() => {})));
  await acp.stop();
});

function makeClient(extra: Partial<NotifyConfig> = {}) {
  const c = new NotifyClient({
    acpServerUrl: acp.url, applicationName: "test-app", clientToken: "client-123", basePackage: "shop",
    flushIntervalMs: 10, flushOnExit: false, logger: quietLogger, ...extra,
  });
  clients.push(c);
  return c;
}

describe("NotifyClient over HTTP", () => {
  it("captures events end to end", async () => {
    const client = makeClient();
    const calls: string[] = [];

    class OrderService {
      placed: string[] = [];
      @client.Event<OrderPayload>({
        key: "ORDER_PLACED", description: "Customer placed an order", eventType: "static",
        scheduleIntent: "immediate", preferredTimeWindow: "09:00-18:00", priority: 2, payload: OrderModel,
      })
      placeOrder(p: OrderPayload): OrderPayload {
        if ((p.amount ?? 0) < 0) throw new RangeError("negative amount");
        this.placed.push(p.orderId);
        return p;
      }
    }
    const svc = new OrderService();

    client.subjectSupplier<OrderPayload>("ORDER_PLACED", async (p) =>
      [new EmailSubject(p.customerId ? `${p.customerId}@example.com` : "", { attributes: { firstName: "A" } })], "order customer");
    client.rule<OrderPayload>({ name: "fraud-check", event: "ORDER_PLACED", description: "Blocks large orders" }, (p) => (p.amount ?? 0) < 1000);
    client.rule({ name: "always" }, () => true);
    client.callback<OrderPayload>("ORDER_PLACED", BEFORE, (p) => void calls.push(`before:${p.orderId}`));
    client.callback<OrderPayload>("ORDER_PLACED", AFTER, (p) => {
      calls.push(`after:${p.orderId}`);
      throw new Error("ignored");
    });

    svc.placeOrder({ orderId: "O-0", customerId: "alice", amount: 5 }); // before start
    client.start();
    await client.waitReady(5000);
    expect(svc.placeOrder({ orderId: "O-1", customerId: "alice", amount: 120 }).orderId).toBe("O-1"); // stays sync
    expect(() => svc.placeOrder({ orderId: "O-2", amount: -1 })).toThrow(RangeError);
    await client.flush();

    expect(calls).toEqual(["before:O-0", "after:O-0", "before:O-1", "after:O-1", "before:O-2", "after:O-2"]);
    expect(svc.placed).toEqual(["O-0", "O-1"]);
    expect(acp.registrations[0]).toEqual({ clientId: "client-123", applicationName: "test-app", basePackage: "shop" });
    expect(acp.captures[0].event.eventType).toBe("REGISTER");
    expect(acp.vocabulary[0][0]).toMatchObject({ className: "OrderPayload", classDescription: "Payload for order placement events" });
    expect(acp.rules.map((r) => r.ruleName)).toEqual(["fraud-check", "always"]);
    expect(acp.rules[1].eventName).toBe("*");

    const [register] = acp.ofType("REGISTER");
    expect(register.event).toMatchObject({ name: "ORDER_PLACED", priority: 2, scheduleIntent: "immediate" });
    expect(register.payload.EventMetadata).toMatchObject({
      methodName: "placeOrder", version: "v1", declaringClassName: "OrderService", parameterTypeNames: ["OrderPayload"],
    });
    expect(register.serviceName).toBe("OrderService");

    const users = acp.ofType("USER");
    expect(users).toHaveLength(3);
    const ok = users[1];
    expect(ok.payload).toEqual({ orderId: "O-1", customerId: "alice", amount: 120 });
    expect(ok.occuredAt).toBe(ok.timestamp);
    expect(ok.result.success).toBe(true);
    expect(ok.result.returnValue).toContain('"orderId":"O-1"');
    expect(ok.subjectResult.subjects[0].email).toBe("alice@example.com");
    expect(ok.ruleResults.map((r: any) => r.result)).toEqual([true, true]);
    expect(ok.callStack.frames[0].fileName).toContain("client.test.ts");

    const failed = users[2];
    expect(failed.result.success).toBe(false);
    expect(failed.exception).toMatchObject({ exceptionType: "RangeError", message: "negative amount" });
    expect(failed.subjectResult.errorMessage).toContain("without a destination address");
    expect(client.metrics()).toMatchObject({ eventCaptureCount: 3, ruleInvokeCount: 6, subjectSupplierInvokeCount: 3 });
  });

  it("supports async functions and awaits async callbacks around them", async () => {
    const client = makeClient();
    const order: string[] = [];
    client.callback("ASYNC", BEFORE, async () => {
      await new Promise((r) => setTimeout(r, 20));
      order.push("before");
    });
    const handle = client.event({ key: "ASYNC", priority: 1 }, async function handle(p: OrderPayload) {
      order.push("fn");
      return `done-${p.orderId}`;
    });
    client.subjectSupplier("ASYNC", async () => new SmsSubject("+1555"));
    client.start();
    await client.waitReady();
    await expect(handle({ orderId: "A" })).resolves.toBe("done-A");
    await client.flush();
    expect(order).toEqual(["before", "fn"]);
    const [capture] = acp.ofType("USER");
    expect(capture.subjectResult.subjects[0].phoneNumber).toBe("+1555");
    expect(capture.result.returnValue).toBe('"done-A"');
    expect(capture.serviceName).toBe("default");
  });

  it("sends registrations made after start", async () => {
    const client = makeClient();
    client.start();
    await client.waitReady();
    const emit = client.emitter({ key: "LATE", priority: 1, payload: defineModel<{ x: number }>("Late", { fields: { x: field.int() } }) });
    client.rule({ name: "late-rule", event: "LATE" }, () => true);
    emit({ x: 1 });
    await client.flush();
    expect(acp.ofType("REGISTER")).toHaveLength(1);
    expect(acp.ofType("USER")[0].payload).toEqual({ x: 1 });
    expect(acp.vocabulary.map((v) => v[0].className)).toEqual(["Late"]);
    expect(acp.rules).toHaveLength(1);
  });

  it("refreshes the token on 401", async () => {
    const client = makeClient();
    const emit = client.emitter<OrderPayload>({ key: "E", priority: 1 });
    client.start();
    await client.waitReady();
    await client.flush();
    acp.reject401Once = true;
    emit({ orderId: "X" });
    await client.flush();
    expect(acp.refreshed).toBe(1);
    expect(acp.captures.at(-1).payload).toEqual({ orderId: "X" });
  });

  it("retries registration and delivers buffered captures", async () => {
    acp.failRegisters = 1;
    const client = makeClient();
    const emit = client.emitter<OrderPayload>({ key: "E", priority: 1 });
    client.start();
    emit({ orderId: "queued" });
    await client.waitReady(5000);
    await until(() => acp.ofType("USER").length > 0);
    expect(acp.ofType("USER")).toHaveLength(1);
  });

  it("supports legacy (experimentalDecorators) method decorators", async () => {
    const client = makeClient();
    class Billing {
      charge(p: OrderPayload) {
        return p.amount;
      }
    }
    const descriptor = Object.getOwnPropertyDescriptor(Billing.prototype, "charge")!;
    Object.defineProperty(Billing.prototype, "charge",
      client.Event({ key: "CHARGED", priority: 3 })(Billing.prototype, "charge", descriptor));
    client.start();
    await client.waitReady();
    expect(new Billing().charge({ orderId: "B-1", amount: 9 })).toBe(9);
    await client.flush();
    const [register] = acp.ofType("REGISTER");
    expect(register.payload.EventMetadata).toMatchObject({ methodName: "charge", declaringClassName: "Billing" });
    expect(acp.ofType("USER")[0].payload).toEqual({ orderId: "B-1", amount: 9 });
  });

  it("rejects invalid specs", () => {
    const client = new NotifyClient({ flushOnExit: false });
    expect(() => client.emitter({ key: "", priority: 1 })).toThrow();
    expect(() => client.emitter({ key: "K", priority: 6 })).toThrow();
    expect(() => client.rule({ name: "" }, () => true)).toThrow();
  });
});

class FakeKafka implements KafkaTransport {
  records: KafkaRecord[] = [];
  schedule: Buffer[] = [];
  creds: unknown;
  async partitions() {
    return 12;
  }
  async produce(records: KafkaRecord[]) {
    this.records.push(...records);
  }
  async consume(_topic: string, handle: (v: Buffer) => Promise<void> | void, signal: AbortSignal) {
    while (!signal.aborted) {
      const next = this.schedule.pop();
      if (next) await handle(next);
      await new Promise((r) => setTimeout(r, 5));
    }
  }
  async close() {}
}

describe("Kafka transport path", () => {
  it("partitions like the Java SDK and consumes scheduled events", async () => {
    const kafka = new FakeKafka();
    const client = makeClient({ kafkaEnabled: true, kafka: (creds) => ((kafka.creds = creds), kafka) });
    const emit = client.emitter<OrderPayload>({ key: "ORDER_PLACED", priority: 2 });
    const leak = client.emitter<{ password: string }>({ key: "LEAK", priority: 1 });
    client.subjectSupplier("ORDER_PLACED", () => [new SmsSubject("CUST-1"), new EmailSubject("alice@example.com")]);
    client.vocabularySupplier("REMINDER", (schedule) => ({ orderId: `from-${schedule.id}` }));

    client.start();
    await client.waitReady();
    expect(kafka.creds).toMatchObject({ clientId: "client-123" });
    emit({ orderId: "K-1" });
    leak({ password: "hunter2" });
    kafka.schedule.push(Buffer.from('{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}'));
    await until(() => kafka.records.length >= 5);
    await client.flush();

    const byKey = Object.fromEntries(kafka.records.map((r) => [r.key, r]));
    for (const r of kafka.records) {
      expect(r.topic).toBe(EVENTS_TOPIC);
      expect(r.headers.Authorization).toBe(`Bearer ${acp.token}`);
      expect(r.value).not.toContain("hunter2");
    }
    // Must equal Java's Math.abs(hashCode) % 12.
    expect({
      sms: byKey["tenant-42:ORDER_PLACED:CUST-1"].partition,
      email: byKey["tenant-42:ORDER_PLACED:alice@example.com"].partition,
      register: byKey["tenant-42:ORDER_PLACED"].partition,
      scheduled: byKey["tenant-42:REMINDER"].partition,
    }).toEqual({ sms: 11, email: 1, register: 7, scheduled: 7 });
    const scheduled = JSON.parse(byKey["tenant-42:REMINDER"].value);
    expect(scheduled.event.eventType).toBe("SCHEDULED");
    expect(scheduled.payload).toEqual({ orderId: "from-s-1" });
    expect(acp.ofType("USER")).toHaveLength(0);
  });

  it("uses HTTP when the token has the basic profile", async () => {
    acp.token = fakeJwt({ tenantId: "t", profile: "basic" });
    const kafka = new FakeKafka();
    const client = makeClient({ kafkaEnabled: true, kafka: () => kafka });
    const emit = client.emitter<OrderPayload>({ key: "E", priority: 1 });
    client.start();
    await client.waitReady();
    emit({ orderId: "B" });
    await client.flush();
    expect(kafka.records).toHaveLength(0);
    expect(acp.ofType("USER")).toHaveLength(1);
  });
});
