/**
 * Runs the SDK over a real Kafka protocol endpoint. Opt-in:
 *   NOTIFY_TEST_KAFKA_BROKERS=localhost:9092 npm test
 * The broker needs notify-v1-events (12 partitions) and notify-v1-scheduled-events.
 */
import { Kafka, logLevel } from "kafkajs";
import { describe, expect, it } from "vitest";
import { EVENTS_TOPIC, NotifyClient, SCHEDULED_EVENTS_TOPIC, SmsSubject } from "../src/index.js";
import { kafkaFactory } from "../src/kafka.js";
import { FakeACP, quietLogger, until } from "./helpers.js";

const BROKERS = process.env.NOTIFY_TEST_KAFKA_BROKERS;

describe.skipIf(!BROKERS)("Kafka integration", () => {
  it("produces, partitions and consumes over the real protocol", async () => {
    const brokers = BROKERS!.split(",");
    const acp = await new FakeACP().start();
    const client = new NotifyClient({
      acpServerUrl: acp.url, clientToken: "client-123", kafkaEnabled: true, flushIntervalMs: 10, flushOnExit: false,
      logger: quietLogger, kafka: kafkaFactory({ brokers, groupId: `ts-${Date.now()}`, logLevel: logLevel.NOTHING }),
    });
    const emit = client.emitter<{ orderId: string; customerId: string }>({ key: "ORDER_PLACED", priority: 2 });
    client.subjectSupplier<{ customerId: string }>("ORDER_PLACED", (p) => [new SmsSubject(p.customerId)]);
    client.vocabularySupplier("REMINDER", (s) => ({ orderId: `scheduled-${s.id}` }));

    const kafka = new Kafka({ brokers, logLevel: logLevel.NOTHING });
    const got = new Map<string, { partition: number; value: string; auth: string }>();
    const reader = kafka.consumer({ groupId: `reader-${Date.now()}` });
    const pub = kafka.producer();
    try {
      client.start();
      await client.waitReady(20_000);
      emit({ orderId: "O-1", customerId: "CUST-1" });
      await pub.connect();
      await pub.send({ topic: SCHEDULED_EVENTS_TOPIC, messages: [{ value: '{"id":"s-1","eventName":"REMINDER"}' }] });

      await reader.connect();
      await reader.subscribe({ topic: EVENTS_TOPIC, fromBeginning: true });
      await reader.run({
        eachMessage: async ({ partition, message }) => {
          got.set(message.key!.toString(), {
            partition, value: message.value!.toString(), auth: message.headers?.Authorization?.toString() ?? "",
          });
        },
      });
      await until(() => got.size >= 3, 30_000);
    } finally {
      await reader.disconnect();
      await pub.disconnect();
      await client.close();
      await acp.stop();
    }
    expect(Object.fromEntries([...got].map(([k, v]) => [k, v.partition]))).toEqual({
      "tenant-42:ORDER_PLACED": 7, "tenant-42:ORDER_PLACED:CUST-1": 11, "tenant-42:REMINDER": 7,
    });
    for (const v of got.values()) expect(v.auth).toBe(`Bearer ${acp.token}`);
    const scheduled = JSON.parse(got.get("tenant-42:REMINDER")!.value);
    expect(scheduled.event.eventType).toBe("SCHEDULED");
    expect(scheduled.payload).toEqual({ orderId: "scheduled-s-1" });
  }, 60_000);
});
