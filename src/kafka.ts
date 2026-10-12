/**
 * Kafka transport on kafkajs (optional peer dependency: npm install kafkajs).
 *
 *   import { kafkaFactory } from "@notify-ai/client/kafka";
 *   const client = new NotifyClient({ ..., kafkaEnabled: true, kafka: kafkaFactory({ brokers: ["broker:9092"] }) });
 */
import { Kafka, logLevel, type Consumer, type Producer, type SASLOptions } from "kafkajs";

export const DEFAULT_BROKER = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092";

/** Mirrors the Java SDK's kafka.* properties. */
export interface KafkaConfig {
  brokers?: string[];
  /** Default "vocab-agent-group". */
  groupId?: string;
  /** Default "<clientToken>-producer" / "-consumer". */
  clientId?: string;
  /** TLS; forced on (with SASL PLAIN) when registration returns an API key/secret. */
  ssl?: boolean;
  sasl?: SASLOptions;
  /** Start a new consumer group at the earliest offset (Java default). Default true. */
  fromBeginning?: boolean;
  logLevel?: logLevel;
}
/** Kafka transport interface. The SDK picks topic, partition, key and headers
 * exactly as the Java SDK does; a transport only writes and reads records. */

export const EVENTS_TOPIC = "notify-v1-events";
export const SCHEDULED_EVENTS_TOPIC = "notify-v1-scheduled-events";

export interface KafkaRecord {
  topic: string;
  partition: number;
  key: string;
  value: string;
  headers: Record<string, string>;
}

/** From the acp-server registration response. */
export interface KafkaCredentials {
  clientId: string;
  apiKey: string;
  apiSecret: string;
}

export interface KafkaTransport {
  partitions(topic: string): Promise<number>;
  produce(records: KafkaRecord[]): Promise<void>;
  /** Deliver record values to handle until signal aborts. */
  consume(topic: string, handle: (value: Buffer) => Promise<void> | void, signal: AbortSignal): Promise<void>;
  close(): Promise<void>;
}

export type KafkaFactory = (creds: KafkaCredentials) => Promise<KafkaTransport> | KafkaTransport;

export class KafkaJsTransport implements KafkaTransport {
  private readonly kafka: Kafka;
  private readonly consumerKafka: Kafka;
  private readonly producer: Producer;
  private connected?: Promise<void>;
  private readonly consumers = new Set<Consumer>();


  constructor(private readonly cfg: KafkaConfig, creds: KafkaCredentials) {
    let ssl = cfg.ssl;
    let sasl = cfg.sasl;
    if (creds.apiKey && creds.apiSecret) {
      ssl = true;
      sasl = { mechanism: "plain", username: creds.apiKey, password: creds.apiSecret };
    }
    const clientId = (role: string) => cfg.clientId || (creds.clientId ? `${creds.clientId}-${role}` : "notify-ai-client");
    const base = { brokers: cfg.brokers?.length ? cfg.brokers : [DEFAULT_BROKER], ssl, sasl, logLevel: cfg.logLevel ?? logLevel.WARN };
    this.kafka = new Kafka({ ...base, clientId: clientId("producer") });
    this.consumerKafka = new Kafka({ ...base, clientId: clientId("consumer") });
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
  }

  private connect(): Promise<void> {
    return (this.connected ??= this.producer.connect());
  }

  async partitions(topic: string): Promise<number> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const meta = await admin.fetchTopicMetadata({ topics: [topic] });
      return meta.topics.find((t) => t.name === topic)?.partitions.length ?? 0;
    } finally {
      await admin.disconnect();
    }
  }

  async produce(records: KafkaRecord[]): Promise<void> {
    await this.connect();
    const byTopic = new Map<string, KafkaRecord[]>();
    for (const r of records) byTopic.set(r.topic, [...(byTopic.get(r.topic) ?? []), r]);
    await this.producer.sendBatch({
      acks: -1,
      topicMessages: [...byTopic].map(([topic, rs]) => ({
        topic,
        messages: rs.map((r) => ({ key: r.key, value: r.value, partition: r.partition, headers: r.headers })),
      })),
    });
  }

  async consume(topic: string, handle: (value: Buffer) => Promise<void> | void, signal: AbortSignal): Promise<void> {
    const consumer = this.consumerKafka.consumer({ groupId: this.cfg.groupId ?? "vocab-agent-group" });
    this.consumers.add(consumer);
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: this.cfg.fromBeginning ?? true });
    // Offsets are committed after each handled message (Java: auto-commit off).
    await consumer.run({ autoCommit: true, eachMessage: async ({ message }) => {
      if (message.value) await handle(message.value);
    } });
    await new Promise<void>((resolve) => {
      if (signal.aborted) return resolve();
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
    await consumer.disconnect();
    this.consumers.delete(consumer);
  }

  async close(): Promise<void> {
    await Promise.all([...this.consumers].map((c) => c.disconnect()));
    if (this.connected) await this.producer.disconnect();
  }
}

export const kafkaFactory = (cfg: KafkaConfig = {}): KafkaFactory => (creds) => new KafkaJsTransport(cfg, creds);
