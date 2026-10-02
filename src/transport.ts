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
