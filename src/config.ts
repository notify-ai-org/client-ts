import { Logger } from "kafkajs";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { KafkaFactory } from "./kafka.js";

/** Mirrors the Java NotifyProperties (prefix notify.ai.properties). */
export interface NotifyConfig {
  /** Default https://app.notify-ai.dev */
  acpServerUrl?: string;
  /** Default "notify-client" */
  applicationName?: string;
  /** Identifies this client to acp-server (sent as clientId at registration). */
  clientToken?: string;
  /** Reported at registration; for Node use the package name. */
  basePackage?: string;
  /** Max records per dispatch. Default 100. */
  bufferBatchSize?: number;
  /** Milliseconds between dispatcher drains. Default 100. */
  flushIntervalMs?: number;
  /** Cap on buffered records; the oldest capture is dropped when full. Default 10000; <0 = unbounded. */
  maxBufferSize?: number;
  /** Deliver captures over Kafka and consume scheduled events (requires `kafka`). */
  kafkaEnabled?: boolean;
  kafka?: KafkaFactory;
  /** Flush buffered captures when the process is about to exit (beforeExit). Default true. */
  flushOnExit?: boolean;
  logger?: Logger;
}

/** NOTIFY_AI_* environment variables (same names as the Go and Python SDKs). */
export function configFromEnv(env: NodeJS.ProcessEnv = process.env): NotifyConfig {
  const num = (k: string) => (env[k] ? Number(env[k]) : undefined);
  return {
    acpServerUrl: env.NOTIFY_AI_ACP_SERVER_URL,
    applicationName: env.NOTIFY_AI_APPLICATION_NAME,
    clientToken: env.NOTIFY_AI_CLIENT_TOKEN,
    basePackage: env.NOTIFY_AI_BASE_PACKAGE,
    bufferBatchSize: num("NOTIFY_AI_BUFFER_BATCH_SIZE"),
    flushIntervalMs: num("NOTIFY_AI_FLUSH_INTERVAL_MS"),
    maxBufferSize: num("NOTIFY_AI_MAX_BUFFER_SIZE"),
    kafkaEnabled: ["1", "true", "yes"].includes((env.NOTIFY_AI_KAFKA_ENABLED ?? "").toLowerCase()),
  };
}

export const SDK_DIR = dirname(fileURLToPath(import.meta.url));
