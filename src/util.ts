import { randomUUID } from "node:crypto";
import { Hook, BufRecord } from "./interfaces.js";
import { Capture } from "./types.js";
import { SDK_DIR } from "./config.js";

export const newUUID = (): string => randomUUID();

/**
 * java.lang.String#hashCode over UTF-16 code units with int32 overflow, so
 * Kafka partition choice matches the Java SDK.
 */
export function javaStringHashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

/** Math.abs(s.hashCode()) % n, without Java's negative result for Integer.MIN_VALUE. */
export function javaPartition(s: string, n: number): number {
  if (n <= 0) return 0;
  return Math.abs(javaStringHashCode(s)) % n;
}

/**
 * Decodes a JWT payload without verifying it. The SDK never holds the server's
 * signing key; claims are read only to pick a transport and a partition key.
 */
export function jwtClaims(token: string | undefined | null): Record<string, unknown> {
  if (!token) return {};
  const parts = token.trim().replace(/^Bearer\s+/, "").split(".");
  if (parts.length !== 3) return {};
  try {
    const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
    return claims && typeof claims === "object" ? (claims as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

export const tenantIdFromToken = (token?: string | null): string => {
  const v = jwtClaims(token).tenantId;
  return v === undefined || v === null ? "unknown-tenant" : String(v);
};

export const hasBasicProfile = (token?: string | null): boolean => jwtClaims(token).profile === "basic";

/** JSON.stringify that also handles bigint, Map and Set. */
export function stringify(value: unknown): string {
  return JSON.stringify(value, (_k, v) => {
    if (typeof v === "bigint") return v.toString();
    if (v instanceof Map) return Object.fromEntries(v);
    if (v instanceof Set) return [...v];
    return v;
  });
}

/** Jackson Instant forms: ISO string or epoch seconds. */
export function parseInstant(v: unknown): Date | undefined {
  if (v === null || v === undefined) return undefined;
  if (typeof v === "number") return new Date(v * 1000);
  return new Date(String(v));
}

export const sleep = (ms: number, signal?: AbortSignal): Promise<boolean> =>
  new Promise((resolve) => {
    if (signal?.aborted) return resolve(false);
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve(true);
    }, ms);
    t.unref?.();
    const onAbort = () => {
      clearTimeout(t);
      resolve(false);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });


  /** Up to 5 application frames from a stack trace, skipping the SDK. */
export function callerFrames(limit = 5): Capture[] {
  const stack = new Error().stack?.split("\n").slice(1) ?? [];
  const frames: Capture[] = [];
  for (const line of stack) {
    if (frames.length >= limit) break;
    const m = line.match(/^\s*at (?:(.+?) \()?(.+?):(\d+):\d+\)?$/);
    if (!m) continue;
    const file = m[2].replace(/^file:\/\//, "");
    if (file.startsWith(SDK_DIR) || file.startsWith("node:")) continue;
    const fn = (m[1] ?? "<anonymous>").replace(/^async /, "");
    const dot = fn.lastIndexOf(".");
    frames.push({
      className: dot > 0 ? fn.slice(0, dot) : "",
      methodName: dot > 0 ? fn.slice(dot + 1) : fn,
      lineNumber: Number(m[3]),
      fileName: file,
    });
  }
  return frames;
}

export const isAsyncFunction = (fn: unknown): boolean =>
  typeof fn === "function" && fn.constructor?.name === "AsyncFunction";

export function ruleRecord(hook: Hook): BufRecord {
  return { type: "RULE", payload: { eventName: hook.event, ruleName: hook.name, ruleDescription: hook.description } };
}

export const isPromiseLike = (v: unknown): v is PromiseLike<unknown> =>
  !!v && (typeof v === "object" || typeof v === "function") && typeof (v as PromiseLike<unknown>).then === "function";
