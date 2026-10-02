import { stringify } from "./util.js";

export class NotifyHTTPError extends Error {
  constructor(
    readonly path: string,
    readonly status: number,
    readonly body: string,
  ) {
    super(`POST ${path} failed: ${status} ${body}`);
    this.name = "NotifyHTTPError";
  }
}

/** HTTP client for acp-server (AcpServerClient). */
export class AcpClient {
  constructor(private readonly baseUrl: string) {}

  private async post<T>(path: string, body: unknown, bearer: string | undefined, timeoutMs: number): Promise<T | undefined> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (bearer) headers.Authorization = `Bearer ${bearer}`;
    const res = await fetch(this.baseUrl + path, {
      method: "POST",
      headers,
      body: stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    // 409 Conflict means the server's idempotency filter saw a duplicate.
    if (res.status >= 400 && res.status !== 409) throw new NotifyHTTPError(path, res.status, text.slice(0, 2000));
    return text.trim() ? (JSON.parse(text) as T) : undefined;
  }

  async register(req: Record<string, unknown>): Promise<RegistrationResponse> {
    return (await this.post<RegistrationResponse>("/client/register", req, undefined, 15_000)) ?? {};
  }

  async refreshToken(req: Record<string, unknown>): Promise<{ token?: string; expiresInMs?: number }> {
    return (await this.post<{ token?: string; expiresInMs?: number }>("/auth/token/refresh", req, undefined, 10_000)) ?? {};
  }

  async postVocabulary(models: unknown[], bearer?: string): Promise<void> {
    if (models.length) await this.post("/api/vocabulary", models, bearer, 30_000);
  }

  async postRule(rule: unknown, bearer?: string): Promise<void> {
    await this.post("/api/vocabulary/rules/process", rule, bearer, 30_000);
  }

  async postEventCaptures(captures: unknown[], bearer?: string): Promise<void> {
    if (captures.length) await this.post("/api/event", captures, bearer, 30_000);
  }
}

export interface RegistrationResponse {
  clientId?: string;
  token?: string;
  apiKey?: string;
  apiSecret?: string;
  refreshToken?: string;
  kafkaHeaderToken?: string;
  expiresInMs?: number;
}
