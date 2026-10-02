import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

export const fakeJwt = (claims: Record<string, unknown>) => {
  const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
  return `${enc({ alg: "HS256" })}.${enc(claims)}.sig`;
};

/** In-memory acp-server. */
export class FakeACP {
  token = fakeJwt({ tenantId: "tenant-42" });
  failRegisters = 0;
  reject401Once = false;
  refreshed = 0;
  registrations: any[] = [];
  vocabulary: any[][] = [];
  rules: any[] = [];
  captures: any[] = [];
  url = "";
  private server!: Server;

  async start() {
    this.server = createServer((req, res) => {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        const json = body ? JSON.parse(body) : null;
        const auth = (req.headers.authorization ?? "").replace("Bearer ", "");
        const reply = (status: number, out: unknown = {}) => {
          res.writeHead(status, { "Content-Type": "application/json" });
          res.end(JSON.stringify(out));
        };
        switch (req.url) {
          case "/client/register":
            if (this.failRegisters > 0) {
              this.failRegisters--;
              return reply(503);
            }
            this.registrations.push(json);
            return reply(200, { token: this.token, refreshToken: "r1", expiresInMs: 60_000 });
          case "/auth/token/refresh":
            this.refreshed++;
            this.token = fakeJwt({ tenantId: "tenant-42", n: this.refreshed });
            return reply(200, { token: this.token, expiresInMs: 60_000 });
        }
        if (this.reject401Once && req.url === "/api/event") {
          this.reject401Once = false;
          return reply(401);
        }
        if (auth !== this.token) return reply(401);
        if (req.url === "/api/vocabulary") this.vocabulary.push(json);
        else if (req.url === "/api/vocabulary/rules/process") this.rules.push(json);
        else if (req.url === "/api/event") this.captures.push(...json);
        else return reply(404);
        reply(200);
      });
    });
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
    return this;
  }

  ofType(t: string) {
    return this.captures.filter((c) => c.event.eventType === t);
  }

  stop() {
    return new Promise<void>((r) => this.server.close(() => r()));
  }
}

export const quietLogger = { info() {}, warn() {}, error() {} };

export async function until(cond: () => boolean, ms = 5000) {
  const end = Date.now() + ms;
  while (!cond() && Date.now() < end) await new Promise((r) => setTimeout(r, 10));
}
