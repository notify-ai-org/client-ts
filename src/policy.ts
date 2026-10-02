/** Port of the Java CredentialPayloadPolicy: payloads must not carry credentials. */

const KEYS = [
  "credentials", "secret", "signingsecret", "callbacksecret", "password", "apikey", "apisecret", "authtoken",
  "accesstoken", "refreshtoken", "authorization", "privatekey", "serviceaccountjson", "secretstring",
  "secretbinary", "awssecretarn",
];

export class CredentialsInPayloadError extends Error {
  constructor() {
    super("Credentials are not allowed in notification payloads");
    this.name = "CredentialsInPayloadError";
  }
}

/**
 * Throws if keys look like credentials (password, apiKey, *_secret, ...) or
 * strings contain private keys. Message prose is not inspected.
 */
export function validateCredentialFree(data: unknown): void {
  let tree: unknown;
  try {
    tree = JSON.parse(JSON.stringify(data ?? null));
  } catch {
    throw new CredentialsInPayloadError();
  }
  if (!ok(tree, 0)) throw new CredentialsInPayloadError();
}

function ok(node: unknown, depth: number): boolean {
  if (depth > 50) return false;
  if (Array.isArray(node)) return node.every((v) => ok(v, depth + 1));
  if (node && typeof node === "object") {
    for (const [k, v] of Object.entries(node)) {
      const compact = k.toLowerCase().replace(/[^a-z0-9]/g, "");
      if (KEYS.some((key) => compact === key || compact.endsWith(key))) return false;
      if (!ok(v, depth + 1)) return false;
    }
    return true;
  }
  if (typeof node === "string") {
    const text = node.trim();
    if (text.includes("-----BEGIN PRIVATE KEY-----") || text.includes("-----BEGIN RSA PRIVATE KEY-----")) return false;
    if (text.startsWith("{") || text.startsWith("[")) {
      try {
        return ok(JSON.parse(text), depth + 1);
      } catch {
        return true;
      }
    }
  }
  return true;
}
