import { describe, expect, it } from "vitest";
import {
  classModel, CredentialsInPayloadError, defineModel, EmailSubject, field, flatten, InAppSubject, javaPartition,
  javaStringHashCode, SmsSubject, validateCredentialFree,
} from "../src/index.js";
import { hasBasicProfile, parseInstant, stringify, tenantIdFromToken } from "../src/util.js";
import { fakeJwt } from "./helpers.js";

describe("java hash parity", () => {
  // Reference values produced by java.lang.String#hashCode on a real JVM.
  const cases: [string, number, number][] = [
    ["", 0, 0], ["hello", 99162322, 10], ["CUST-1", 1999207223, 11], ["alice@example.com", 2145772861, 1],
    ["tenant-42", -1852830079, 7], ["héllo wörld", 1628148953, 5], ["📦 order", 1563790519, 7],
    ["polygenelubricants", -2147483648, 8], // Java partition would be -8 (invalid)
  ];
  it.each(cases)("%s", (s, hash, partition) => {
    expect(javaStringHashCode(s)).toBe(hash);
    expect(javaPartition(s, 12)).toBe(partition);
  });
});

interface Address { city: string; zip: string }
interface Order { orderId: string; amount: number; items: string[]; shipping?: Address; internal?: string }

const AddressModel = defineModel<Address>("Address", { fields: { city: field.string("Delivery city"), zip: field.string() } });
const OrderModel = defineModel<Order>("Order", {
  description: "Order payload",
  fields: {
    orderId: field.string("Unique order identifier"),
    amount: field.double(),
    items: field.list(),
    shipping: field.model(AddressModel),
  },
});

describe("models", () => {
  it("flattens like the Java VocabularyManager", () => {
    expect(flatten({ orderId: "O-1", amount: 12.5, items: ["a"], shipping: { city: "Pune", zip: "411001" }, internal: "x" }, OrderModel))
      .toEqual({ orderId: "O-1", amount: 12.5, items: ["a"], city: "Pune", zip: "411001" });
    expect(flatten({ orderId: "O-2", amount: 1, items: [] }, OrderModel).shipping).toBeNull();
    expect(flatten({ a: 1 })).toEqual({ a: 1 });
    expect(flatten("x")).toEqual({ String: "x" });
    const renamed = defineModel<{ id: string }>("R", { fields: { id: field.string("", { name: "orderId" }) } });
    expect(flatten({ id: "1" }, renamed)).toEqual({ orderId: "1" });
  });

  it("builds class models", () => {
    const m = classModel(OrderModel) as any;
    expect(m.className).toBe("Order");
    expect(Object.fromEntries(m.attributes.map((a: any) => [a.name, a.type])))
      .toEqual({ orderId: "String", amount: "double", items: "List", shipping: "Address" });
  });
});

describe("credential policy", () => {
  it("rejects credential-like payloads", () => {
    for (const bad of [{ password: "x" }, { user: { api_key: "x" } }, { stripeSecret: "x" },
      { list: [{ Authorization: "y" }] }, { note: "-----BEGIN PRIVATE KEY-----abc" }, { json: '{"refresh_token":"x"}' }]) {
      expect(() => validateCredentialFree(bad)).toThrow(CredentialsInPayloadError);
    }
    expect(() => validateCredentialFree({ orderId: "1", message: "your password was changed", tokens: 3, json: "{oops" })).not.toThrow();
  });
});

describe("subjects", () => {
  it("serialize like the Java SDK", () => {
    const attrs = { firstName: "Alice" };
    expect(JSON.parse(JSON.stringify(new EmailSubject("a@x.com", { bcc: "b@x.com", correlationId: "c1", attributes: attrs }))))
      .toEqual({ channel: "EMAIL", correlationId: "c1", attributes: attrs, email: "a@x.com", cc: null, bcc: "b@x.com" });
    expect(new SmsSubject("+1555", { correlationId: "c2" }).toJSON())
      .toEqual({ channel: "SMS", correlationId: "c2", attributes: null, phoneNumber: "+1555" });
    expect(new InAppSubject(" /inbox ", { userId: "u1" }).toJSON().url).toBe("/inbox");
    expect(new SmsSubject("1").correlationId).toBeTruthy();
    expect(new EmailSubject("A@X.com").addressFingerprint).toBe("email:a@x.com");
  });
});

describe("util", () => {
  it("reads JWT claims and Jackson instants", () => {
    const tok = fakeJwt({ tenantId: "t-9", profile: "basic" });
    expect(tenantIdFromToken(tok)).toBe("t-9");
    expect(hasBasicProfile(`Bearer ${tok}`)).toBe(true);
    expect(tenantIdFromToken("garbage")).toBe("unknown-tenant");
    expect(parseInstant(1767225600.5)?.toISOString()).toBe("2026-01-01T00:00:00.500Z");
    expect(stringify({ n: 1n, m: new Map([["a", 1]]), s: new Set([1]) })).toBe('{"n":"1","m":{"a":1},"s":[1]}');
  });
});
