/**
 * TypeScript port of examples/ecommerce-app's order flow, exercising every SDK
 * hook type against a running acp-server.
 *
 *   NOTIFY_AI_ACP_SERVER_URL=http://localhost:8080 NOTIFY_AI_CLIENT_TOKEN=client-... \
 *     npx tsx examples/ecommerce.ts
 *
 *   curl -X POST localhost:8093/api/orders/place \
 *     -d '{"orderId":"ORD-1","customerId":"CUST-1","amount":120,"items":["Mechanical Keyboard"]}'
 */
import { createServer } from "node:http";
import {
  AFTER, BEFORE, configFromEnv, defineModel, EmailSubject, field, NotifyClient, SmsSubject, type Subject,
} from "../src/index.js";

// ── Models (@Model / @Vocabulary) ─────────────────────────────────────────────

interface OrderPayload { orderId: string; customerId: string; amount: number; items?: string[]; shippingAddress?: string }
interface ShipmentPayload { orderId: string; trackingNumber: string; carrier: string; estimatedDelivery?: string }
interface CartPayload { cartId: string; customerId: string; items?: string[]; lastActivityAt?: string }

const OrderModel = defineModel<OrderPayload>("OrderPayload", {
  description: "Payload for order placement and payment events",
  fields: {
    orderId: field.string("Unique order identifier"),
    customerId: field.string("Customer who placed the order"),
    amount: field.double("Total order amount in USD"),
    items: field.list("List of item names in the order"),
    shippingAddress: field.string("Delivery address for the order"),
  },
});
const ShipmentModel = defineModel<ShipmentPayload>("ShipmentPayload", {
  description: "Payload for shipment tracking events",
  fields: {
    orderId: field.string("Order being shipped"),
    trackingNumber: field.string("Carrier tracking number"),
    carrier: field.string("Shipping carrier name"),
    estimatedDelivery: field.string("Estimated delivery date (YYYY-MM-DD)"),
  },
});
const CartModel = defineModel<CartPayload>("CartPayload", {
  description: "Payload for abandoned cart events",
  fields: {
    cartId: field.string("Unique cart identifier"),
    customerId: field.string("Customer who owns the cart"),
    items: field.list("Items left in the cart"),
    lastActivityAt: field.string("Timestamp of last cart activity (ISO-8601)"),
  },
});

const customers = new Map([
  ["CUST-1", { name: "Alice Johnson", email: "alice@example.com", phone: "+1-555-0101" }],
  ["CUST-2", { name: "Bob Smith", email: "bob@example.com", phone: "+1-555-0102" }],
  ["CUST-3", { name: "Carol Davis", email: "carol@example.com", phone: "+1-555-0103" }],
]);
const orders = new Map<string, OrderPayload>();

const env = configFromEnv();
const client = new NotifyClient({
  ...env,
  acpServerUrl: env.acpServerUrl ?? "http://localhost:8080",
  applicationName: env.applicationName ?? "ecommerce-app-ts",
  basePackage: "examples/ecommerce",
});

// ── Service with events (@Event) ──────────────────────────────────────────────

class OrderService {
  @client.Event<OrderPayload>({ key: "ORDER_PLACED", description: "Customer placed an order", eventType: "static",
    scheduleIntent: "immediate", preferredTimeWindow: "09:00-18:00", priority: 2, payload: OrderModel })
  placeOrder(p: OrderPayload) {
    orders.set(p.orderId, p);
    console.log(`📦 Order placed: ${p.orderId} for customer ${p.customerId} — $${p.amount}`);
    return p;
  }

  @client.Event<OrderPayload>({ key: "PAYMENT_FAILED", description: "Payment processing failed for an order",
    eventType: "static", scheduleIntent: "immediate", preferredTimeWindow: "00:00-23:59", priority: 4, payload: OrderModel })
  reportPaymentFailed(p: OrderPayload) {
    console.log(`💳 Payment FAILED for order: ${p.orderId} — $${p.amount}`);
    return p;
  }

  @client.Event<ShipmentPayload>({ key: "ORDER_SHIPPED", description: "Order has been shipped to the customer",
    eventType: "static", scheduleIntent: "immediate", preferredTimeWindow: "09:00-18:00", priority: 5, payload: ShipmentModel })
  shipOrder(p: ShipmentPayload) {
    console.log(`Order shipped: ${p.orderId} via ${p.carrier} — tracking: ${p.trackingNumber}`);
    return p;
  }

  @client.Event<CartPayload>({ key: "ABANDONED_CART", description: "Customer abandoned their shopping cart",
    eventType: "deferred", scheduleIntent: "deferred", preferredTimeWindow: "09:00-21:00", priority: 3, payload: CartModel })
  abandonCart(p: CartPayload) {
    console.log(`🛒 Cart abandoned: ${p.cartId} by customer ${p.customerId}`);
    return p;
  }
}

// ── Subject suppliers, rules, callbacks ───────────────────────────────────────

const emailTo = (customerId: string): Subject[] => {
  const c = customers.get(customerId);
  return c ? [new EmailSubject(c.email, { attributes: { firstName: c.name } })] : [];
};

client.subjectSupplier<OrderPayload>("ORDER_PLACED", (p) => emailTo(p.customerId), "Resolves order customer to email recipients");
client.subjectSupplier<OrderPayload>("PAYMENT_FAILED", (p) => {
  const c = customers.get(p.customerId);
  return c ? [new SmsSubject(c.phone, { attributes: { firstName: c.name } })] : [];
}, "Resolves customer to SMS for urgent payment alerts");
client.subjectSupplier<ShipmentPayload>("ORDER_SHIPPED", (p) => {
  const order = orders.get(p.orderId);
  return order ? emailTo(order.customerId) : [];
}, "Resolves order to email recipients for shipment tracking");
client.subjectSupplier<CartPayload>("ABANDONED_CART", (p) => emailTo(p.customerId), "Resolves cart owner to email for re-engagement");

client.rule<OrderPayload>({ name: "fraud-check", event: "ORDER_PLACED", description: "Blocks orders over $1000 as potential fraud" },
  (p) => p.amount < 1000);
client.rule<OrderPayload>({ name: "inventory-check", event: "ORDER_PLACED", description: "Checks whether all items are in stock" },
  () => true);

client.callback<OrderPayload>("ORDER_PLACED", BEFORE, (p) => console.log(`⏳ [BEFORE] About to process ORDER_PLACED for order: ${p.orderId}`));
client.callback<OrderPayload>("ORDER_PLACED", AFTER, (p) => console.log(`✅ [AFTER] ORDER_PLACED processing complete for order: ${p.orderId}`));

// ── HTTP endpoints ────────────────────────────────────────────────────────────

const svc = new OrderService();
const routes: Record<string, (b: any) => unknown> = {
  "/api/orders/place": (b) => (svc.placeOrder(b), { status: "ORDER_PLACED", orderId: b.orderId, amount: b.amount }),
  "/api/orders/payment-failed": (b) => (svc.reportPaymentFailed(b), { status: "PAYMENT_FAILED", orderId: b.orderId }),
  "/api/orders/ship": (b) => (svc.shipOrder(b), { status: "ORDER_SHIPPED", orderId: b.orderId, trackingNumber: b.trackingNumber }),
  "/api/orders/abandon-cart": (b) => (svc.abandonCart(b), { status: "ABANDONED_CART", cartId: b.cartId }),
};

client.start();
const server = createServer((req, res) => {
  const route = req.method === "POST" ? routes[req.url ?? ""] : undefined;
  if (!route) return void res.writeHead(404).end();
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    try {
      const out = route(JSON.parse(body || "{}"));
      res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify(out));
    } catch (e) {
      res.writeHead(400, { "Content-Type": "application/json" }).end(JSON.stringify({ error: String(e) }));
    }
  });
});
const port = Number(process.env.PORT ?? 8093);
server.listen(port, "127.0.0.1", () => console.log(`ecommerce example listening on :${port}`));

process.once("SIGINT", async () => {
  server.close();
  await client.close();
  process.exit(0);
});
