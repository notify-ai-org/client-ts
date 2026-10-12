/**
 * The SDK runtime: Bootstrapper, EventListener, InvokeManager, Dispatcher,
 * AcpServerClient and TokenHolder from the Java SDK in one client.
 */
import { classModel, flatten, type ModelDef } from "./models.js";
import { Subject } from "./subjects.js";
import { callerFrames, hasBasicProfile, isAsyncFunction, isPromiseLike, javaPartition, ruleRecord, sleep, stringify, tenantIdFromToken } from "./util.js";
import { AFTER, BEFORE, CallbackFn, Capture, RuleFn, SubjectSupplierFn, VocabularySupplierFn, When } from "./types.js";
import { BufRecord, EventMeta, EventSchedule, EventSpec, Hook, Metrics, RuleSpec } from "./interfaces.js";
import { consoleLogger, Logger } from "./logger.js";
import { NotifyConfig } from "./config.js";
import { EVENTS_TOPIC, KafkaFactory, KafkaRecord, KafkaTransport, SCHEDULED_EVENTS_TOPIC } from "./kafka.js";
import { AcpClient, RegistrationResponse, NotifyHTTPError } from "./acp.js";
import { validateCredentialFree, CredentialsInPayloadError } from "./policy.js";


export class NotifyClient {
  private readonly cfg: Required<Omit<NotifyConfig, "kafka" | "logger">>;
  private readonly log: Logger;
 
  private readonly acp: AcpClient;
  
  private kafka?: KafkaTransport;
  private partitions = 0;
  private readonly kafkaFactory?: KafkaFactory;

  private readonly events: EventMeta[] = [];
  private readonly models = new Map<string, ModelDef<any>>();
  private readonly rules: Hook[] = [];
  private readonly subjectSuppliers = new Map<string, Hook>();
  private readonly vocabularySuppliers = new Map<string, Hook>();
  private readonly callbacks: Record<When, Map<string, Hook[]>> = { BEFORE: new Map(), AFTER: new Map() };

  private buffer: BufRecord[] = [];
  private readonly pending = new Set<Promise<void>>();

  private readonly metricsData: Metrics = {
    ruleInvokeCount: 0, 
    vocabularySupplierInvokeCount: 0, 
    subjectSupplierInvokeCount: 0, 
    eventCaptureCount: 0,
    totalRuleInvokeMs: 0, 
    totalVocabularySupplierMs: 0, 
    totalSubjectSupplierMs: 0, 
    droppedCaptureCount: 0,
  };

  private tokens: { access?: string; refresh?: string; header?: string; expiresAt: number } = { expiresAt: 0 };
  private registration: RegistrationResponse = {};
  private clientId: string;
  private registered = false;
  private started = false;
  private closed = false;
  private readonly abort = new AbortController();
  private readonly readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private isReady = false;
  private dispatching: Promise<void> = Promise.resolve();
  private wake?: () => void;
  private loop?: Promise<void>;
  private consumer?: Promise<void>;
  private readonly onBeforeExit = () => void this.close().catch((e) => this.log.warn("flush at exit failed", e));

  constructor(config: NotifyConfig = {}) {
    this.cfg = {
      acpServerUrl: (config.acpServerUrl || "https://app.notify-ai.dev").replace(/\/+$/, ""),
      applicationName: config.applicationName || "notify-client",
      clientToken: config.clientToken ?? "",
      basePackage: config.basePackage ?? "",
      bufferBatchSize: config.bufferBatchSize && config.bufferBatchSize > 0 ? config.bufferBatchSize : 100,
      flushIntervalMs: config.flushIntervalMs && config.flushIntervalMs > 0 ? config.flushIntervalMs : 100,
      maxBufferSize: config.maxBufferSize || 10_000,
      kafkaEnabled: !!config.kafkaEnabled,
      flushOnExit: config.flushOnExit ?? true,
    };
    this.log = config.logger ?? consoleLogger;
    this.kafkaFactory = config.kafka;
    this.acp = new AcpClient(this.cfg.acpServerUrl);
    this.clientId = this.cfg.clientToken;
    this.readyPromise = new Promise((r) => (this.resolveReady = r));
  }

  // ── registration (the annotation equivalents) ──────────────────────────────

  /** @Model: register a model's vocabulary (done automatically for EventSpec.payload). */
  registerModel(model: ModelDef<any>): void {
    const id = `${model.packageName}.${model.className}`;
    if (this.models.has(id)) return;
    this.models.set(id, model);
    for (const f of Object.values(model.fields as Record<string, { model?: ModelDef<any> }>)) {
      if (f.model) this.registerModel(f.model);
    }
    if (this.registered) this.add({ type: "VOCABULARY", payload: [classModel(model)] });
  }

  /**
   * @Event: returns a function with fn's signature that captures every call.
   * The payload is the first argument. Sync functions stay sync.
   */
  event<P, R, A extends unknown[] = []>(spec: EventSpec<P>, fn: (payload: P, ...rest: A) => R): (payload: P, ...rest: A) => R {
    const meta = this.defineEvent(spec, fn.name || "anonymous", spec.serviceName ?? "default");
    const client = this;
    const async = isAsyncFunction(fn);
    return function (this: unknown, payload: P, ...rest: A): R {
      return client.invoke(meta, this, payload, () => fn.call(this, payload, ...rest), async);
    };
  }

  /**
   * @Event as a method decorator (TypeScript 5 standard decorators, or
   * experimentalDecorators). The payload is the method's first argument.
   */
  Event<P = any>(spec: EventSpec<P>): any {
    const client = this;
    return (...args: any[]): any => {
      const legacy = args.length === 3 && typeof args[2] === "object" && args[2] && "value" in args[2];
      const method: (...a: any[]) => any = legacy ? args[2].value : args[0];
      const name = legacy ? String(args[1]) : String(args[1]?.name ?? method.name);
      const className = legacy ? args[0]?.constructor?.name ?? args[0]?.name : undefined;
      const meta = client.defineEvent(spec, name, spec.serviceName ?? className ?? "default", !className);
      const async = isAsyncFunction(method);
      const wrapped = function (this: unknown, payload: P, ...rest: unknown[]) {
        return client.invoke(meta, this, payload, () => method.call(this, payload, ...rest), async);
      };
      if (legacy) {
        args[2].value = wrapped;
        return args[2];
      }
      return wrapped;
    };
  }

  /** An event with no business logic; returns emit(payload). */
  emitter<P>(spec: EventSpec<P>): (payload: P) => void {
    return this.event(spec, function emit(_payload: P) {
      return undefined;
    });
  }

  /** @Rule; the result (typically boolean) is reported with each capture. */
  rule<P = any>(spec: RuleSpec, fn: RuleFn<P>): void {
    if (!spec.name?.trim()) throw new Error("notify: rule name must not be blank");
    const hook: Hook = { name: spec.name, event: spec.event || "*", description: spec.description ?? "", fn };
    this.rules.push(hook);
    if (this.registered) this.add(ruleRecord(hook));
  }

  /** @SubjectSupplier: recipients for an event (one per event; later replaces earlier). */
  subjectSupplier<P = any>(event: string, fn: SubjectSupplierFn<P>, description = ""): void {
    this.requireEvent(event);
    this.subjectSuppliers.set(event, { name: fn.name, event, description, fn });
  }

  /** @VocabularySupplier: payload for scheduled triggers of an event. */
  vocabularySupplier(event: string, fn: VocabularySupplierFn, description = ""): void {
    this.requireEvent(event);
    this.vocabularySuppliers.set(event, { name: fn.name, event, description, fn });
  }

  /** @Callback; errors are logged and never affect the event. */
  callback<P = any>(event: string, when: When, fn: CallbackFn<P>): void {
    this.requireEvent(event);
    if (when !== BEFORE && when !== AFTER) throw new Error("notify: when must be BEFORE or AFTER");
    const list = this.callbacks[when].get(event) ?? [];
    list.push({ name: fn.name, event, description: "", fn });
    this.callbacks[when].set(event, list);
  }

  private requireEvent(event: string) {
    if (!event?.trim()) throw new Error("notify: event must not be blank");
  }

  private defineEvent(spec: EventSpec<any>, methodName: string, serviceName: string, resolveLater = false): EventMeta {
    if (!spec.key?.trim()) throw new Error("notify: EventSpec.key must not be blank");
    if (!(spec.priority >= 1 && spec.priority <= 5)) {
      throw new Error(`notify: event "${spec.key}" has priority=${spec.priority}; must be between 1 and 5`);
    }
    if (spec.payload) this.registerModel(spec.payload);
    const meta: EventMeta = {
      spec: { version: "v1", ...spec },
      methodName,
      declaringClassName: resolveLater ? "" : serviceName,
      serviceName,
    };
    this.events.push(meta);
    if (this.registered) this.add({ type: "EVENT_CAPTURE", payload: this.registerCapture(meta) });
    return meta;
  }

  // ── capture (EventListener#aroundEvent) ────────────────────────────────────

  /**
   * BEFORE callbacks, the function, AFTER callbacks, then the capture (subject
   * supplier and rules) is built in the background. Sync functions stay sync;
   * async BEFORE/AFTER callbacks are awaited only around async functions.
   */
  private invoke<R>(meta: EventMeta, self: unknown, payload: unknown, run: () => R, async: boolean): R {
    if (!meta.declaringClassName && self && typeof self === "object") {
      meta.declaringClassName = meta.serviceName = (self as object).constructor.name;
    }
    const start = Date.now();
    const frames = callerFrames();
    const key = meta.spec.key;
    const before = this.runCallbacks(BEFORE, key, payload);

    const finish = (result: unknown, error: unknown, failed: boolean) =>
      this.track(this.capture(meta, payload, result, error, failed, start, frames));

    const runAndCapture = (): R => {
      let result: R;
      try {
        result = run();
      } catch (e) {
        const after = this.runCallbacks(AFTER, key, payload);
        if (after) void after.then(() => finish(undefined, e, true));
        else finish(undefined, e, true);
        throw e;
      }
      if (isPromiseLike(result)) {
        return (async () => {
          try {
            const value = await result;
            await this.runCallbacks(AFTER, key, payload);
            finish(value, undefined, false);
            return value;
          } catch (e) {
            await this.runCallbacks(AFTER, key, payload);
            finish(undefined, e, true);
            throw e;
          }
        })() as R;
      }
      const after = this.runCallbacks(AFTER, key, payload);
      if (after) void after.then(() => finish(result, undefined, false));
      else finish(result, undefined, false);
      return result;
    };

    if (before && async) return before.then(runAndCapture) as R;
    return runAndCapture();
  }

  /** Runs callbacks; returns a promise only if one of them was async. */
  private runCallbacks(when: When, key: string, payload: unknown): Promise<void> | undefined {
    const hooks = this.callbacks[when].get(key) ?? [];
    const promises: Promise<void>[] = [];
    for (const cb of hooks) {
      try {
        const r = cb.fn(payload);
        if (isPromiseLike(r)) {
          promises.push(Promise.resolve(r).then(
            () => undefined,
            (e) => this.log.warn(`${when} callback for ${key} failed`, e),
          ));
        }
      } catch (e : any) {
        this.log.warn(`${when} callback for ${key} failed`, e);
      }
    }
    return promises.length ? Promise.all(promises).then(() => undefined) : undefined;
  }

  private track(p: Promise<void>) {
    this.pending.add(p);
    void p.finally(() => this.pending.delete(p));
  }

  private async capture(meta: EventMeta, payload: unknown, result: unknown, error: unknown, failed: boolean,
    start: number, frames: Capture[]): Promise<void> {
    try {
      const now = new Date().toISOString();
      const capture: Capture = {
        timestamp: now,
        occuredAt: now,
        payload: flatten(payload, meta.spec.payload),
        event: { name: meta.spec.key, eventType: "USER", description: meta.spec.description ?? "", priority: 0 },
        callStack: { frames },
        durationMillis: Date.now() - start,
        serviceName: meta.serviceName,
      };
      if (failed) {
        const err = error as { name?: string; constructor?: { name?: string }; message?: string; stack?: string };
        capture.exception = {
          exceptionType: err?.constructor?.name || err?.name || typeof error,
          message: err?.message ?? String(error),
          stackTrace: err?.stack ?? String(error),
        };
        capture.result = { success: false, returnValue: null };
      } else {
        capture.result = { success: true, returnValue: result === undefined || result === null ? null : stringify(result) };
      }
      capture.subjectResult = await this.subjectResult(meta.spec.key, payload);
      capture.ruleResults = await this.ruleResults(meta.spec.key, payload);
      this.enqueueCapture(capture);
    } catch (e : any) {
      this.log.error(`failed to build capture for ${meta.spec.key}`, e);
    }
  }

  private async subjectResult(key: string, payload: unknown): Promise<Capture> {
    const res: Capture = { eventKey: key, subjects: [], executionTimeMillis: 0, success: true, errorMessage: null };
    const hook = this.subjectSuppliers.get(key);
    if (!hook) return res;
    const start = Date.now();
    try {
      const out = await hook.fn(payload);
      const subjects = out == null ? [] : Array.isArray(out) ? out : [out];
      this.validateSubjects(key, subjects);
      res.subjects = subjects;
    } catch (e) {
      res.success = false;
      res.errorMessage = e instanceof Error ? e.message : String(e);
    }
    res.executionTimeMillis = Date.now() - start;
    this.metricsData.subjectSupplierInvokeCount++;
    this.metricsData.totalSubjectSupplierMs += res.executionTimeMillis;
    return res;
  }

  private async ruleResults(key: string, payload: unknown): Promise<Capture[]> {
    const rules = [...this.rules.filter((r) => r.event === key), ...this.rules.filter((r) => r.event === "*")];
    const out: Capture[] = [];
    for (const rule of rules) {
      const start = Date.now();
      const entry: Capture = { ruleName: rule.name, eventKey: key, result: null, success: true, errorMessage: null };
      try {
        const r = await rule.fn(payload);
        entry.result = r === undefined ? null : r;
      } catch (e) {
        entry.success = false;
        entry.errorMessage = e instanceof Error ? e.message : String(e);
      }
      entry.executionTimeMillis = Date.now() - start;
      this.metricsData.ruleInvokeCount++;
      this.metricsData.totalRuleInvokeMs += entry.executionTimeMillis;
      out.push(entry);
    }
    return out;
  }

  private enqueueCapture(capture: Capture) {
    this.add({ type: "EVENT_CAPTURE", payload: capture });
    this.metricsData.eventCaptureCount++;
  }

  /** EventListener#onScheduledEvent. */
  private async onScheduled(schedule: EventSchedule): Promise<void> {
    const now = new Date().toISOString();
    const capture: Capture = {
      timestamp: now, occuredAt: now, payload: null, durationMillis: 0,
      event: { name: schedule.eventName, eventType: "SCHEDULED", priority: 0 },
    };
    const hook = this.vocabularySuppliers.get(schedule.eventName ?? "");
    if (hook) {
      const start = Date.now();
      try {
        const value = await hook.fn(schedule);
        if (value !== null && value !== undefined) capture.payload = flatten(value);
      } catch (e : any) {
        this.log.warn(`vocabulary supplier for ${schedule.eventName} failed`, e);
      }
      this.metricsData.vocabularySupplierInvokeCount++;
      this.metricsData.totalVocabularySupplierMs += Date.now() - start;
    }
    this.enqueueCapture(capture);
  }

    public async initKafka(onScheduled : (schedule: EventSchedule) => Promise<void>): Promise<void> {
      if (!this.kafkaFactory) {
        this.log.warn("kafkaEnabled is set but no kafka factory was supplied; using HTTP");
        return;
      }
      let transport: KafkaTransport;
      try {
        transport = await this.kafkaFactory({
          clientId: this.cfg.clientToken,
          apiKey: this.registration.apiKey ?? "",
          apiSecret: this.registration.apiSecret ?? "",
        });
        this.partitions = await transport.partitions(EVENTS_TOPIC);
      } catch (e : any) {
        this.log.error("Kafka transport unavailable; using HTTP", e);
        return;
      }
      if (this.partitions <= 0) {
        this.log.error(`${EVENTS_TOPIC} has no partitions; using HTTP`);
        await transport.close();
        return;
      }
      this.kafka = transport;
      this.consumer = transport
        .consume(SCHEDULED_EVENTS_TOPIC, async (value) => {
          let schedule: EventSchedule;
          try {
            schedule = JSON.parse(value.toString("utf8"));
          } catch (e : any) {
            this.log.warn("invalid scheduled event", e);
            return;
          }
          await onScheduled(schedule);
        }, this.abort.signal)
        .catch((e) => {
          if (!this.abort.signal.aborted) this.log.error("scheduled-event consumer stopped", e);
        });
      this.log.info(`Kafka transport ready (${this.partitions} partitions)`);
    }

  private add(record: BufRecord) {
    const max = this.cfg.maxBufferSize;
    if (max > 0 && this.buffer.length >= max) {
      const i = this.buffer.findIndex((r) => r.type === "EVENT_CAPTURE");
      if (i >= 0) {
        this.buffer.splice(i, 1);
        this.metricsData.droppedCaptureCount++;
      }
    }
    this.buffer.push(record);
    this.wake?.();
  }

  // ── lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Registers with acp-server and starts dispatching in the background. Does
   * not block; registration is retried with backoff until it succeeds.
   */
  start(): this {
    if (this.started) throw new Error("notify: client already started");
    if (this.closed) throw new Error("notify: client is closed");
    this.started = true;
    if (this.cfg.flushOnExit) process.once("beforeExit", this.onBeforeExit);
    this.loop = this.run(this.onScheduled).catch((e) => this.log.error("dispatcher stopped", e));
    return this;
  }

  /** Resolves when registration completes; rejects after timeoutMs. */
  

  metrics(): Metrics {
    return { ...this.metricsData };
  }


  /** Stops background work, flushes within timeoutMs, closes Kafka. */
  async close(timeoutMs = 10_000): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    process.removeListener("beforeExit", this.onBeforeExit);
    this.abort.abort  ();
    this.wake?.();
    await this.loop;
    await this.consumer;
    try {
      if (this.isReady) await this.flush(timeoutMs);
      else if (this.buffer.length) this.log.warn(`closed before registration; ${this.buffer.length} record(s) discarded`);
    } finally {
      await this.kafka?.close();
    }
  }
  
  registerCapture(meta: EventMeta): Capture {
    const s = meta.spec;
    const event = {
      name: s.key, description: s.description ?? "", priority: s.priority, scheduleIntent: s.scheduleIntent ?? "",
      preferredTimeWindow: s.preferredTimeWindow ?? "", eventType: "REGISTER",
    };
    const now = new Date().toISOString();
    return {
      timestamp: now, occuredAt: now, event, durationMillis: 0, serviceName: meta.serviceName,
      payload: {
        EventMetadata: {
          event, version: s.version, methodName: meta.methodName,
          declaringClassName: meta.declaringClassName || meta.serviceName,
          returnTypeName: "Object", parameterTypeNames: [s.payload?.className ?? "Object"],
        },
      },
    };
  }

  validateSubjects(key: string, subjects: unknown[]) {
    subjects.forEach((s, i) => {
      if (!(s instanceof Subject)) {
        throw new Error(`SubjectSupplier for event '${key}' returned non-Subject item at index ${i}`);
      }
      if (!s.address || !String(s.address).trim()) {
        throw new Error(`SubjectSupplier for event '${key}' returned ${s.channel} subject at index ${i} without a destination address`);
      }
    });
  }

  async run(onScheduled: (schedule: EventSchedule) => Promise<void>): Promise<void> {
    if (!(await this.registerWithRetry())) return;
    if (this.cfg.kafkaEnabled) await this.initKafka(onScheduled);
    else this.log.info("Kafka disabled; event captures use HTTP transport");
    this.enqueueRegistrations();
    this.isReady = true;
    this.resolveReady();
    await this.dispatchLoop();
  }

  private async registerWithRetry(): Promise<boolean> {
    let backoff = 1000;
    while (!this.abort.signal.aborted) {
      try {
        const resp = await this.acp.register({
          clientId: this.cfg.clientToken,
          applicationName: this.cfg.applicationName,
          basePackage: this.cfg.basePackage,
        });
        this.registration = resp;
        this.clientId = resp.clientId || this.cfg.clientToken;
        if (resp.token) {
          this.setTokens(resp.token, resp.expiresInMs, resp.refreshToken, resp.kafkaHeaderToken);
          this.log.info(`registered with acp-server at ${this.cfg.acpServerUrl}`);
        } else {
          this.log.warn("registration response did not contain tokens");
        }
        return true;
      } catch (e : any) {
        this.log.warn(`registration failed; retrying in ${backoff / 1000}s`, e instanceof Error ? e.message : e);
        if (!(await sleep(backoff, this.abort.signal))) return false;
        backoff = Math.min(backoff * 2, 60_000);
      }
    }
    return false;
  }

  waitReady(timeoutMs = 30_000): Promise<void> {
    return Promise.race([
      this.readyPromise,
      new Promise<void>((_, reject) => {
        const t = setTimeout(() => reject(new Error("notify: registration timed out")), timeoutMs);
        t.unref?.();
        void this.readyPromise.then(() => clearTimeout(t));
      }),
    ]);
  }

  private setTokens(access?: string, expiresInMs?: number, refresh?: string, header?: string) {
    this.tokens.access = access;
    if (refresh) this.tokens.refresh = refresh;
    if (header !== undefined || refresh !== undefined) this.tokens.header = header;
    this.tokens.expiresAt = expiresInMs && expiresInMs > 0 ? Date.now() + expiresInMs : Number.POSITIVE_INFINITY;
  }

  
  /** REGISTER captures, vocabulary, then rules — ahead of buffered captures. */
  private enqueueRegistrations() {
    const records: BufRecord[] = this.events.map((e) => ({ type: "EVENT_CAPTURE", payload: this.registerCapture(e) }));
    if (this.models.size) records.push({ type: "VOCABULARY", payload: [...this.models.values()].map(classModel) });
    records.push(...this.rules.map(ruleRecord));
    this.buffer.unshift(...records);
    this.registered = true;
  }

  private async dispatchLoop() {
    let backoff = 1000;
    while (!this.abort.signal.aborted) {
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, this.cfg.flushIntervalMs);
        t.unref?.();
        this.wake = () => {
          clearTimeout(t);
          resolve();
        };
      });
      this.wake = undefined;
      while (this.buffer.length && !this.abort.signal.aborted) {
        try {
          await this.dispatchOnce();
          backoff = 1000;
        } catch (e : any) {
          this.log.error(`dispatch failed; retrying in ${backoff / 1000}s`, e instanceof Error ? e.message : e);
          if (!(await sleep(backoff, this.abort.signal))) return;
          backoff = Math.min(backoff * 2, 30_000);
        }
      }
    }
  }

  /** One dispatch at a time, so flush() waits for in-flight sends. */
  private dispatchOnce(): Promise<void> {
    const next = this.dispatching.catch(() => undefined).then(() => this.dispatchBatch());
    this.dispatching = next;
    return next;
  }

  /**
   * Dispatcher#run iteration: vocabulary/rules over HTTP; captures over Kafka
   * when available (unless the token has the basic profile), else HTTP.
   * Unsent records are requeued at the front.
   */
  private async dispatchBatch(): Promise<void> {
    const records = this.buffer.splice(0, this.cfg.bufferBatchSize);
    const captures: BufRecord[] = [];
    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      try {
        if (rec.type === "VOCABULARY") await this.withAuth((t) => this.acp.postVocabulary(rec.payload, t));
        else if (rec.type === "RULE") await this.withAuth((t) => this.acp.postRule(rec.payload, t));
        else captures.push(rec);
      } catch (e) {
        this.buffer.unshift(...captures, ...records.slice(i));
        throw e;
      }
    }
    if (!captures.length) return;
    try {
      const payloads = captures.map((r) => r.payload as Capture);
      if (!this.kafka || hasBasicProfile(this.tokens.access)) {
        await this.withAuth((t) => this.acp.postEventCaptures(payloads, t));
      } else {
        await this.produce(payloads);
      }
    } catch (e) {
      this.buffer.unshift(...captures);
      throw e;
    }
  }

  /**
   * Kafka path: one record per subject keyed tenant:event:subjectId on the
   * subject's Java-hash partition, or one tenant:event record when there are no
   * subjects. The Authorization header carries the Kafka header token.
   */
  private async produce(captures: Capture[]) {
    const token = this.tokens.header || this.tokens.access || "";
    const tenant = tenantIdFromToken(token);
    const headers: Record<string, string> = token
      ? { Authorization: token.startsWith("Bearer ") ? token : `Bearer ${token}` }
      : {};
    const records: KafkaRecord[] = [];
    for (const capture of captures) {
      const name = capture.event.name;
      try {
        validateCredentialFree(capture.payload);
      } catch (e) {
        if (e instanceof CredentialsInPayloadError) {
          this.log.error(`dropping ${name} capture with credential-like payload fields`);
          continue;
        }
        throw e;
      }
      const value = stringify(capture);
      const subjects: Subject[] = capture.subjectResult?.subjects ?? [];
      if (!subjects.length) {
        records.push({ topic: EVENTS_TOPIC, partition: javaPartition(tenant, this.partitions), key: `${tenant}:${name}`, value, headers });
      }
      for (const s of subjects) {
        records.push({
          topic: EVENTS_TOPIC, partition: javaPartition(s.subjectId, this.partitions),
          key: `${tenant}:${name}:${s.subjectId}`, value, headers,
        });
      }
    }
    if (records.length) await this.kafka!.produce(records);
  }

  /** Refresh first if expired, and once more on HTTP 401. */
  private async withAuth(op: (token?: string) => Promise<void>) {
    if (this.tokens.access && Date.now() >= this.tokens.expiresAt) await this.refresh();
    try {
      await op(this.tokens.access);
    } catch (e) {
      if (!(e instanceof NotifyHTTPError) || e.status !== 401) throw e;
      await this.refresh();
      await op(this.tokens.access);
    }
  }

  private async refresh() {
    if (!this.tokens.refresh) return;
    try {
      const resp = await this.acp.refreshToken({ clientId: this.clientId, refreshToken: this.tokens.refresh });
      this.setTokens(resp.token, resp.expiresInMs);
      this.log.info("access token refreshed");
    } catch (e : any) {
      this.log.error("token refresh failed", e);
    }
  }

  /** Sends everything buffered, including captures still being built. */
  async flush(timeoutMs = 10_000): Promise<void> {
    if (!this.isReady) throw new Error("notify: not registered with acp-server yet");
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (Date.now() > deadline) throw new Error(`notify: ${this.buffer.length} record(s) not flushed`);
      await Promise.all([...this.pending]);
      await this.dispatchOnce();
      if (this.buffer.length === 0 && this.pending.size === 0) return;
    }
  }
    
}



/** REGISTER capture; payload matches the Java SDK's flattened EventMetadata. */



