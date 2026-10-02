import { newUUID } from "./util.js";

export enum Channel {
  SMS = "SMS",
  EMAIL = "EMAIL",
  WHATSAPP = "WHATSAPP",
  PUSH = "PUSH",
  NONE = "NONE",
  WEBHOOK = "WEBHOOK",
  IN_APP = "IN_APP",
}

export type Attributes = Record<string, string>;

/**
 * Notification recipient. JSON matches the Java SDK's Subject hierarchy
 * (discriminated by "channel"). subjectId is the destination address.
 */
export abstract class Subject {
  abstract readonly channel: Channel;
  readonly correlationId: string;
  readonly attributes: Attributes | null;

  protected constructor(correlationId?: string | null, attributes?: Attributes | null) {
    this.correlationId = correlationId || newUUID();
    this.attributes = attributes ?? null;
  }

  abstract get address(): string;
  get subjectId(): string {
    return this.address;
  }
  get addressFingerprint(): string {
    return `${this.channel.toLowerCase()}:${this.address}`;
  }
  protected abstract fields(): Record<string, unknown>;

  toJSON(): Record<string, unknown> {
    return { channel: this.channel, correlationId: this.correlationId, attributes: this.attributes, ...this.fields() };
  }
}

export interface SubjectOptions {
  correlationId?: string;
  attributes?: Attributes;
}

export class EmailSubject extends Subject {
  readonly channel = Channel.EMAIL;
  constructor(
    readonly email: string,
    opts: SubjectOptions & { cc?: string; bcc?: string } = {},
  ) {
    super(opts.correlationId, opts.attributes);
    this.cc = opts.cc ?? null;
    this.bcc = opts.bcc ?? null;
  }
  readonly cc: string | null;
  readonly bcc: string | null;
  get address() {
    return this.email;
  }
  override get addressFingerprint() {
    return "email:" + (this.email ?? "").toLowerCase();
  }
  protected fields() {
    return { email: this.email, cc: this.cc, bcc: this.bcc };
  }
}

export class SmsSubject extends Subject {
  readonly channel = Channel.SMS;
  constructor(readonly phoneNumber: string, opts: SubjectOptions = {}) {
    super(opts.correlationId, opts.attributes);
  }
  get address() {
    return this.phoneNumber;
  }
  protected fields() {
    return { phoneNumber: this.phoneNumber };
  }
}

export class WhatsAppSubject extends Subject {
  readonly channel = Channel.WHATSAPP;
  readonly phoneNumber: string;
  constructor(phoneNumber: string, opts: SubjectOptions = {}) {
    super(opts.correlationId, opts.attributes);
    this.phoneNumber = (phoneNumber ?? "").trim();
  }
  get address() {
    return this.phoneNumber;
  }
  protected fields() {
    return { phoneNumber: this.phoneNumber };
  }
}

export class PushSubject extends Subject {
  readonly channel = Channel.PUSH;
  constructor(readonly deviceToken: string, opts: SubjectOptions = {}) {
    super(opts.correlationId, opts.attributes);
  }
  get address() {
    return this.deviceToken;
  }
  protected fields() {
    return { deviceToken: this.deviceToken };
  }
}

export class WebhookSubject extends Subject {
  readonly channel = Channel.WEBHOOK;
  constructor(readonly url: string, opts: SubjectOptions = {}) {
    super(opts.correlationId, opts.attributes);
  }
  get address() {
    return this.url;
  }
  protected fields() {
    return { url: this.url };
  }
}

export class InAppSubject extends Subject {
  readonly channel = Channel.IN_APP;
  readonly url: string;
  readonly userId: string | null;
  constructor(url: string, opts: SubjectOptions & { userId?: string } = {}) {
    super(opts.correlationId, opts.attributes);
    this.url = (url ?? "").trim();
    this.userId = opts.userId ?? null;
  }
  get address() {
    return this.url;
  }
  override get addressFingerprint() {
    return `in_app:${this.url}:${this.userId ?? ""}`;
  }
  protected fields() {
    return { url: this.url, userId: this.userId };
  }
}
