import { EventSchedule } from "./interfaces.js";
import { Subject } from "./subjects.js";

export type RecordType = "VOCABULARY" | "RULE" | "EVENT_CAPTURE";

export type Capture = Record<string, any>;

type MaybePromise<T> = T | Promise<T>;
export type SubjectSupplierFn<P> = (payload: P) => MaybePromise<Subject[] | Subject | null | undefined>;
export type RuleFn<P> = (payload: P) => unknown;
export type CallbackFn<P> = (payload: P) => unknown;
/** Receives the scheduled trigger from acp-server; returns the capture payload. */
export type VocabularySupplierFn = (schedule: EventSchedule) => unknown;

export type When = "BEFORE" | "AFTER";
export const BEFORE: When = "BEFORE";
export const AFTER: When = "AFTER";
