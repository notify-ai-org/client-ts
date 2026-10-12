import { ModelDef } from "./models.js";
import { RecordType } from "./types.js";

/** @Event attributes. */
export interface EventSpec<P = unknown> {
  key: string;
  priority: number;
  description?: string;
  preferredTimeWindow?: string;
  eventType?: string;
  scheduleIntent?: string;
  version?: string;
  /** Vocabulary of the payload; without it object payloads flatten by their own keys. */
  payload?: ModelDef<P>;
  /** Service name reported with captures. Defaults to the class (decorator) or "default". */
  serviceName?: string;
}

export interface RuleSpec {
  name: string;
  /** Empty applies the rule to every event ("*"). */
  event?: string;
  description?: string;
}


export interface EventSchedule {
  id?: string;
  eventName?: string;
  description?: string;
  triggerType?: string;
  scheduledAt?: string | number;
  cronExpression?: string;
}

export interface Metrics {
  ruleInvokeCount: number;
  vocabularySupplierInvokeCount: number;
  subjectSupplierInvokeCount: number;
  eventCaptureCount: number;
  totalRuleInvokeMs: number;
  totalVocabularySupplierMs: number;
  totalSubjectSupplierMs: number;
  droppedCaptureCount: number;
}

export interface EventMeta {
  spec: EventSpec<any>;
  methodName: string;
  declaringClassName: string;
  serviceName: string;
}

export interface Hook {
  name: string;
  event: string;
  description: string;
  fn: (payload: any) => unknown;
}

export interface BufRecord {
  type: RecordType;
  payload: any;
}
