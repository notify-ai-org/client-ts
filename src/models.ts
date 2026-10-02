/**
 * Models and vocabulary — the TypeScript counterpart of @Model / @Vocabulary.
 * TypeScript types are erased at runtime, so the vocabulary is declared:
 *
 *   const OrderModel = defineModel<OrderPayload>("OrderPayload", {
 *     description: "Payload for order placement events",
 *     fields: {
 *       orderId: field.string("Unique order identifier"),
 *       amount: field.double("Total order amount in USD"),
 *     },
 *   });
 *
 * Field keys are checked against the payload type. A field's vocabulary name
 * defaults to its key; override with { name }.
 */

export interface FieldDef {
  readonly type: string;
  readonly description: string;
  readonly name?: string;
  readonly model?: ModelDef<any>;
}

export interface ModelDef<T = unknown> {
  readonly className: string;
  readonly packageName: string;
  readonly description: string;
  readonly fields: Readonly<Partial<Record<keyof T & string, FieldDef>>>;
  /** Phantom marker for type inference; never set. */
  readonly __type?: T;
}

export interface ModelOptions<T> {
  description?: string;
  /** Reported as the Java "packageName"; defaults to "". */
  packageName?: string;
  fields: Partial<Record<keyof T & string, FieldDef>>;
}

export function defineModel<T>(className: string, options: ModelOptions<T>): ModelDef<T> {
  if (!className?.trim()) throw new Error("notify: model className must not be blank");
  return Object.freeze({
    className,
    packageName: options.packageName ?? "",
    description: options.description ?? "",
    fields: Object.freeze({ ...options.fields }),
  });
}

type Opts = { name?: string };
const make =
  (type: string) =>
  (description = "", opts: Opts = {}): FieldDef => ({ type, description, ...opts });

/** Field builders; type names are the Java simple names acp-server sees from every SDK. */
export const field = {
  string: make("String"),
  int: make("int"),
  long: make("long"),
  double: make("double"),
  boolean: make("boolean"),
  instant: make("Instant"),
  date: make("LocalDate"),
  list: make("List"),
  map: make("Map"),
  object: make("Object"),
  of: (type: string, description = "", opts: Opts = {}): FieldDef => ({ type, description, ...opts }),
  model: (model: ModelDef<any>, description = "", opts: Opts = {}): FieldDef => ({
    type: model.className,
    description,
    model,
    ...opts,
  }),
};

/** ClassModel DTO (VocabularyManager#toClassModelDtoList). */
export function classModel(m: ModelDef<any>): Record<string, unknown> {
  return {
    packageName: m.packageName,
    className: m.className,
    classDescription: m.description,
    classType: null,
    superClass: null,
    interfaces: [],
    attributes: Object.entries(m.fields as Record<string, FieldDef>).map(([key, f]) => ({
      name: f.name || key,
      type: f.type,
      description: f.description,
      defaultValue: null,
    })),
    methods: [],
  };
}

const javaName = (v: unknown): string => {
  switch (typeof v) {
    case "string":
      return "String";
    case "number":
      return Number.isInteger(v) ? "long" : "double";
    case "boolean":
      return "boolean";
    case "bigint":
      return "long";
    default:
      return (v as object)?.constructor?.name || "Object";
  }
};

/**
 * VocabularyManager#toFlattenedMap: with a model, {vocabName: value} and nested
 * models expanded into their own leaves; without one, an object is flattened
 * by its own keys and a primitive becomes {TypeName: value}.
 */
export function flatten(instance: unknown, model?: ModelDef<any>): Record<string, unknown> {
  if (instance === null || instance === undefined) return {};
  if (typeof instance !== "object") return { [javaName(instance)]: instance };
  if (!model) {
    if (Array.isArray(instance)) return { List: instance };
    if (instance instanceof Date) return { Instant: instance };
    return { ...(instance as Record<string, unknown>) };
  }
  const out: Record<string, unknown> = {};
  expand(instance as Record<string, unknown>, model, out, new Set());
  return out;
}

function expand(obj: Record<string, unknown>, model: ModelDef<any>, out: Record<string, unknown>, seen: Set<object>) {
  seen.add(obj);
  for (const [key, f] of Object.entries(model.fields as Record<string, FieldDef>)) {
    const value = obj[key] ?? null;
    if (f.model && value && typeof value === "object" && !seen.has(value)) {
      expand(value as Record<string, unknown>, f.model, out, seen);
    } else {
      out[f.name || key] = value;
    }
  }
}
