import {
  DataType,
  RecordBatch,
  Table,
  tableFromIPC,
  tableToIPC,
  type Schema,
} from 'apache-arrow';

import type * as Glue from './wasm/datafusion_vtab.js';
import { drive, engine, type Engine } from './engine.js';

export {
  EngineCrashedError,
  heapStats,
  init,
  type HeapStats,
  type InitOptions,
} from './engine.js';

// ── Filters ──────────────────────────────────────────────────────────

/**
 * A literal in a pushed-down filter. Integers are numbers when they fit in
 * 2^53 and bigints otherwise. Dates and timestamps are `Date`s, so timestamps
 * finer than milliseconds lose precision.
 */
export type Literal = null | boolean | number | bigint | string | Date;

export type ComparisonOp = '=' | '!=' | '<' | '<=' | '>' | '>=';

/** A filter DataFusion offers to a virtual table, as a small expression tree. */
export type Filter =
  | { readonly op: ComparisonOp; readonly column: string; readonly value: Literal }
  | { readonly op: 'in' | 'notIn'; readonly column: string; readonly values: readonly Literal[] }
  | { readonly op: 'isNull' | 'isNotNull'; readonly column: string }
  | { readonly op: 'and' | 'or'; readonly args: readonly Filter[] }
  | { readonly op: 'not'; readonly arg: Filter };

/**
 * How a table handles a filter:
 * - `exact`: every row `scan` yields matches the filter. DataFusion does not
 *   check it again.
 * - `inexact`: `scan` may use the filter to skip rows, and DataFusion checks
 *   it again on what `scan` yields.
 * - `unsupported`: the filter is not passed to `scan`.
 */
export type FilterSupport = 'exact' | 'inexact' | 'unsupported';

// ── Virtual tables ───────────────────────────────────────────────────

export interface ScanRequest {
  /** Columns the query needs, in schema order. May be empty (for `COUNT(*)`). */
  readonly projection: readonly string[];
  /** Filters this table accepted as `exact` or `inexact`. Rows must match all of them. */
  readonly filters: readonly Filter[];
  /**
   * Maximum number of rows the query needs, or null. Only set when every
   * filter in the query was pushed down as `exact`.
   */
  readonly limit: number | null;
}

/** Batches a scan yields. Each must contain at least the projected columns. */
export type ScanBatch = RecordBatch | Table;

export interface VirtualTable {
  /** The table's Arrow schema. Called once, when the table is registered. */
  schema(): Schema;
  /**
   * Decides, for each filter, whether `scan` handles it. Only called for
   * tables registered with `pushdown: true`. Default: `inexact` for all.
   */
  supportsFilters?(
    filters: readonly Filter[],
  ): readonly FilterSupport[] | Promise<readonly FilterSupport[]>;
  /**
   * Yields the table's rows. Called each time a query reads the table.
   * DataFusion pulls batches as it needs them and calls `return()` on the
   * iterator if it stops early, so `finally` blocks run.
   */
  scan(request: ScanRequest): AsyncIterable<ScanBatch> | Iterable<ScanBatch>;
}

export interface RegisterTableOptions {
  /**
   * Offer filters to `supportsFilters` and pass accepted filters and limits
   * to `scan`. Projection is always passed. Default: false.
   */
  readonly pushdown?: boolean;
}

// ── Queries ──────────────────────────────────────────────────────────

export interface SessionContextOptions {
  /**
   * Bytes DataFusion may reserve for query state (hash tables, sorts,
   * aggregations) in this session. Queries that need more fail with a
   * "Resources exhausted" error. Default: 256 MiB.
   */
  readonly memoryLimit?: number;
  /** Target rows per batch inside DataFusion. Default: 8192. */
  readonly batchSize?: number;
}

export interface SqlOptions {
  /** Return at most this many rows and set `truncated` if there were more. */
  readonly maxRows?: number;
  /**
   * Reject DDL (`CREATE`, `DROP`), DML (`INSERT`) and statements (`SET`).
   * Default: true, so queries cannot change the session or its limits.
   */
  readonly readonly?: boolean;
}

export interface Column {
  readonly name: string;
  /** Arrow type, for example `Int64` or `Utf8`. */
  readonly type: string;
  readonly nullable: boolean;
}

export interface QueryResult {
  readonly columns: readonly Column[];
  /**
   * One array per row, in column order. Values are JSON-friendly: 64-bit
   * integers are numbers, or decimal strings beyond 2^53; dates and
   * timestamps are ISO 8601 strings; lists and structs are arrays and
   * objects; binary values are `Uint8Array`s.
   */
  readonly rows: readonly unknown[][];
  /** True when the query had more than `maxRows` rows. */
  readonly truncated: boolean;
}

export interface IpcResult {
  /** The result as an Arrow IPC stream. */
  readonly ipc: Uint8Array;
  readonly truncated: boolean;
}

export interface ArrowResult {
  readonly table: Table;
  readonly truncated: boolean;
}

interface Registration {
  readonly schemaIpc: Uint8Array;
  readonly pushdown: boolean;
  readonly callbacks: TableCallbacks;
}

interface TableCallbacks {
  supportsFilters(filtersJson: string): Promise<string>;
  scan(requestJson: string): ScanIterator;
}

interface ScanIterator {
  next(): Promise<Uint8Array | null>;
  close(): void;
}

const DEFAULT_MEMORY_LIMIT = 256 * 1024 * 1024;
const DEFAULT_BATCH_SIZE = 8192;

/**
 * A SQL session over virtual tables, backed by Apache DataFusion running in
 * WebAssembly.
 */
export class SessionContext {
  private readonly memoryLimit: number;
  private readonly batchSize: number;
  private readonly tables = new Map<string, Registration>();
  private native: { engine: Engine; context: Glue.Context } | undefined;
  private closed = false;

  constructor(options: SessionContextOptions = {}) {
    this.memoryLimit = options.memoryLimit ?? DEFAULT_MEMORY_LIMIT;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    if (!(this.memoryLimit > 0)) throw new RangeError('memoryLimit must be positive');
    if (!(this.batchSize > 0)) throw new RangeError('batchSize must be positive');
  }

  /** Registers a virtual table. Replaces any table with the same name. */
  registerTable(name: string, table: VirtualTable, options: RegisterTableOptions = {}): void {
    this.assertOpen();
    const schema = table.schema();
    const registration: Registration = {
      schemaIpc: tableToIPC(new Table(schema), 'stream'),
      pushdown: options.pushdown ?? false,
      callbacks: tableCallbacks(table, schema),
    };
    this.tables.set(name, registration);
    const native = this.liveNative();
    if (native) {
      native.engine.call(() => {
        native.context.deregisterTable(name);
        register(native.context, name, registration);
      });
    }
  }

  /** Removes a table. Returns false if no table had that name. */
  deregisterTable(name: string): boolean {
    this.assertOpen();
    const native = this.liveNative();
    if (native) native.engine.call(() => native.context.deregisterTable(name));
    return this.tables.delete(name);
  }

  /** Runs a query and returns rows as JSON-friendly values. */
  async sql(query: string, options?: SqlOptions): Promise<QueryResult> {
    const { table, truncated } = await this.sqlArrow(query, options);
    return { ...toRows(table), truncated };
  }

  /** Runs a query and returns an apache-arrow Table. */
  async sqlArrow(query: string, options?: SqlOptions): Promise<ArrowResult> {
    const { ipc, truncated } = await this.sqlIpc(query, options);
    return { table: tableFromIPC(ipc), truncated };
  }

  /** Runs a query and returns the result as an Arrow IPC stream. */
  async sqlIpc(query: string, options: SqlOptions = {}): Promise<IpcResult> {
    this.assertOpen();
    const { maxRows, readonly = true } = options;
    if (maxRows !== undefined && !(Number.isInteger(maxRows) && maxRows >= 0)) {
      throw new RangeError('maxRows must be a non-negative integer');
    }
    const { engine: e, context } = await this.ensureNative();
    const task = e.call(() => context.query(query, maxRows, readonly));
    return (await drive(e, task)) as IpcResult;
  }

  /** Bytes reserved in this session's DataFusion memory pool right now. */
  get memoryReserved(): number {
    const native = this.liveNative();
    return native ? native.engine.call(() => native.context.memoryReserved()) : 0;
  }

  /** Releases the session's WebAssembly memory. The session cannot be used afterwards. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const native = this.liveNative();
    this.native = undefined;
    if (native) native.engine.call(() => native.context.free());
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('SessionContext is closed');
  }

  private liveNative() {
    return this.native && !this.native.engine.crashed ? this.native : undefined;
  }

  private async ensureNative() {
    const live = this.liveNative();
    if (live) return live;
    const e = await engine();
    // Another call may have created it while this one waited.
    const raced = this.liveNative();
    if (raced) return raced;
    const context = e.call(() => {
      const ctx = new e.glue.Context(this.memoryLimit, this.batchSize);
      for (const [name, registration] of this.tables) register(ctx, name, registration);
      return ctx;
    });
    this.native = { engine: e, context };
    return this.native;
  }
}

function register(context: Glue.Context, name: string, r: Registration): void {
  context.registerTable(name, r.schemaIpc, r.pushdown, r.callbacks as unknown as Parameters<Glue.Context["registerTable"]>[3]);
}

// ── Bridging tables to the engine ────────────────────────────────────

function tableCallbacks(table: VirtualTable, schema: Schema): TableCallbacks {
  return {
    async supportsFilters(filtersJson) {
      const filters = (JSON.parse(filtersJson) as WireFilter[]).map(decodeFilter);
      const answers = table.supportsFilters
        ? await table.supportsFilters(filters)
        : filters.map((): FilterSupport => 'inexact');
      return JSON.stringify(answers);
    },
    scan(requestJson) {
      const wire = JSON.parse(requestJson) as WireScanRequest;
      const request: ScanRequest = {
        projection: wire.projection,
        filters: wire.filters.map(decodeFilter),
        limit: wire.limit,
      };
      const iterable = table.scan(request);
      const iterator: AsyncIterator<ScanBatch> | Iterator<ScanBatch> =
        Symbol.asyncIterator in iterable
          ? iterable[Symbol.asyncIterator]()
          : (iterable as Iterable<ScanBatch>)[Symbol.iterator]();
      return {
        async next() {
          const result = await iterator.next();
          if (result.done) return null;
          return toIpc(result.value, schema);
        },
        close() {
          Promise.resolve()
            .then(() => iterator.return?.())
            .catch(() => {});
        },
      };
    },
  };
}

function toIpc(batch: ScanBatch, schema: Schema): Uint8Array {
  if (batch instanceof Table) return tableToIPC(batch, 'stream');
  if (batch instanceof RecordBatch) return tableToIPC(new Table(batch), 'stream');
  throw new TypeError(
    `scan must yield apache-arrow RecordBatch or Table values, got ${describe(batch)} ` +
      `(table schema: ${schema.fields.map((f) => f.name).join(', ')})`,
  );
}

function describe(value: unknown): string {
  if (value === null) return 'null';
  if (typeof value === 'object') return value.constructor?.name ?? 'object';
  return typeof value;
}

interface WireScanRequest {
  projection: string[];
  filters: WireFilter[];
  limit: number | null;
}

type WireLiteral =
  | { t: 'null' }
  | { t: 'bool'; v: boolean }
  | { t: 'int'; v: string }
  | { t: 'float'; v: number | string }
  | { t: 'str'; v: string }
  | { t: 'date'; v: string }
  | { t: 'ts'; v: string; unit: 's' | 'ms' | 'us' | 'ns'; tz: string | null };

type WireFilter =
  | { op: ComparisonOp; column: string; value: WireLiteral }
  | { op: 'in' | 'notIn'; column: string; values: WireLiteral[] }
  | { op: 'isNull' | 'isNotNull'; column: string }
  | { op: 'and' | 'or'; args: WireFilter[] }
  | { op: 'not'; arg: WireFilter };

function decodeFilter(f: WireFilter): Filter {
  switch (f.op) {
    case 'and':
    case 'or':
      return { op: f.op, args: f.args.map(decodeFilter) };
    case 'not':
      return { op: 'not', arg: decodeFilter(f.arg) };
    case 'in':
    case 'notIn':
      return { op: f.op, column: f.column, values: f.values.map(decodeLiteral) };
    case 'isNull':
    case 'isNotNull':
      return { op: f.op, column: f.column };
    default:
      return { op: f.op, column: f.column, value: decodeLiteral(f.value) };
  }
}

const NANOS_PER_UNIT = { s: 1_000_000_000n, ms: 1_000_000n, us: 1_000n, ns: 1n } as const;

function decodeLiteral(l: WireLiteral): Literal {
  switch (l.t) {
    case 'null':
      return null;
    case 'bool':
    case 'str':
      return l.v;
    case 'int': {
      const n = BigInt(l.v);
      return n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(n) : n;
    }
    case 'float':
      if (typeof l.v === 'number') return l.v;
      return l.v === 'NaN' ? NaN : l.v.startsWith('-') ? -Infinity : Infinity;
    case 'date':
      return new Date(Number(l.v));
    case 'ts': {
      const nanos = BigInt(l.v) * NANOS_PER_UNIT[l.unit];
      return new Date(Number(nanos / 1_000_000n));
    }
  }
}

// ── Converting results to rows ───────────────────────────────────────

function toRows(table: Table): { columns: Column[]; rows: unknown[][] } {
  const fields = table.schema.fields;
  const columns = fields.map((f) => ({ name: f.name, type: String(f.type), nullable: f.nullable }));
  const vectors = fields.map((_, i) => table.getChildAt(i)!);
  const rows: unknown[][] = [];
  for (let r = 0; r < table.numRows; r++) {
    rows.push(vectors.map((v, c) => toJsonValue(v.get(r), fields[c]!.type)));
  }
  return { columns, rows };
}

function toJsonValue(value: unknown, type: DataType): unknown {
  if (value === null || value === undefined) return null;
  if (DataType.isTimestamp(type) || DataType.isDate(type)) {
    const ms = typeof value === 'bigint' ? Number(value) : value instanceof Date ? value.getTime() : Number(value);
    return new Date(ms).toISOString();
  }
  return plain(value);
}

function plain(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value === 'bigint') {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
  }
  if (typeof value !== 'object' || value instanceof Uint8Array || value instanceof Date) return value;
  if ('toJSON' in value && typeof value.toJSON === 'function') return plain(value.toJSON());
  if (Array.isArray(value)) return value.map(plain);
  if (ArrayBuffer.isView(value)) return Array.from(value as unknown as ArrayLike<number | bigint>, plain);
  return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, plain(v)]));
}

export type { Schema, Table, RecordBatch } from 'apache-arrow';
