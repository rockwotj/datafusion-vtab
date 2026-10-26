# datafusion-vtab

[Apache DataFusion](https://datafusion.apache.org/) compiled to WebAssembly, with a TypeScript API for
registering async virtual tables and running SQL over them.

- Tables are JavaScript objects. `scan` is an async generator of Arrow record batches, so a table can
  page through an API, a database or a file.
- Projection, filter and limit pushdown reach `scan`. The table decides per filter whether it handles
  it exactly, approximately or not at all, and may decide asynchronously.
- Joins, aggregations and sorts run in DataFusion's vectorized engine (hash joins, not nested loops).
- Each session has a memory limit enforced by DataFusion's memory pool, and the WebAssembly heap has a
  hard cap. A runaway query fails with an error instead of taking down the process.
- JSON stored in string columns can be queried with `->`, `->>` and `json_get_*` functions.
- No native builds: one `.wasm` file runs on every platform Node runs on, including inside `worker_threads`.
  It is 20.5 MB (5.7 MB gzipped), built with `opt-level = "z"`, LTO and `wasm-opt -Oz`. Almost all of
  that is DataFusion's planner and execution engine.

## Install

```bash
npm install datafusion-vtab apache-arrow
```

Requires Node.js 20.16 or later. The package is ESM only.

## Usage

```ts
import { Field, Int32, Schema, Utf8, tableFromArrays } from 'apache-arrow';
import { SessionContext, type Filter, type VirtualTable } from 'datafusion-vtab';

const users: VirtualTable = {
  schema: () =>
    new Schema([new Field('id', new Int32()), new Field('name', new Utf8()), new Field('profile', new Utf8())]),

  // Called before planning with every filter DataFusion can push into this table.
  async supportsFilters(filters: readonly Filter[]) {
    return filters.map((f) => ('column' in f && f.column === 'id' ? 'exact' : 'unsupported'));
  },

  // Called when a query reads the table. Yield Arrow tables or record batches.
  async *scan({ projection, filters, limit }) {
    for await (const page of fetchUserPages({ projection, filters, limit })) {
      yield tableFromArrays(page);
    }
  },
};

const ctx = new SessionContext({ memoryLimit: 256 * 1024 * 1024 });
ctx.registerTable('users', users, { pushdown: true });
ctx.registerTable('orders', orders, { pushdown: true });

const { columns, rows, truncated } = await ctx.sql(
  `SELECT u.name, u.profile->>'country' AS country, COUNT(*) AS orders
   FROM users u JOIN orders o ON o.user_id = u.id
   WHERE u.id < 1000
   GROUP BY 1, 2
   ORDER BY orders DESC`,
  { maxRows: 100 },
);
```

## API

### `new SessionContext(options?)`

| Option        | Default   | Meaning                                                                                     |
| ------------- | --------- | ------------------------------------------------------------------------------------------- |
| `memoryLimit` | 256 MiB   | Bytes DataFusion may reserve for hash tables, sorts and aggregations in this session.       |
| `batchSize`   | 8192      | Target rows per batch inside DataFusion.                                                    |

Sessions are cheap. Creating one per query is fine.

### `ctx.registerTable(name, table, { pushdown })`

Registers a `VirtualTable`, replacing any table with the same name. `deregisterTable(name)` removes it.

```ts
interface VirtualTable {
  schema(): Schema;
  supportsFilters?(filters: readonly Filter[]): FilterSupport[] | Promise<FilterSupport[]>;
  scan(request: ScanRequest): AsyncIterable<RecordBatch | Table> | Iterable<RecordBatch | Table>;
}

interface ScanRequest {
  projection: string[]; // columns the query reads, in schema order; empty for COUNT(*)
  filters: Filter[];    // filters accepted as 'exact' or 'inexact'; rows must match all of them
  limit: number | null; // rows the query needs; only set when every filter in the query is exact
}
```

`scan` is called each time a query reads the table, and only when the query runs (`EXPLAIN` does not
scan). Batches must contain at least the projected columns. Extra columns are dropped and types are
cast to the declared schema, so dictionary-encoded strings from `tableFromArrays` are fine. If the query
stops early (a `LIMIT`, an error, a join that no longer needs the input), the iterator's `return()` is
called, so `finally` blocks run. Errors thrown by `scan` fail the query with a message naming the table.

Projection is always passed. Filters and limits are only passed when the table is registered with
`pushdown: true`.

### Filters

Filters arrive as a small expression tree. DataFusion splits `WHERE a AND b` into separate filters
before offering them.

```ts
type Filter =
  | { op: '=' | '!=' | '<' | '<=' | '>' | '>='; column: string; value: Literal }
  | { op: 'in' | 'notIn'; column: string; values: Literal[] }
  | { op: 'isNull' | 'isNotNull'; column: string }
  | { op: 'and' | 'or'; args: Filter[] }
  | { op: 'not'; arg: Filter };

type Literal = null | boolean | number | bigint | string | Date;
```

`literal op column` is flipped to `column op' literal`. Integers beyond 2^53 are `bigint`s. Dates and
timestamps are `Date`s (timestamps finer than milliseconds lose precision). Anything else, such as
`LIKE`, function calls, arithmetic or comparisons between two columns, is never offered and DataFusion
evaluates it.

`supportsFilters` answers one of these per filter:

| Answer        | `scan` gets the filter | DataFusion re-checks rows | `LIMIT` can be pushed |
| ------------- | ---------------------- | ------------------------- | --------------------- |
| `exact`       | yes                    | no                        | yes                   |
| `inexact`     | yes                    | yes                       | no                    |
| `unsupported` | no                     | yes                       | no                    |

Without `supportsFilters`, every filter is `inexact`. `supportsFilters` may be async: the query is
planned once to collect the filters, the answers are awaited, and then the query is planned for real.

### Running queries

```ts
ctx.sql(query, options?): Promise<{ columns: Column[]; rows: unknown[][]; truncated: boolean }>
ctx.sqlArrow(query, options?): Promise<{ table: Table; truncated: boolean }>  // apache-arrow Table
ctx.sqlIpc(query, options?): Promise<{ ipc: Uint8Array; truncated: boolean }> // Arrow IPC stream
```

| Option     | Default | Meaning                                                                                  |
| ---------- | ------- | ---------------------------------------------------------------------------------------- |
| `maxRows`  | none    | Return at most this many rows. `truncated` is true if there were more.                   |
| `readonly` | `true`  | Reject DDL (`CREATE`), DML (`INSERT`) and statements (`SET`), so a query cannot change the session or raise its own memory limit. |

`sql` returns rows as arrays of JSON-friendly values: 64-bit integers are numbers, or decimal strings
beyond 2^53. Dates and timestamps are ISO 8601 strings. Lists and structs are arrays and objects. JSON
values returned by `->` and `json_get` are JSON text.

The SQL dialect is DataFusion's, parsed as PostgreSQL. See the
[DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html).

### JSON functions

From [`datafusion-functions-json`](https://github.com/datafusion-contrib/datafusion-functions-json):
`json_get`, `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`, `json_get_json`,
`json_get_array`, `json_as_text`, `json_contains`, `json_length`, `json_object_keys`, and the
operators `->`, `->>`, `?`, `#>` and `#>>`.

```sql
SELECT doc->>'name', json_get_int(doc, 'stats', 'count') FROM events WHERE doc->>'type' = 'click'
```

### Memory

There are two limits:

1. **Session memory limit** (`memoryLimit`). DataFusion reserves memory from a pool before it builds a
   hash table, sorts or aggregates. A query that needs more than the limit fails with
   `Resources exhausted: ...`, and the session keeps working. There is no disk to spill to.
2. **Heap limit** (`init({ maxHeapBytes })`, default 1 GiB). A cap on all memory the engine allocates,
   shared by all sessions. It covers allocations the pool does not track, such as result buffers. If a
   query reaches it, the WebAssembly instance traps. Every query running on it fails with
   `EngineCrashedError`, and the next query starts a new instance. Sessions and their tables carry over.

```ts
import { init, heapStats } from 'datafusion-vtab';

await init({ maxHeapBytes: 512 * 1024 * 1024 }); // optional; queries start the engine on first use
heapStats(); // { used, peak, limit }
```

Keep `memoryLimit` times the number of concurrent sessions well under `maxHeapBytes`.

### Browsers

The engine loads its `.wasm` file from next to the module, by reading the file in Node and with
`fetch` elsewhere. To load it from somewhere else, pass `init({ module })` a URL, the bytes, or a
compiled `WebAssembly.Module`. Browser use is untested.

## How it works

The Rust crate in [`crate/`](crate/) wraps DataFusion with `wasm-bindgen`. Each registered table is a
DataFusion `TableProvider` whose scan calls back into JavaScript. Record batches cross the boundary as
Arrow IPC streams and filters as JSON. JavaScript promises are awaited from Rust with
`wasm-bindgen-futures`. The query future is polled from JavaScript instead of from a microtask, so a
trap inside Rust surfaces as an exception the wrapper can catch. Everything runs on one thread with
`target_partitions = 1`.

## Building from source

Requirements: Rust with the `wasm32-unknown-unknown` target, `wasm-bindgen-cli` at the version in
`crate/Cargo.lock`, clang, and an `llvm-ar` (from LLVM or `rustup component add llvm-tools`).

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.129 --locked
npm install
npm run build   # WASM_OPT=0 skips wasm-opt for faster local builds
npm test
```

## License

Apache-2.0
