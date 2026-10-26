import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Field, Int64, Schema, TimestampMillisecond, Utf8, tableFromArrays } from 'apache-arrow';

import { SessionContext, type Filter, type FilterSupport } from '../dist/index.js';
import { recordingTable, usersData, usersTable } from './helpers.ts';

describe('projection pushdown', () => {
  it('passes only the columns the query reads', async () => {
    const users = usersTable();
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    await ctx.sql('SELECT name FROM users WHERE age > 1');
    assert.deepEqual(users.scans[0]!.projection, ['name', 'age']);
  });

  it('passes an empty projection for COUNT(*)', async () => {
    const users = usersTable();
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    assert.deepEqual((await ctx.sql('SELECT COUNT(*) FROM users')).rows, [[5]]);
    assert.deepEqual(users.scans[0]!.projection, []);
  });

  it('accepts scans that return only the projected columns', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersTable().schema(),
      async *scan({ projection }) {
        const all = usersData();
        yield all.select(projection as ('id' | 'name' | 'age')[]);
      },
    });
    assert.deepEqual((await ctx.sql('SELECT age FROM users ORDER BY age LIMIT 1')).rows, [[25]]);
  });

  it('reports a missing projected column', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersTable().schema(),
      async *scan() {
        yield tableFromArrays({ id: Int32Array.from([1]) });
      },
    });
    await assert.rejects(ctx.sql('SELECT name FROM users'), /without column "name"/);
  });
});

describe('filter pushdown', () => {
  it('passes no filters to tables without supportsFilters', async () => {
    const users = usersTable();
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    const result = await ctx.sql('SELECT id FROM users WHERE age > 30 ORDER BY id LIMIT 1');
    assert.deepEqual(result.rows, [[3]]);
    assert.deepEqual(users.scans[0]!.filters, []);
    assert.equal(users.scans[0]!.limit, null);
  });

  it('serializes comparisons, IN lists, IS NULL, AND, OR and NOT', async () => {
    const offered: Filter[] = [];
    const users = usersTable({
      supportsFilters: (filters) => {
        offered.push(...filters);
        return filters.map(() => 'inexact' as const);
      },
    });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    await ctx.sql(`
      SELECT id FROM users
      WHERE age >= 25
        AND 40 > age
        AND name IN ('Alice', 'Bob', 'Charlie', 'Diana')
        AND id NOT IN (7, 8, 9, 10)
        AND name IS NOT NULL
        AND (age = 30 OR name = 'Bob' OR age IS NULL)
        AND NOT (name = 'Zed' AND age < 3)
    `);
    const expected: Filter[] = [
      { op: '>=', column: 'age', value: 25 },
      { op: '<', column: 'age', value: 40 },
      { op: 'in', column: 'name', values: ['Alice', 'Bob', 'Charlie', 'Diana'] },
      { op: 'notIn', column: 'id', values: [7, 8, 9, 10] },
      { op: 'isNotNull', column: 'name' },
      {
        op: 'or',
        args: [
          { op: '=', column: 'age', value: 30 },
          { op: '=', column: 'name', value: 'Bob' },
          { op: 'isNull', column: 'age' },
        ],
      },
    ];
    for (const filter of expected) {
      assert.ok(
        offered.some((f) => isDeepStrictEqual(f, filter)),
        `expected ${JSON.stringify(filter)} in ${JSON.stringify(offered)}`,
      );
    }
    // DataFusion may rewrite NOT (a AND b) into (a' OR b'); either form is fine.
    assert.ok(
      offered.some((f) => f.op === 'not' || (f.op === 'or' && JSON.stringify(f).includes('Zed'))),
    );
    // Every offered filter reaches scan because all were accepted.
    assert.equal(users.scans[0]!.filters.length, offered.length);
  });

  it('decodes literals by type', async () => {
    const offered: Filter[] = [];
    const schema = new Schema([
      new Field('big', new Int64()),
      new Field('at', new TimestampMillisecond()),
      new Field('s', new Utf8()),
    ]);
    const table = recordingTable(schema, () => tableFromArrays({ big: [1n], s: ['x'] }), {
      supportsFilters: (filters) => {
        offered.push(...filters);
        return filters.map(() => 'unsupported' as const);
      },
    });
    const ctx = new SessionContext();
    ctx.registerTable('t', table);
    await ctx.sql(`
      EXPLAIN SELECT * FROM t
      WHERE big = 9007199254740993 AND big > -5
        AND at < TIMESTAMP '2024-05-06T07:08:09.123Z' AND s = 'it''s'
    `);
    assert.deepEqual(offered, [
      { op: '=', column: 'big', value: 9007199254740993n },
      { op: '>', column: 'big', value: -5 },
      { op: '<', column: 'at', value: new Date('2024-05-06T07:08:09.123Z') },
      { op: '=', column: 's', value: "it's" },
    ]);
  });

  it('marks filters it cannot express as unsupported without asking', async () => {
    const offered: Filter[] = [];
    const users = usersTable({
      supportsFilters: (filters) => {
        offered.push(...filters);
        return filters.map(() => 'exact' as const);
      },
    });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    const result = await ctx.sql(
      "SELECT id FROM users WHERE upper(name) = 'BOB' AND age + 1 > 20 AND name LIKE 'B%'",
    );
    assert.deepEqual(result.rows, [[2]]);
    assert.deepEqual(offered, []);
    assert.deepEqual(users.scans[0]!.filters, []);
  });
});

describe('supportsFilters', () => {
  // The table ignores filters and returns every row, so the result shows
  // whether DataFusion re-applied the filter.
  async function query(support: FilterSupport) {
    const users = usersTable({ supportsFilters: (filters) => filters.map(() => support) });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    const result = await ctx.sql('SELECT id FROM users WHERE age > 30 ORDER BY id');
    return { ids: result.rows.map((r) => r[0]), scan: users.scans[0]! };
  }

  it('exact: scan gets the filter and DataFusion trusts it', async () => {
    const { ids, scan } = await query('exact');
    assert.deepEqual(scan.filters, [{ op: '>', column: 'age', value: 30 }]);
    assert.deepEqual(ids, [1, 2, 3, 4, 5]);
  });

  it('inexact: scan gets the filter and DataFusion re-applies it', async () => {
    const { ids, scan } = await query('inexact');
    assert.deepEqual(scan.filters, [{ op: '>', column: 'age', value: 30 }]);
    assert.deepEqual(ids, [3, 5]);
  });

  it('unsupported: scan does not get the filter', async () => {
    const { ids, scan } = await query('unsupported');
    assert.deepEqual(scan.filters, []);
    assert.deepEqual(ids, [3, 5]);
  });

  it('decides per filter and may answer asynchronously', async () => {
    const users = usersTable({
      async supportsFilters(filters) {
        await new Promise((resolve) => setTimeout(resolve, 5));
        return filters.map((f) => ('column' in f && f.column === 'age' ? 'exact' : 'unsupported'));
      },
    });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    const result = await ctx.sql("SELECT id FROM users WHERE age > 30 AND name != 'Eve' ORDER BY id");
    assert.deepEqual(users.scans[0]!.filters, [{ op: '>', column: 'age', value: 30 }]);
    // The age filter was exact and the table ignored it; the name filter was applied by DataFusion.
    assert.deepEqual(result.rows, [[1], [2], [3], [4]]);
  });

  it('rejects a wrong number of answers', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable({ supportsFilters: () => [] }));
    await assert.rejects(ctx.sql('SELECT id FROM users WHERE age > 30'), /returned 0 answers for 1 filters/);
  });

  it('propagates errors from supportsFilters', async () => {
    const ctx = new SessionContext();
    ctx.registerTable(
      'users',
      usersTable({
        supportsFilters: async () => {
          throw new Error('index lookup failed');
        },
      })
    );
    await assert.rejects(ctx.sql('SELECT id FROM users WHERE age > 30'), /index lookup failed/);
  });
});

describe('limit pushdown', () => {
  it('passes the limit when there are no filters', async () => {
    const users = usersTable();
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    const result = await ctx.sql('SELECT id FROM users LIMIT 2');
    assert.equal(result.rows.length, 2);
    assert.equal(users.scans[0]!.limit, 2);
  });

  it('passes the limit when every filter is exact', async () => {
    const users = usersTable({ supportsFilters: (filters) => filters.map(() => 'exact' as const) });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    await ctx.sql('SELECT id FROM users WHERE age > 30 LIMIT 2');
    assert.equal(users.scans[0]!.limit, 2);
  });

  it('withholds the limit when a filter is inexact', async () => {
    const users = usersTable({ supportsFilters: (filters) => filters.map(() => 'inexact' as const) });
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    await ctx.sql('SELECT id FROM users WHERE age > 30 LIMIT 2');
    assert.equal(users.scans[0]!.limit, null);
  });

  it('stops pulling batches and closes the iterator once the limit is reached', async () => {
    let yielded = 0;
    let closed = false;
    const ctx = new SessionContext();
    ctx.registerTable(
      'numbers',
      {
        schema: () => new Schema([new Field('n', new Int64())]),
        async *scan() {
          try {
            for (let i = 0; i < 1000; i++) {
              yielded++;
              yield tableFromArrays({ n: BigInt64Array.from([BigInt(i * 10), BigInt(i * 10 + 1)]) });
            }
          } finally {
            closed = true;
          }
        },
      }
    );
    const result = await ctx.sql('SELECT n FROM numbers LIMIT 3');
    assert.equal(result.rows.length, 3);
    assert.equal(yielded, 2);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(closed, true);
  });
});

function isDeepStrictEqual(a: unknown, b: unknown): boolean {
  try {
    assert.deepStrictEqual(a, b);
    return true;
  } catch {
    return false;
  }
}
