import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  Field,
  Float64,
  Int64,
  Schema,
  TimestampMillisecond,
  Utf8,
  tableFromArrays,
  tableFromIPC,
  vectorFromArray,
  Table,
} from 'apache-arrow';

import { SessionContext } from '../dist/index.js';
import { recordingTable, usersTable } from './helpers.ts';

describe('queries', () => {
  it('runs a simple SELECT', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const result = await ctx.sql('SELECT * FROM users ORDER BY id');
    assert.deepEqual(
      result.columns.map((c) => [c.name, c.type]),
      [
        ['id', 'Int32'],
        ['name', 'Utf8'],
        ['age', 'Int32'],
      ],
    );
    assert.equal(result.rows.length, 5);
    assert.deepEqual(result.rows[0], [1, 'Alice', 30]);
    assert.equal(result.truncated, false);
  });

  it('filters, aggregates and groups', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const filtered = await ctx.sql('SELECT name, age FROM users WHERE age > 30 ORDER BY age');
    assert.deepEqual(filtered.rows, [
      ['Eve', 32],
      ['Charlie', 35],
    ]);

    const agg = await ctx.sql('SELECT COUNT(*) AS n, MIN(age) AS lo, MAX(age) AS hi FROM users');
    assert.deepEqual(agg.rows, [[5, 25, 35]]);

    ctx.registerTable(
      'items',
      recordingTable(
        new Schema([new Field('category', new Utf8()), new Field('price', new Float64())]),
        () =>
          tableFromArrays({
            category: ['a', 'b', 'a', 'b', 'a'],
            price: Float64Array.from([10, 20, 30, 40, 50]),
          }),
      ),
    );
    const grouped = await ctx.sql(
      'SELECT category, SUM(price) AS total FROM items GROUP BY category ORDER BY category',
    );
    assert.deepEqual(grouped.rows, [
      ['a', 90],
      ['b', 60],
    ]);
  });

  it('returns columns for an empty result', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const result = await ctx.sql('SELECT id, name FROM users WHERE id > 100');
    assert.deepEqual(result.columns.map((c) => c.name), ['id', 'name']);
    assert.deepEqual(result.rows, []);
  });

  it('truncates at maxRows', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const result = await ctx.sql('SELECT id FROM users ORDER BY id', { maxRows: 2 });
    assert.deepEqual(result.rows, [[1], [2]]);
    assert.equal(result.truncated, true);

    const exact = await ctx.sql('SELECT id FROM users', { maxRows: 5 });
    assert.equal(exact.rows.length, 5);
    assert.equal(exact.truncated, false);
  });

  it('runs EXPLAIN with maxRows', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const result = await ctx.sql('EXPLAIN SELECT id FROM users WHERE age > 30', { maxRows: 100 });
    assert.deepEqual(result.columns.map((c) => c.name), ['plan_type', 'plan']);
    assert.ok(result.rows.length >= 2);
    assert.equal(result.truncated, false);

    const one = await ctx.sql('EXPLAIN SELECT id FROM users', { maxRows: 1 });
    assert.equal(one.rows.length, 1);
    assert.equal(one.truncated, true);
  });

  it('runs EXPLAIN ANALYZE with maxRows', async () => {
    const ctx = new SessionContext();
    const users = usersTable();
    ctx.registerTable('users', users);
    const result = await ctx.sql('EXPLAIN ANALYZE SELECT id FROM users ORDER BY id', { maxRows: 100 });
    assert.deepEqual(result.columns.map((c) => c.name), ['plan_type', 'plan']);
    assert.equal(result.rows.length, 1);
    const [planType, plan] = result.rows[0] ?? [];
    assert.equal(planType, 'Plan with Metrics');
    assert.match(String(plan), /output_rows=5/);
    assert.equal(result.truncated, false);
    assert.equal(users.scans.length, 1);

    const verbose = await ctx.sql('EXPLAIN ANALYZE VERBOSE SELECT id FROM users', { maxRows: 2 });
    assert.deepEqual(verbose.rows.map((r) => r[0]), ['Plan with Metrics', 'Plan with Full Metrics']);
    assert.equal(verbose.truncated, true);
  });

  it('runs DESCRIBE with maxRows', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const result = await ctx.sql('DESCRIBE users', { maxRows: 2 });
    assert.deepEqual(result.rows.map((r) => r[0]), ['id', 'name']);
    assert.equal(result.truncated, true);
  });

  it('returns Arrow IPC and apache-arrow tables', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    const { ipc, truncated } = await ctx.sqlIpc('SELECT id, name FROM users ORDER BY id', { maxRows: 3 });
    assert.ok(ipc instanceof Uint8Array);
    assert.equal(truncated, true);
    const table = tableFromIPC(ipc);
    assert.equal(table.numRows, 3);
    assert.equal(table.getChild('name')?.get(2), 'Charlie');

    const arrow = await ctx.sqlArrow('SELECT COUNT(*) AS n FROM users');
    assert.equal(arrow.table.getChild('n')?.get(0), 5n);
  });

  it('converts 64-bit integers and timestamps to JSON-friendly values', async () => {
    const ctx = new SessionContext();
    const schema = new Schema([
      new Field('big', new Int64()),
      new Field('at', new TimestampMillisecond()),
    ]);
    ctx.registerTable(
      't',
      recordingTable(schema, () =>
        new Table({
          big: vectorFromArray([1n, 2n ** 60n], new Int64()),
          at: vectorFromArray(
            [new Date(Date.UTC(2024, 0, 2)), new Date(Date.UTC(2025, 5, 6, 7, 8, 9))],
            new TimestampMillisecond(),
          ),
        }),
      ),
    );
    const result = await ctx.sql('SELECT big, at FROM t ORDER BY big');
    assert.deepEqual(result.rows, [
      [1, '2024-01-02T00:00:00.000Z'],
      [(2n ** 60n).toString(), '2025-06-06T07:08:09.000Z'],
    ]);
  });

  it('rejects DDL, DML and SET', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    await assert.rejects(ctx.sql('CREATE TABLE x AS SELECT 1'), /DDL not supported/);
    await assert.rejects(ctx.sql('CREATE VIEW v AS SELECT 1'), /DDL not supported/);
    await assert.rejects(ctx.sql('INSERT INTO users VALUES (9, \'x\', 1)'), /not supported/);
    await assert.rejects(ctx.sql("SET datafusion.runtime.memory_limit = '10G'"), /not supported/);
    // Queries, EXPLAIN and SHOW still work.
    assert.equal((await ctx.sql('SHOW TABLES')).rows.length > 0, true);
    assert.equal((await ctx.sql('EXPLAIN SELECT 1')).rows.length > 0, true);
  });

  it('includes window, table, crypto and encoding functions', async () => {
    const ctx = new SessionContext();
    const result = await ctx.sql(`
      SELECT value, row_number() OVER (ORDER BY value DESC) AS rank,
        md5('hi') AS md5, encode('hi', 'hex') AS hex
      FROM generate_series(1, 2) ORDER BY value
    `);
    assert.deepEqual(result.rows, [
      [1, 2, '49f68a5c8493ec2c0bf489821c21fc3b', '6869'],
      [2, 1, '49f68a5c8493ec2c0bf489821c21fc3b', '6869'],
    ]);
  });

  it('reports SQL errors', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    await assert.rejects(ctx.sql('SELECT nope FROM users'), /nope/);
    await assert.rejects(ctx.sql('SELECT * FROM missing'), /missing/);
    // The session still works.
    assert.equal((await ctx.sql('SELECT COUNT(*) FROM users')).rows[0]![0], 5);
  });

  it('runs concurrent queries on one session', async () => {
    const ctx = new SessionContext();
    ctx.registerTable(
      'slow',
      recordingTable(new Schema([new Field('v', new Int64())]), () => tableFromArrays({ v: [1n] }), {
        async *scan() {
          await new Promise((resolve) => setTimeout(resolve, 20));
          yield tableFromArrays({ v: BigInt64Array.from([1n, 2n, 3n]) });
        },
      }),
    );
    const results = await Promise.all(
      Array.from({ length: 8 }, (_, i) => ctx.sql(`SELECT SUM(v) + ${i} AS s FROM slow`)),
    );
    assert.deepEqual(
      results.map((r) => r.rows[0]![0]),
      Array.from({ length: 8 }, (_, i) => 6 + i),
    );
  });

  it('re-scans tables on each query and deregisters them', async () => {
    let counter = 0;
    const ctx = new SessionContext();
    ctx.registerTable(
      'counter',
      recordingTable(new Schema([new Field('n', new Int64())]), () =>
        tableFromArrays({ n: BigInt64Array.from([BigInt(++counter)]) }),
      ),
    );
    assert.deepEqual((await ctx.sql('SELECT n FROM counter')).rows, [[1]]);
    assert.deepEqual((await ctx.sql('SELECT n FROM counter')).rows, [[2]]);
    assert.equal(ctx.deregisterTable('counter'), true);
    assert.equal(ctx.deregisterTable('counter'), false);
    await assert.rejects(ctx.sql('SELECT n FROM counter'), /counter/);
  });

  it('accepts scans that yield record batches from a sync iterable', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersTable().schema(),
      scan: () => tableFromArrays({ id: Int32Array.from([7]), name: ['Zed'], age: Int32Array.from([1]) }).batches,
    });
    assert.deepEqual((await ctx.sql('SELECT * FROM users')).rows, [[7, 'Zed', 1]]);
  });

  it('closes sessions', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    await ctx.sql('SELECT 1');
    ctx.close();
    await assert.rejects(ctx.sql('SELECT 1'), /closed/);
  });
});
