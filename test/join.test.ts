import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Field, Int32, Schema, Utf8, tableFromArrays } from 'apache-arrow';

import { SessionContext } from '../dist/index.js';
import { recordingTable, usersTable } from './helpers.ts';

function departments() {
  return recordingTable(
    new Schema([new Field('user_id', new Int32()), new Field('dept', new Utf8())]),
    () =>
      tableFromArrays({
        user_id: Int32Array.from([1, 2, 3, 4, 5]),
        dept: ['eng', 'sales', 'eng', 'hr', 'eng'],
      }),
  );
}

/** A table of `rows` rows with ids 0..rows-1, yielded in batches of 4096. */
function bigTable(rows: number) {
  let scans = 0;
  return {
    get scans() {
      return scans;
    },
    schema: () => new Schema([new Field('id', new Int32()), new Field('label', new Utf8())]),
    async *scan() {
      scans++;
      for (let start = 0; start < rows; start += 4096) {
        const n = Math.min(4096, rows - start);
        const ids = Int32Array.from({ length: n }, (_, i) => start + i);
        yield tableFromArrays({ id: ids, label: Array.from(ids, (id) => `row-${id}`) });
      }
    },
  };
}

describe('joins', () => {
  it('joins two virtual tables', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    ctx.registerTable('departments', departments());
    const result = await ctx.sql(`
      SELECT u.name, d.dept FROM users u JOIN departments d ON u.id = d.user_id
      WHERE d.dept = 'eng' ORDER BY u.name
    `);
    assert.deepEqual(result.rows, [
      ['Alice', 'eng'],
      ['Charlie', 'eng'],
      ['Eve', 'eng'],
    ]);
  });

  it('uses a hash join and scans each table once', async () => {
    const users = usersTable();
    const depts = departments();
    const ctx = new SessionContext();
    ctx.registerTable('users', users);
    ctx.registerTable('departments', depts);
    const query = 'SELECT u.name, d.dept FROM users u JOIN departments d ON u.id = d.user_id';

    const plan = await ctx.sql(`EXPLAIN ${query}`);
    const physical = String(plan.rows.find((r) => r[0] === 'physical_plan')?.[1]);
    assert.match(physical, /HashJoinExec: mode=CollectLeft, join_type=Inner/);
    assert.doesNotMatch(physical, /NestedLoopJoinExec/);
    // EXPLAIN plans the query without scanning.
    assert.equal(users.scans.length + depts.scans.length, 0);

    const result = await ctx.sql(query);
    assert.equal(result.rows.length, 5);
    assert.equal(users.scans.length, 1);
    assert.equal(depts.scans.length, 1);
  });

  it('joins 20k x 20k rows quickly', async () => {
    const left = bigTable(20_000);
    const right = bigTable(20_000);
    const ctx = new SessionContext();
    ctx.registerTable('l', left);
    ctx.registerTable('r', right);
    const started = performance.now();
    const result = await ctx.sql(`
      SELECT COUNT(*) AS n, SUM(l.id) AS total, MAX(r.label) AS max_label
      FROM l JOIN r ON l.id = r.id
    `);
    const elapsed = performance.now() - started;
    assert.deepEqual(result.rows, [[20_000, (19_999 * 20_000) / 2, 'row-9999']]);
    assert.equal(left.scans, 1);
    assert.equal(right.scans, 1);
    console.log(`20k x 20k join: ${elapsed.toFixed(0)} ms`);
    assert.ok(elapsed < 5_000, `join took ${elapsed} ms`);
  });

  it('pushes filters into each side of a join', async () => {
    const users = usersTable();
    const depts = departments();
    const ctx = new SessionContext();
    ctx.registerTable('users', users, { pushdown: true });
    ctx.registerTable('departments', depts, { pushdown: true });
    await ctx.sql(`
      SELECT u.name FROM users u JOIN departments d ON u.id = d.user_id
      WHERE d.dept = 'eng' AND u.age > 29
    `);
    assert.deepEqual(users.scans[0]!.filters, [{ op: '>', column: 'age', value: 29 }]);
    assert.deepEqual(depts.scans[0]!.filters, [{ op: '=', column: 'dept', value: 'eng' }]);
  });
});
