import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Field, Int32, Schema, Utf8, tableFromArrays } from 'apache-arrow';

import { EngineCrashedError, SessionContext, heapStats, init, type VirtualTable } from '../dist/index.js';

/** `rows` rows of (id, 100-character label), yielded in batches of 8192. */
function wideTable(rows: number): VirtualTable {
  const label = 'x'.repeat(100);
  return {
    schema: () => new Schema([new Field('id', new Int32()), new Field('label', new Utf8())]),
    async *scan() {
      for (let start = 0; start < rows; start += 8192) {
        const n = Math.min(8192, rows - start);
        yield tableFromArrays({
          id: Int32Array.from({ length: n }, (_, i) => start + i),
          label: Array.from({ length: n }, () => label),
        });
      }
    },
  };
}

// Run in order: the last tests lower the engine-wide heap limit.
describe('memory limits', { concurrency: false }, () => {
  it('fails a query that exceeds the session memory limit', async () => {
    const ctx = new SessionContext({ memoryLimit: 4 * 1024 * 1024 });
    ctx.registerTable('a', wideTable(200_000));
    ctx.registerTable('b', wideTable(200_000));
    await assert.rejects(
      ctx.sql('SELECT COUNT(*), MAX(a.label) FROM a JOIN b ON a.id = b.id'),
      /Resources exhausted.*HashJoinInput/s,
    );
    assert.equal(ctx.memoryReserved, 0);

    await assert.rejects(ctx.sql('SELECT label, id FROM a ORDER BY label, id DESC'), /Resources exhausted/);
    assert.equal(ctx.memoryReserved, 0);

    // The session still runs queries that fit.
    const small = await ctx.sql('SELECT COUNT(*) FROM a WHERE id < 10');
    assert.deepEqual(small.rows, [[10]]);
  });

  it('runs the same query with a larger limit', async () => {
    const ctx = new SessionContext({ memoryLimit: 512 * 1024 * 1024 });
    ctx.registerTable('a', wideTable(200_000));
    ctx.registerTable('b', wideTable(200_000));
    const result = await ctx.sql('SELECT COUNT(*), MAX(a.label) FROM a JOIN b ON a.id = b.id');
    assert.deepEqual(result.rows, [[200_000, 'x'.repeat(100)]]);
    assert.equal(ctx.memoryReserved, 0);
  });

  it('crashes the engine at the heap limit and restarts it on the next query', async () => {
    await init({ maxHeapBytes: 96 * 1024 * 1024 });
    assert.equal(heapStats()?.limit, 96 * 1024 * 1024);

    // The pool allows far more than the heap holds, so the heap limit trips first.
    const ctx = new SessionContext({ memoryLimit: 3 * 1024 * 1024 * 1024 });
    ctx.registerTable('a', wideTable(1_000_000));
    ctx.registerTable('b', wideTable(1_000_000));

    // A query in flight on the same engine fails too.
    const other = new SessionContext();
    other.registerTable('slow', {
      schema: () => new Schema([new Field('id', new Int32())]),
      async *scan() {
        await new Promise((resolve) => setTimeout(resolve, 200));
        yield tableFromArrays({ id: Int32Array.from([1]) });
      },
    });
    const inFlight = other.sql('SELECT * FROM slow');

    await assert.rejects(ctx.sql('SELECT COUNT(*), MAX(a.label) FROM a JOIN b ON a.id = b.id'), (error: unknown) => {
      assert.ok(error instanceof EngineCrashedError);
      assert.match(error.message, /heap limit of 100663296 bytes reached/);
      return true;
    });
    await assert.rejects(inFlight, EngineCrashedError);

    // Both sessions work again on a new engine, with their tables still registered.
    assert.deepEqual((await ctx.sql('SELECT COUNT(*) FROM a WHERE id < 5')).rows, [[5]]);
    assert.deepEqual((await other.sql('SELECT * FROM slow')).rows, [[1]]);
    assert.equal(heapStats()?.limit, 96 * 1024 * 1024);
  });
});
