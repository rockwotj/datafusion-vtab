import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { tableFromArrays } from 'apache-arrow';

import { SessionContext } from '../dist/index.js';
import { usersSchema, usersTable } from './helpers.ts';

describe('errors from JavaScript scans', () => {
  it('propagates an error thrown inside the scan generator', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersSchema,
      // oxlint-disable-next-line require-yield
      async *scan() {
        throw new Error('firestore unavailable');
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users'), {
      message: /scan on table "users" failed: firestore unavailable/,
    });
  });

  it('propagates an error thrown after some batches', async () => {
    let closed = false;
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersSchema,
      async *scan() {
        try {
          yield tableFromArrays({ id: Int32Array.from([1]), name: ['a'], age: Int32Array.from([1]) });
          await new Promise((resolve) => setTimeout(resolve, 5));
          throw new Error('page 2 failed');
        } finally {
          closed = true;
        }
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users'), /page 2 failed/);
    assert.equal(closed, true);
  });

  it('propagates an error thrown synchronously by scan', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersSchema,
      scan() {
        throw new TypeError('bad request');
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users'), /bad request/);
  });

  it('propagates non-Error rejections', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersSchema,
      // oxlint-disable-next-line require-yield
      async *scan() {
        throw 'plain string';
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users'), /plain string/);
  });

  it('rejects batches that are not Arrow', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', {
      schema: () => usersSchema,
      async *scan() {
        yield { id: 1 } as never;
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users'), /must yield apache-arrow RecordBatch or Table/);
  });

  it('fails a join when one side fails and keeps the session usable', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    ctx.registerTable('bad', {
      schema: () => usersSchema,
      async *scan() {
        yield tableFromArrays({ id: Int32Array.from([1]), name: ['a'], age: Int32Array.from([1]) });
        throw new Error('probe side broke');
      },
    });
    await assert.rejects(ctx.sql('SELECT * FROM users u JOIN bad b ON u.id = b.id'), /probe side broke/);
    assert.equal((await ctx.sql('SELECT COUNT(*) FROM users')).rows[0]![0], 5);
  });

  it('closes a scan that a join stops reading early', async () => {
    let closed = false;
    const ctx = new SessionContext();
    ctx.registerTable('users', usersTable());
    ctx.registerTable('endless', {
      schema: () => usersSchema,
      async *scan() {
        try {
          for (let i = 0; ; i++) {
            yield tableFromArrays({ id: Int32Array.from([1]), name: ['a'], age: Int32Array.from([i]) });
          }
        } finally {
          closed = true;
        }
      },
    });
    const result = await ctx.sql('SELECT e.age FROM users u JOIN endless e ON u.id = e.id LIMIT 3');
    assert.equal(result.rows.length, 3);
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(closed, true);
  });
});
