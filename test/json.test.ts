import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Field, Int32, Schema, Utf8, tableFromArrays } from 'apache-arrow';

import { SessionContext } from '../dist/index.js';
import { recordingTable } from './helpers.ts';

function docs() {
  return recordingTable(
    new Schema([new Field('id', new Int32()), new Field('doc', new Utf8())]),
    () =>
      tableFromArrays({
        id: Int32Array.from([1, 2, 3, 4]),
        doc: [
          '{"name": "a", "count": 1, "tags": ["x", "y"], "meta": {"ok": true}}',
          '{"name": "b", "count": 2.5, "tags": []}',
          '{"count": "3"}',
          'not json',
        ],
      }),
  );
}

describe('JSON functions', () => {
  it('supports -> and ->>', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('docs', docs());
    const result = await ctx.sql(`
      SELECT id, doc->>'name' AS name, doc->'tags' AS tags, doc->'meta'->>'ok' AS ok
      FROM docs ORDER BY id
    `);
    assert.deepEqual(result.rows, [
      [1, 'a', '["x", "y"]', 'true'],
      [2, 'b', '[]', null],
      [3, null, null, null],
      [4, null, null, null],
    ]);
  });

  it('supports json_get_* functions', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('docs', docs());
    const result = await ctx.sql(`
      SELECT id,
        json_get_str(doc, 'name') AS name,
        json_get_int(doc, 'count') AS count_int,
        json_get_float(doc, 'count') AS count_float,
        json_get_bool(doc, 'meta', 'ok') AS ok,
        json_get_str(doc, 'tags', 1) AS second_tag,
        json_length(doc, 'tags') AS tag_count,
        json_contains(doc, 'meta') AS has_meta,
        json_get_json(doc, 'meta') AS meta
      FROM docs ORDER BY id
    `);
    assert.deepEqual(result.rows, [
      [1, 'a', 1, 1, true, 'y', 2, true, '{"ok": true}'],
      [2, 'b', null, 2.5, null, null, 0, false, null],
      // json_get_int and json_get_float parse numeric strings.
      [3, null, 3, 3, null, null, null, false, null],
      [4, null, null, null, null, null, null, false, null],
    ]);
  });

  it('filters and casts JSON values', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('docs', docs());
    const result = await ctx.sql(`
      SELECT id FROM docs WHERE (doc->'count')::float > 1 ORDER BY id
    `);
    assert.deepEqual(result.rows, [[2], [3]]);
    const byName = await ctx.sql("SELECT id FROM docs WHERE doc->>'name' = 'b'");
    assert.deepEqual(byName.rows, [[2]]);
  });

  it('supports the ?, #> and #>> operators', async () => {
    const ctx = new SessionContext();
    ctx.registerTable('docs', docs());
    const result = await ctx.sql(`
      SELECT id, doc ? 'meta' AS has_meta, doc #>> array['meta', 'ok'] AS ok, doc #> array['tags'] AS tags
      FROM docs WHERE id <= 2 ORDER BY id
    `);
    assert.deepEqual(result.rows, [
      [1, true, 'true', '["x", "y"]'],
      [2, false, null, '[]'],
    ]);
  });
});
