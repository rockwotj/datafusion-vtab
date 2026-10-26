import { parentPort, workerData } from 'node:worker_threads';

import { Field, Int32, Schema, tableFromArrays } from 'apache-arrow';

import { SessionContext } from '../../dist/index.js';

const ctx = new SessionContext({ memoryLimit: 64 * 1024 * 1024 });
ctx.registerTable('numbers', {
  schema: () => new Schema([new Field('n', new Int32())]),
  async *scan() {
    yield tableFromArrays({ n: Int32Array.from({ length: workerData.rows }, (_, i) => i) });
  },
});
const result = await ctx.sql('SELECT COUNT(*) AS c, SUM(n) AS s FROM numbers');
parentPort.postMessage(result);
