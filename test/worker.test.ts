import assert from 'node:assert/strict';
import { once } from 'node:events';
import { describe, it } from 'node:test';
import { Worker } from 'node:worker_threads';

describe('worker_threads', () => {
  it('runs queries inside a worker', async () => {
    const worker = new Worker(new URL('./fixtures/worker.mjs', import.meta.url), {
      workerData: { rows: 1000 },
    });
    const [result] = await once(worker, 'message');
    assert.deepEqual(result.rows, [[1000, 499_500]]);
    await worker.terminate();
  });
});
