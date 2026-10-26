import { Field, Int32, Schema, Utf8, tableFromArrays, type Table } from 'apache-arrow';

import type { ScanRequest, VirtualTable } from '../dist/index.js';

/** A table backed by one in-memory Arrow table, which records every scan. */
export function recordingTable(
  schema: Schema,
  data: () => Table,
  extra: Partial<VirtualTable> = {},
): VirtualTable & { scans: ScanRequest[] } {
  const scans: ScanRequest[] = [];
  return {
    scans,
    schema: () => schema,
    async *scan(request) {
      scans.push(request);
      yield data();
    },
    ...extra,
  };
}

export const usersSchema = new Schema([
  new Field('id', new Int32(), false),
  new Field('name', new Utf8(), true),
  new Field('age', new Int32(), true),
]);

export function usersData(): Table {
  return tableFromArrays({
    id: Int32Array.from([1, 2, 3, 4, 5]),
    name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    age: Int32Array.from([30, 25, 35, 28, 32]),
  });
}

export function usersTable(extra: Partial<VirtualTable> = {}) {
  return recordingTable(usersSchema, usersData, extra);
}
