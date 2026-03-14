import { describe, expect, it } from 'vitest';
import { parseVersionColumn } from '../introspect.js';

describe('parseVersionColumn', () => {
  it('returns null for MergeTree', () => {
    expect(parseVersionColumn('MergeTree', 'MergeTree')).toBeNull();
  });

  it('returns null for ReplacingMergeTree without version column', () => {
    expect(parseVersionColumn('ReplacingMergeTree', 'ReplacingMergeTree')).toBeNull();
  });

  it('extracts version column from ReplacingMergeTree(updated_at)', () => {
    expect(
      parseVersionColumn('ReplacingMergeTree', 'ReplacingMergeTree(updated_at)'),
    ).toBe('updated_at');
  });

  it('extracts version column from ReplicatedReplacingMergeTree', () => {
    expect(
      parseVersionColumn(
        'ReplicatedReplacingMergeTree',
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/my_table', '{replica}', updated_at)",
      ),
    ).toBe('updated_at');
  });

  it('returns null when last arg is quoted (no version column)', () => {
    expect(
      parseVersionColumn(
        'ReplicatedReplacingMergeTree',
        "ReplicatedReplacingMergeTree('/path', '{replica}')",
      ),
    ).toBeNull();
  });

  it('returns null for non-replacing engines', () => {
    expect(parseVersionColumn('SummingMergeTree', 'SummingMergeTree(amount)')).toBeNull();
  });
});
