import { describe, expect, it } from 'bun:test';
import { parseVersionColumn, matchesPattern, filterTables } from '../introspect.js';

describe('parseVersionColumn', () => {
  it('returns null for MergeTree', () => {
    expect(parseVersionColumn('MergeTree', 'MergeTree')).toBeNull();
  });
  it('returns null for RMT without version column', () => {
    expect(parseVersionColumn('ReplacingMergeTree', 'ReplacingMergeTree')).toBeNull();
  });
  it('extracts from ReplacingMergeTree(updated_at)', () => {
    expect(parseVersionColumn('ReplacingMergeTree', 'ReplacingMergeTree(updated_at)')).toBe('updated_at');
  });
  it('extracts from ReplicatedReplacingMergeTree', () => {
    expect(
      parseVersionColumn(
        'ReplicatedReplacingMergeTree',
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', updated_at)",
      ),
    ).toBe('updated_at');
  });
  it('returns null when last arg is quoted', () => {
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

describe('matchesPattern', () => {
  it('matches exact names', () => {
    expect(matchesPattern('users', 'users')).toBe(true);
    expect(matchesPattern('users', 'events')).toBe(false);
  });
  it('matches wildcard patterns', () => {
    expect(matchesPattern('market_discover_serving', 'market_*')).toBe(true);
    expect(matchesPattern('events', 'market_*')).toBe(false);
  });
  it('matches dot patterns', () => {
    expect(matchesPattern('.inner.table', '.inner.*')).toBe(true);
  });
});

describe('filterTables', () => {
  const tables = [
    { name: 'users', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '' },
    { name: 'events', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '' },
    { name: 'market_data', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '' },
    { name: 'market_stats', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '' },
  ];

  it('returns all when no filters', () => {
    expect(filterTables(tables, [], [])).toHaveLength(4);
  });
  it('filters by include', () => {
    const result = filterTables(tables, ['market_*'], []);
    expect(result.map((t) => t.name)).toEqual(['market_data', 'market_stats']);
  });
  it('filters by exclude', () => {
    const result = filterTables(tables, [], ['market_*']);
    expect(result.map((t) => t.name)).toEqual(['users', 'events']);
  });
  it('applies both include and exclude', () => {
    const result = filterTables(tables, ['market_*'], ['*_stats']);
    expect(result.map((t) => t.name)).toEqual(['market_data']);
  });
});
