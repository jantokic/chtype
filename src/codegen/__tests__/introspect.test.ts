import { describe, expect, it } from 'bun:test';
import { parseVersionColumn, matchesPattern, filterTables, schemaHash } from '../introspect.js';
import type { IntrospectedTable } from '../introspect.js';

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

describe('schemaHash', () => {
  const makeTable = (name: string, columns: { name: string; type: string }[]): IntrospectedTable => ({
    name,
    engine: 'MergeTree',
    engineFull: 'MergeTree',
    versionColumn: null,
    sortingKey: '',
    partitionKey: '',
    primaryKey: '',
    comment: '',
    columns: columns.map((c) => ({
      ...c,
      defaultKind: '' as const,
      defaultExpression: '',
      comment: '',
      isInSortingKey: false,
      isInPrimaryKey: false,
      isInPartitionKey: false,
    })),
  });

  it('returns deterministic hash', () => {
    const tables = [makeTable('users', [{ name: 'id', type: 'String' }])];
    expect(schemaHash(tables)).toBe(schemaHash(tables));
  });

  it('changes when column type changes', () => {
    const a = [makeTable('users', [{ name: 'id', type: 'String' }])];
    const b = [makeTable('users', [{ name: 'id', type: 'UInt64' }])];
    expect(schemaHash(a)).not.toBe(schemaHash(b));
  });

  it('changes when a column is added', () => {
    const a = [makeTable('users', [{ name: 'id', type: 'String' }])];
    const b = [makeTable('users', [{ name: 'id', type: 'String' }, { name: 'name', type: 'String' }])];
    expect(schemaHash(a)).not.toBe(schemaHash(b));
  });

  it('changes when table name changes', () => {
    const a = [makeTable('users', [{ name: 'id', type: 'String' }])];
    const b = [makeTable('accounts', [{ name: 'id', type: 'String' }])];
    expect(schemaHash(a)).not.toBe(schemaHash(b));
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
