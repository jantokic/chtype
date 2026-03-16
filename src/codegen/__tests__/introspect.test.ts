import { describe, expect, it } from 'bun:test';
import { parseVersionColumn, parseSourceTable, matchesPattern, filterTables, schemaHash } from '../introspect.js';
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

describe('parseSourceTable', () => {
  it('extracts table from simple SELECT', () => {
    expect(parseSourceTable('SELECT date, count() FROM events GROUP BY date')).toBe('events');
  });
  it('extracts table from backtick-quoted name', () => {
    expect(parseSourceTable('SELECT * FROM `my_events`')).toBe('my_events');
  });
  it('extracts table from database-qualified name', () => {
    expect(parseSourceTable('SELECT * FROM my_db.events')).toBe('events');
  });
  it('returns null for empty string', () => {
    expect(parseSourceTable('')).toBeNull();
  });
  it('returns null when no FROM clause', () => {
    expect(parseSourceTable('SELECT 1')).toBeNull();
  });
  it('handles case insensitive FROM', () => {
    expect(parseSourceTable('select * from events')).toBe('events');
  });
  it('extracts table from backtick-qualified db.table', () => {
    expect(parseSourceTable('SELECT * FROM `my_db`.`events`')).toBe('events');
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
    { name: 'users', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '', as_select: '' },
    { name: 'events', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '', as_select: '' },
    { name: 'market_data', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '', as_select: '' },
    { name: 'market_stats', engine: '', engine_full: '', sorting_key: '', partition_key: '', primary_key: '', comment: '', as_select: '' },
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
