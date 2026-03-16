import { describe, expect, it } from 'bun:test';
import type { IntrospectedTable } from '../introspect.js';
import { generate } from '../generator.js';

const sampleTable: IntrospectedTable = {
  name: 'users',
  engine: 'ReplacingMergeTree',
  engineFull: 'ReplacingMergeTree(updated_at)',
  versionColumn: 'updated_at',
  sortingKey: 'user_id',
  partitionKey: '',
  primaryKey: 'user_id',
  comment: '',
  columns: [
    { name: 'user_id', type: 'String', defaultKind: '', defaultExpression: '', comment: 'Primary identifier', isInSortingKey: true, isInPrimaryKey: true, isInPartitionKey: false },
    { name: 'name', type: 'String', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
    { name: 'score', type: 'Nullable(Float64)', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
    { name: 'tags', type: 'Array(String)', defaultKind: 'DEFAULT', defaultExpression: '[]', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
    { name: 'updated_at', type: 'DateTime64(3)', defaultKind: 'DEFAULT', defaultExpression: 'now64(3)', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
    { name: 'computed_field', type: 'String', defaultKind: 'MATERIALIZED', defaultExpression: "concat(user_id, '-', name)", comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
  ],
};

describe('generate', () => {
  it('generates Row interface with all columns', () => {
    const output = generate([sampleTable], { database: 'test_db' });
    expect(output).toContain('export interface UsersRow {');
    expect(output).toContain('user_id: string;');
    expect(output).toContain('score: number | null;');
    expect(output).toContain('tags: string[];');
    expect(output).toContain('computed_field: string;');
  });

  it('makes DEFAULT columns optional in Insert', () => {
    const output = generate([sampleTable], { database: 'test_db' });
    expect(output).toContain('tags?: string[];');
    expect(output).toContain('updated_at?: number | string;');
  });

  it('excludes MATERIALIZED columns from Insert', () => {
    const output = generate([sampleTable], { database: 'test_db' });
    const insertSection = output.split('export interface UsersInsert')[1]!.split('}')[0]!;
    expect(insertSection).not.toContain('computed_field');
  });

  it('generates Database registry', () => {
    const output = generate([sampleTable], { database: 'test_db' });
    expect(output).toContain('export type Database = {');
    expect(output).toContain('row: UsersRow;');
    expect(output).toContain('insert: UsersInsert;');
    expect(output).toContain('engine: "ReplacingMergeTree"');
    expect(output).toContain('versionColumn: "updated_at"');
  });

  it('includes JSDoc with engine metadata', () => {
    const output = generate([sampleTable], { database: 'test_db' });
    expect(output).toContain('Engine: ReplacingMergeTree(updated_at)');
    expect(output).toContain('Version column: updated_at');
  });

  it('quotes non-identifier column names', () => {
    const table: IntrospectedTable = {
      ...sampleTable,
      name: 'weird_table',
      columns: [
        { name: 'normal', type: 'String', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
        { name: 'hyphen-col', type: 'String', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
        { name: '2count', type: 'UInt32', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
      ],
    };
    const output = generate([table], { database: 'test_db' });
    expect(output).toContain('normal: string;');
    expect(output).toContain('"hyphen-col": string;');
    expect(output).toContain('"2count": number;');
  });

  it('preserves casing in snakeToPascal', () => {
    const table: IntrospectedTable = {
      ...sampleTable,
      name: 'market_USDC_volume',
    };
    const output = generate([table], { database: 'test_db' });
    expect(output).toContain('export interface MarketUSDCVolumeRow {');
  });

  it('respects bigints option', () => {
    const table: IntrospectedTable = {
      ...sampleTable,
      name: 'counters',
      columns: [{ name: 'count', type: 'UInt64', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false }],
    };
    const output = generate([table], { database: 'test_db' });
    // Row type stays string, Insert type gets coercion
    expect(output).toContain('export interface CountersRow {\n  count: string;\n}');
    expect(output).toContain('export interface CountersInsert {\n  count: number | string;\n}');

    const bigintOutput = generate([table], { database: 'test_db', bigints: true });
    expect(bigintOutput).toContain('export interface CountersRow {\n  count: bigint;\n}');
    expect(bigintOutput).toContain('export interface CountersInsert {\n  count: number | bigint;\n}');
  });

  it('applies insertCoerce to Insert but not Row', () => {
    const table: IntrospectedTable = {
      ...sampleTable,
      name: 'snapshots',
      columns: [
        { name: 'id', type: 'String', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: true, isInPrimaryKey: true, isInPartitionKey: false },
        { name: 'amount', type: 'Decimal(18, 8)', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
        { name: 'captured_at', type: 'DateTime', defaultKind: '', defaultExpression: '', comment: '', isInSortingKey: false, isInPrimaryKey: false, isInPartitionKey: false },
      ],
    };
    const output = generate([table], { database: 'test_db' });
    // Row types: Decimal and DateTime stay as string
    expect(output).toContain('export interface SnapshotsRow {\n  id: string;\n  amount: string;\n  captured_at: string;\n}');
    // Insert types: Decimal and DateTime get number | string
    expect(output).toContain('export interface SnapshotsInsert {\n  id: string;\n  amount: number | string;\n  captured_at: number | string;\n}');
  });
});
