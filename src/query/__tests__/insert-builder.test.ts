import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import type { DatabaseSchema } from '../types.js';

interface TestDB extends DatabaseSchema {
  users: {
    row: { user_id: string; name: string; score: number };
    insert: { user_id: string; name: string; score?: number };
    engine: 'ReplacingMergeTree';
    versionColumn: 'updated_at';
  };
}

describe('InsertBuilder', () => {
  const qb = createQueryBuilder<TestDB>();

  it('builds INSERT with table name', () => {
    const result = qb.insertInto('users').values([]).compile();
    expect(result.sql).toBe('INSERT INTO users');
    expect(result.rows).toHaveLength(0);
  });

  it('builds INSERT with columns from first row', () => {
    const result = qb
      .insertInto('users')
      .values([{ user_id: '1', name: 'Alice' }])
      .compile();
    expect(result.sql).toBe('INSERT INTO users (user_id, name)');
    expect(result.rows).toHaveLength(1);
    expect(result.table).toBe('users');
  });

  it('includes all rows', () => {
    const result = qb
      .insertInto('users')
      .values([
        { user_id: '1', name: 'Alice', score: 10 },
        { user_id: '2', name: 'Bob', score: 20 },
      ])
      .compile();
    expect(result.rows).toHaveLength(2);
    expect(result.sql).toContain('user_id, name, score');
  });
});
