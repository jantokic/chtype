import { describe, expect, it } from 'vitest';
import { createQueryBuilder } from '../query-builder.js';
import type { DatabaseSchema } from '../types.js';

// Sample generated schema for testing
interface TestDB extends DatabaseSchema {
  users: {
    row: { user_id: string; name: string; score: number; updated_at: string };
    insert: { user_id: string; name: string; score: number; updated_at?: string };
    engine: 'ReplacingMergeTree';
    versionColumn: 'updated_at';
  };
  events: {
    row: { event_id: string; type: string; timestamp: string };
    insert: { event_id: string; type: string; timestamp: string };
    engine: 'MergeTree';
    versionColumn: null;
  };
}

describe('SelectBuilder', () => {
  const qb = createQueryBuilder<TestDB>();

  it('builds a basic SELECT *', () => {
    const { sql } = qb.selectFrom('users').compile();
    expect(sql).toBe('SELECT *\nFROM users');
  });

  it('builds SELECT with specific columns', () => {
    const { sql } = qb.selectFrom('users').select(['user_id', 'name']).compile();
    expect(sql).toBe('SELECT user_id, name\nFROM users');
  });

  it('builds SELECT with WHERE', () => {
    const { sql, params } = qb
      .selectFrom('users')
      .select(['user_id', 'name'])
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();

    expect(sql).toBe('SELECT user_id, name\nFROM users\nWHERE user_id = {id:String}');
    expect(params).toHaveProperty('id');
  });

  it('builds SELECT with multiple WHERE conditions', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id'])
      .where('score', '>', 100)
      .where('name', '!=', qb.param('name', 'String'))
      .compile();

    expect(sql).toContain('WHERE score > 100 AND name != {name:String}');
  });

  it('builds SELECT with GROUP BY and HAVING', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having('total', '>', 5)
      .compile();

    expect(sql).toContain('GROUP BY name');
    expect(sql).toContain('HAVING total > 5');
  });

  it('builds SELECT with ORDER BY', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id', 'score'])
      .orderBy('score', 'DESC')
      .compile();

    expect(sql).toContain('ORDER BY score DESC');
  });

  it('builds SELECT with LIMIT and OFFSET', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id'])
      .limit(20)
      .offset(40)
      .compile();

    expect(sql).toContain('LIMIT 20');
    expect(sql).toContain('OFFSET 40');
  });

  it('builds SELECT with FINAL modifier', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id'])
      .final()
      .compile();

    expect(sql).toContain('FROM users FINAL');
  });

  it('builds SELECT with SETTINGS', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id'])
      .settings({ max_execution_time: 30 })
      .compile();

    expect(sql).toContain('SETTINGS max_execution_time = 30');
  });

  it('builds SELECT with argMax expressions', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([
        qb.fn.argMax('name', 'updated_at').as('name'),
        qb.fn.argMax('score', 'updated_at').as('score'),
      ])
      .groupBy('user_id')
      .compile();

    expect(sql).toContain('argMax(name, updated_at) AS name');
    expect(sql).toContain('argMax(score, updated_at) AS score');
    expect(sql).toContain('GROUP BY user_id');
  });

  it('builds SELECT with parameterized LIMIT', () => {
    const { sql, params } = qb
      .selectFrom('users')
      .select(['user_id'])
      .limit(qb.param('limit', 'UInt32'))
      .compile();

    expect(sql).toContain('LIMIT {limit:UInt32}');
    expect(params).toHaveProperty('limit');
  });
});
