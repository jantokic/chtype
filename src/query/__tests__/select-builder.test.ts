import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import type { DatabaseSchema } from '../types.js';

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

  it('builds SELECT *', () => {
    const { sql } = qb.selectFrom('users').compile();
    expect(sql).toBe('SELECT *\nFROM users');
  });

  it('builds SELECT with columns', () => {
    const { sql } = qb.selectFrom('users').select(['user_id', 'name']).compile();
    expect(sql).toBe('SELECT user_id, name\nFROM users');
  });

  it('builds WHERE with params (no raw strings)', () => {
    const { sql, params } = qb
      .selectFrom('users')
      .select(['user_id'])
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toContain('WHERE user_id = {id:String}');
    expect(params).toHaveProperty('id');
  });

  it('builds multiple WHERE conditions', () => {
    const { sql } = qb
      .selectFrom('users')
      .select(['user_id'])
      .where('score', '>', qb.param('minScore', 'Float64'))
      .where('name', '!=', qb.param('name', 'String'))
      .compile();
    expect(sql).toContain('WHERE score > {minScore:Float64} AND name != {name:String}');
  });

  it('builds GROUP BY + HAVING', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having('total', '>', qb.param('min', 'UInt32'))
      .compile();
    expect(sql).toContain('GROUP BY name');
    expect(sql).toContain('HAVING total > {min:UInt32}');
  });

  it('builds ORDER BY', () => {
    const { sql } = qb.selectFrom('users').select(['user_id']).orderBy('score', 'DESC').compile();
    expect(sql).toContain('ORDER BY score DESC');
  });

  it('builds LIMIT and OFFSET', () => {
    const { sql } = qb.selectFrom('users').select(['user_id']).limit(20).offset(40).compile();
    expect(sql).toContain('LIMIT 20');
    expect(sql).toContain('OFFSET 40');
  });

  it('builds FINAL', () => {
    const { sql } = qb.selectFrom('users').select(['user_id']).final().compile();
    expect(sql).toContain('FROM users FINAL');
  });

  it('builds SETTINGS', () => {
    const { sql } = qb.selectFrom('users').select(['user_id']).settings({ max_execution_time: 30 }).compile();
    expect(sql).toContain('SETTINGS max_execution_time = 30');
  });

  it('rejects invalid setting keys', () => {
    expect(() => {
      qb.selectFrom('users').settings({ 'bad; DROP TABLE': 1 });
    }).toThrow('Invalid ClickHouse setting name');
  });

  it('builds argMax expressions', () => {
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
  });

  it('builds parameterized LIMIT', () => {
    const { sql, params } = qb
      .selectFrom('users')
      .select(['user_id'])
      .limit(qb.param('limit', 'UInt32'))
      .compile();
    expect(sql).toContain('LIMIT {limit:UInt32}');
    expect(params).toHaveProperty('limit');
  });
});
