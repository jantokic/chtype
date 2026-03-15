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

  describe('IN / NOT IN', () => {
    it('builds WHERE IN with Array param', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('user_id', 'IN', qb.param('ids', 'Array(String)'))
        .compile();
      expect(sql).toContain('WHERE user_id IN {ids:Array(String)}');
      expect(params).toHaveProperty('ids');
    });

    it('builds WHERE NOT IN', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', 'NOT IN', qb.param('excludeIds', 'Array(String)'))
        .compile();
      expect(sql).toContain('WHERE user_id NOT IN {excludeIds:Array(String)}');
    });

    it('combines IN with other conditions', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('user_id', 'IN', qb.param('ids', 'Array(String)'))
        .where('score', '>', qb.param('minScore', 'Float64'))
        .compile();
      expect(sql).toContain('WHERE user_id IN {ids:Array(String)} AND score > {minScore:Float64}');
    });
  });

  describe('JOIN', () => {
    it('builds INNER JOIN', () => {
      const { sql } = qb
        .selectFrom('users')
        .innerJoin('events', 'e', 'users.user_id', 'e.user_id')
        .select(['user_id', 'name'])
        .compile();
      expect(sql).toContain('INNER JOIN events AS e ON users.user_id = e.user_id');
    });

    it('builds LEFT JOIN without alias', () => {
      const { sql } = qb
        .selectFrom('users')
        .leftJoin('events', undefined, 'users.user_id', 'events.user_id')
        .select(['user_id'])
        .compile();
      expect(sql).toContain('LEFT JOIN events ON users.user_id = events.user_id');
    });

    it('builds RIGHT JOIN', () => {
      const { sql } = qb
        .selectFrom('events')
        .rightJoin('users', 'u', 'events.user_id', 'u.user_id')
        .compile();
      expect(sql).toContain('RIGHT JOIN users AS u ON events.user_id = u.user_id');
    });

    it('builds CROSS JOIN', () => {
      const { sql } = qb
        .selectFrom('users')
        .crossJoin('events', 'e')
        .compile();
      expect(sql).toContain('CROSS JOIN events AS e');
      expect(sql).not.toContain('ON');
    });

    it('builds ANY LEFT JOIN (ClickHouse-specific)', () => {
      const { sql } = qb
        .selectFrom('users')
        .anyLeftJoin('events', 'e', 'users.user_id', 'e.user_id')
        .compile();
      expect(sql).toContain('ANY LEFT JOIN events AS e ON users.user_id = e.user_id');
    });

    it('builds generic join with custom type', () => {
      const { sql } = qb
        .selectFrom('users')
        .join('ASOF JOIN', 'events', 'e', 'users.user_id', 'e.user_id')
        .compile();
      expect(sql).toContain('ASOF JOIN events AS e ON users.user_id = e.user_id');
    });

    it('builds table alias', () => {
      const { sql } = qb
        .selectFrom('users')
        .as('u')
        .select(['user_id'])
        .compile();
      expect(sql).toContain('FROM users AS u');
    });

    it('builds multiple joins', () => {
      const { sql } = qb
        .selectFrom('users')
        .as('u')
        .innerJoin('events', 'e', 'u.user_id', 'e.user_id')
        .leftJoin('events', 'e2', 'u.user_id', 'e2.user_id')
        .select(['user_id'])
        .compile();
      expect(sql).toContain('INNER JOIN events AS e ON u.user_id = e.user_id');
      expect(sql).toContain('LEFT JOIN events AS e2 ON u.user_id = e2.user_id');
    });

    it('places JOINs after FROM and before WHERE', () => {
      const { sql } = qb
        .selectFrom('users')
        .innerJoin('events', 'e', 'users.user_id', 'e.user_id')
        .where('score', '>', qb.param('min', 'Float64'))
        .compile();
      const lines = sql.split('\n');
      const fromIdx = lines.findIndex((l) => l.startsWith('FROM'));
      const joinIdx = lines.findIndex((l) => l.includes('JOIN'));
      const whereIdx = lines.findIndex((l) => l.startsWith('WHERE'));
      expect(joinIdx).toBeGreaterThan(fromIdx);
      expect(whereIdx).toBeGreaterThan(joinIdx);
    });
  });
});
