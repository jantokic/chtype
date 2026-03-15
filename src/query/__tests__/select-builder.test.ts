import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import { or, and, fn } from '../expressions.js';
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

  describe('DISTINCT', () => {
    it('builds SELECT DISTINCT', () => {
      const { sql } = qb.selectFrom('users').select(['user_id', 'name']).distinct().compile();
      expect(sql).toBe('SELECT DISTINCT user_id, name\nFROM users');
    });

    it('builds SELECT DISTINCT *', () => {
      const { sql } = qb.selectFrom('users').distinct().compile();
      expect(sql).toBe('SELECT DISTINCT *\nFROM users');
    });
  });

  describe('IS NULL / IS NOT NULL', () => {
    it('builds WHERE IS NULL', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', 'IS NULL')
        .compile();
      expect(sql).toContain('WHERE score IS NULL');
    });

    it('builds WHERE IS NOT NULL', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', 'IS NOT NULL')
        .compile();
      expect(sql).toContain('WHERE score IS NOT NULL');
    });

    it('combines IS NULL with other conditions', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', '=', qb.param('name', 'String'))
        .where('score', 'IS NOT NULL')
        .compile();
      expect(sql).toContain('WHERE name = {name:String} AND score IS NOT NULL');
    });
  });

  describe('BETWEEN', () => {
    it('builds WHERE BETWEEN', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', 'BETWEEN', [qb.param('low', 'Float64'), qb.param('high', 'Float64')])
        .compile();
      expect(sql).toContain('WHERE score BETWEEN {low:Float64} AND {high:Float64}');
      expect(params).toHaveProperty('low');
      expect(params).toHaveProperty('high');
    });

    it('builds WHERE NOT BETWEEN', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', 'NOT BETWEEN', [qb.param('low', 'Float64'), qb.param('high', 'Float64')])
        .compile();
      expect(sql).toContain('WHERE score NOT BETWEEN {low:Float64} AND {high:Float64}');
    });
  });

  describe('NOT LIKE / ILIKE', () => {
    it('builds WHERE NOT LIKE', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', 'NOT LIKE', qb.param('pattern', 'String'))
        .compile();
      expect(sql).toContain('WHERE name NOT LIKE {pattern:String}');
    });

    it('builds WHERE ILIKE', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', 'ILIKE', qb.param('pattern', 'String'))
        .compile();
      expect(sql).toContain('WHERE name ILIKE {pattern:String}');
    });

    it('builds WHERE NOT ILIKE', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', 'NOT ILIKE', qb.param('pattern', 'String'))
        .compile();
      expect(sql).toContain('WHERE name NOT ILIKE {pattern:String}');
    });
  });

  describe('OR / AND grouping', () => {
    it('builds WHERE with OR group', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where(or(
          ['score', '>', qb.param('min', 'Float64')],
          ['name', '=', qb.param('name', 'String')],
        ))
        .compile();
      expect(sql).toContain('WHERE (score > {min:Float64} OR name = {name:String})');
      expect(params).toHaveProperty('min');
      expect(params).toHaveProperty('name');
    });

    it('builds WHERE with AND group', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where(and(
          ['score', '>', qb.param('min', 'Float64')],
          ['name', '=', qb.param('name', 'String')],
        ))
        .compile();
      expect(sql).toContain('WHERE (score > {min:Float64} AND name = {name:String})');
    });

    it('combines OR group with regular conditions', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', '!=', qb.param('excludeId', 'String'))
        .where(or(
          ['score', '>', qb.param('min', 'Float64')],
          ['name', 'LIKE', qb.param('pattern', 'String')],
        ))
        .compile();
      expect(sql).toContain(
        'WHERE user_id != {excludeId:String} AND (score > {min:Float64} OR name LIKE {pattern:String})',
      );
    });

    it('nests OR inside AND', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where(and(
          or(
            ['score', '>', qb.param('min', 'Float64')],
            ['score', '<', qb.param('neg', 'Float64')],
          ),
          ['name', '!=', qb.param('name', 'String')],
        ))
        .compile();
      expect(sql).toContain(
        'WHERE ((score > {min:Float64} OR score < {neg:Float64}) AND name != {name:String})',
      );
    });

    it('handles Expression value inside or() tuple', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where(or(
          ['updated_at', '>', fn.raw('now()')],
          ['score', '=', qb.param('score', 'Float64')],
        ))
        .compile();
      expect(sql).toContain('(updated_at > now() OR score = {score:Float64})');
    });
  });

  describe('BETWEEN with Expression bounds', () => {
    it('builds BETWEEN with Expression bounds', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('updated_at', 'BETWEEN', [fn.raw("toStartOfDay(now())"), fn.raw('now()')])
        .compile();
      expect(sql).toContain('WHERE updated_at BETWEEN toStartOfDay(now()) AND now()');
    });
  });

  describe('settings injection prevention', () => {
    it('rejects string values with single quotes', () => {
      expect(() => {
        qb.selectFrom('users').settings({ log_comment: "foo' , inject = 'bar" });
      }).toThrow("contains invalid character: '");
    });
  });
});
