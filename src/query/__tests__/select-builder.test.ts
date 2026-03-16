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

  it('builds HAVING with IS NOT NULL (unary)', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having('total', 'IS NOT NULL')
      .compile();
    expect(sql).toContain('HAVING total IS NOT NULL');
  });

  it('builds HAVING with BETWEEN', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having('total', 'BETWEEN', [qb.param('low', 'UInt32'), qb.param('high', 'UInt32')])
      .compile();
    expect(sql).toContain('HAVING total BETWEEN {low:UInt32} AND {high:UInt32}');
  });

  it('builds HAVING with IN (set op)', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having('total', 'IN', qb.param('vals', 'Array(UInt32)'))
      .compile();
    expect(sql).toContain('HAVING total IN {vals:Array(UInt32)}');
  });

  it('builds HAVING with or() group', () => {
    const { sql } = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total')])
      .groupBy('name')
      .having(or(
        ['total', '>', qb.param('min', 'UInt32')],
        ['total', '<', qb.param('max', 'UInt32')],
      ))
      .compile();
    expect(sql).toContain('HAVING (total > {min:UInt32} OR total < {max:UInt32})');
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

    it('builds WHERE GLOBAL IN with Array param', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('user_id', 'GLOBAL IN', qb.param('ids', 'Array(String)'))
        .compile();
      expect(sql).toContain('WHERE user_id GLOBAL IN {ids:Array(String)}');
      expect(params).toHaveProperty('ids');
    });

    it('builds WHERE GLOBAL NOT IN', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', 'GLOBAL NOT IN', qb.param('excludeIds', 'Array(String)'))
        .compile();
      expect(sql).toContain('WHERE user_id GLOBAL NOT IN {excludeIds:Array(String)}');
    });

    it('builds WHERE GLOBAL IN with subquery', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('user_id', 'GLOBAL IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('WHERE user_id GLOBAL IN (SELECT event_id\nFROM events)');
    });

    it('builds WHERE GLOBAL NOT IN with subquery', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', 'GLOBAL NOT IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('WHERE user_id GLOBAL NOT IN (SELECT event_id\nFROM events)');
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

  describe('LIKE / NOT LIKE / ILIKE', () => {
    it('builds WHERE LIKE with Param', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', 'LIKE', qb.param('pattern', 'String'))
        .compile();
      expect(sql).toContain('WHERE name LIKE {pattern:String}');
      expect(params).toHaveProperty('pattern');
    });

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

  describe('fn.raw() params in SELECT columns', () => {
    it('registers params from fn.raw() expressions in SELECT', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select([
          'user_id',
          qb.fn.raw('now() - INTERVAL ', qb.param('hours', 'UInt32'), ' HOUR').as('cutoff'),
        ])
        .compile();
      expect(sql).toContain('now() - INTERVAL {hours:UInt32} HOUR AS cutoff');
      expect(params).toHaveProperty('hours');
    });
  });

  describe('SAMPLE', () => {
    it('builds SAMPLE with ratio', () => {
      const { sql } = qb.selectFrom('users').select(['user_id']).sample(0.1).compile();
      expect(sql).toContain('FROM users SAMPLE 0.1');
    });

    it('builds SAMPLE with ratio and offset', () => {
      const { sql } = qb.selectFrom('users').select(['user_id']).sample(0.1, 0.5).compile();
      expect(sql).toContain('FROM users SAMPLE 0.1 OFFSET 0.5');
    });

    it('builds SAMPLE with absolute count', () => {
      const { sql } = qb.selectFrom('users').select(['user_id']).sample(10000).compile();
      expect(sql).toContain('FROM users SAMPLE 10000');
    });

    it('combines SAMPLE with FINAL', () => {
      const { sql } = qb.selectFrom('users').select(['user_id']).final().sample(0.1).compile();
      expect(sql).toContain('FROM users FINAL SAMPLE 0.1');
    });
  });

  describe('Subqueries', () => {
    it('builds WHERE IN (subquery)', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('user_id', 'IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('WHERE user_id IN (SELECT event_id\nFROM events)');
    });

    it('builds WHERE NOT IN (subquery)', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', 'NOT IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('WHERE user_id NOT IN (SELECT event_id\nFROM events)');
    });

    it('merges params from subquery', () => {
      const inner = qb
        .selectFrom('events')
        .select(['event_id'])
        .where('type', '=', qb.param('eventType', 'String'));
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('user_id', 'IN', qb.subquery(inner))
        .where('score', '>', qb.param('minScore', 'Float64'))
        .compile();
      expect(sql).toContain('WHERE user_id IN (SELECT event_id\nFROM events\nWHERE type = {eventType:String})');
      expect(params).toHaveProperty('eventType');
      expect(params).toHaveProperty('minScore');
    });

    it('combines subquery with other conditions', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', '!=', qb.param('name', 'String'))
        .where('user_id', 'IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('WHERE name != {name:String} AND user_id IN (SELECT event_id');
    });
  });

  describe('PREWHERE', () => {
    it('builds PREWHERE with comparison', () => {
      const { sql, params } = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .prewhere('score', '>', qb.param('min', 'Float64'))
        .compile();
      expect(sql).toContain('PREWHERE score > {min:Float64}');
      expect(params).toHaveProperty('min');
    });

    it('places PREWHERE before WHERE', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('score', '>', qb.param('min', 'Float64'))
        .where('name', '=', qb.param('name', 'String'))
        .compile();
      const lines = sql.split('\n');
      const prewhereIdx = lines.findIndex((l) => l.startsWith('PREWHERE'));
      const whereIdx = lines.findIndex((l) => l.startsWith('WHERE'));
      expect(prewhereIdx).toBeGreaterThan(-1);
      expect(whereIdx).toBeGreaterThan(prewhereIdx);
    });

    it('builds PREWHERE with IS NULL', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('score', 'IS NOT NULL')
        .compile();
      expect(sql).toContain('PREWHERE score IS NOT NULL');
    });

    it('builds PREWHERE with BETWEEN', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('score', 'BETWEEN', [qb.param('low', 'Float64'), qb.param('high', 'Float64')])
        .compile();
      expect(sql).toContain('PREWHERE score BETWEEN {low:Float64} AND {high:Float64}');
    });

    it('builds PREWHERE with or() group', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere(or(
          ['score', '>', qb.param('min', 'Float64')],
          ['name', '=', qb.param('name', 'String')],
        ))
        .compile();
      expect(sql).toContain('PREWHERE (score > {min:Float64} OR name = {name:String})');
    });

    it('builds multiple PREWHERE conditions', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('score', '>', qb.param('min', 'Float64'))
        .prewhere('name', '!=', qb.param('name', 'String'))
        .compile();
      expect(sql).toContain('PREWHERE score > {min:Float64} AND name != {name:String}');
    });

    it('builds PREWHERE with IN (subquery)', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('user_id', 'IN', qb.subquery(inner))
        .compile();
      expect(sql).toContain('PREWHERE user_id IN (SELECT event_id\nFROM events)');
    });

    it('builds PREWHERE with NOT IN', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .prewhere('user_id', 'NOT IN', qb.param('ids', 'Array(String)'))
        .compile();
      expect(sql).toContain('PREWHERE user_id NOT IN {ids:Array(String)}');
    });
  });

  describe('WITH / CTE', () => {
    it('builds single CTE', () => {
      const inner = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('score', '>', qb.param('min', 'Float64'));
      const { sql, params } = qb
        .selectFrom('users')
        .with('active_users', inner)
        .select(['user_id'])
        .compile();
      expect(sql).toContain('WITH active_users AS (SELECT user_id, name\nFROM users\nWHERE score > {min:Float64})');
      expect(sql).toContain('SELECT user_id\nFROM users');
      expect(params).toHaveProperty('min');
    });

    it('builds multiple CTEs in a single WITH block', () => {
      const cte1 = qb.selectFrom('users').select(['user_id']);
      const cte2 = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .selectFrom('users')
        .with('active', cte1)
        .with('recent', cte2)
        .select(['user_id'])
        .compile();
      expect(sql).toContain('WITH active AS (SELECT user_id\nFROM users)');
      expect(sql).toContain('recent AS (SELECT event_id\nFROM events)');
      // Verify both CTEs are in the same WITH block (only one WITH keyword)
      const withCount = sql.split('WITH').length - 1;
      expect(withCount).toBe(1);
    });

    it('places WITH before SELECT', () => {
      const inner = qb.selectFrom('users').select(['user_id']);
      const { sql } = qb
        .selectFrom('users')
        .with('cte', inner)
        .select(['user_id'])
        .compile();
      const lines = sql.split('\n');
      const withIdx = lines.findIndex((l) => l.startsWith('WITH'));
      const selectIdx = lines.findIndex((l) => l.startsWith('SELECT'));
      expect(withIdx).toBeLessThan(selectIdx);
    });

    it('accepts Subquery directly', () => {
      const compiled = qb.selectFrom('users').select(['user_id']).compile();
      const sub = qb.subquery({ compile: () => compiled });
      const { sql } = qb
        .selectFrom('users')
        .with('cte', sub)
        .select(['user_id'])
        .compile();
      expect(sql).toContain('WITH cte AS (SELECT user_id\nFROM users)');
    });

    it('merges params from CTE and main query', () => {
      const inner = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', '>', qb.param('cteMin', 'Float64'));
      const { params } = qb
        .selectFrom('users')
        .with('active', inner)
        .select(['user_id'])
        .where('name', '=', qb.param('name', 'String'))
        .compile();
      expect(params).toHaveProperty('cteMin');
      expect(params).toHaveProperty('name');
    });

    it('rejects invalid CTE names', () => {
      const inner = qb.selectFrom('users').select(['user_id']);
      expect(() => {
        qb.selectFrom('users').with('bad; DROP TABLE users; --', inner);
      }).toThrow('Invalid CTE name');
    });

    it('throws on param name collision between subquery and outer query', () => {
      const inner = qb
        .selectFrom('events')
        .select(['event_id'])
        .where('type', '=', qb.param('val', 'String'));
      expect(() => {
        qb.selectFrom('users')
          .select(['user_id'])
          .where('user_id', 'IN', qb.subquery(inner))
          .where('score', '>', qb.param('val', 'Float64'))
          .compile();
      }).toThrow('Param name collision');
    });

    it('throws on param name collision between CTE and outer query', () => {
      const inner = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('score', '>', qb.param('val', 'Float64'));
      expect(() => {
        qb.selectFrom('users')
          .with('active', inner)
          .select(['user_id'])
          .where('name', '=', qb.param('val', 'String'))
          .compile();
      }).toThrow('Param name collision');
    });
  });

  describe('qb.with() — CTE on QueryBuilder', () => {
    it('builds CTE via qb.with().selectFrom()', () => {
      const inner = qb.selectFrom('users').select(['user_id', 'name']);
      const { sql } = qb
        .with('active', inner)
        .selectFrom('active')
        .select(['user_id', 'name'])
        .compile();
      expect(sql).toContain('WITH active AS (SELECT user_id, name\nFROM users)');
      expect(sql).toContain('SELECT user_id, name\nFROM active');
    });

    it('chains multiple CTEs', () => {
      const cte1 = qb.selectFrom('users').select(['user_id']);
      const cte2 = qb.selectFrom('events').select(['event_id']);
      const { sql } = qb
        .with('u', cte1)
        .with('e', cte2)
        .selectFrom('u')
        .select(['user_id'])
        .compile();
      expect(sql).toContain('WITH u AS (SELECT user_id\nFROM users)');
      expect(sql).toContain('e AS (SELECT event_id\nFROM events)');
      expect(sql).toContain('FROM u');
    });

    it('merges params from CTE and outer query', () => {
      const inner = qb
        .selectFrom('users')
        .select(['user_id', 'score'])
        .where('score', '>', qb.param('minScore', 'Float64'));
      const { params } = qb
        .with('top', inner)
        .selectFrom('top')
        .select(['user_id'])
        .where('user_id', '!=', qb.param('excludeId', 'String'))
        .compile();
      expect(params).toHaveProperty('minScore');
      expect(params).toHaveProperty('excludeId');
    });

    it('rejects invalid CTE names on qb.with()', () => {
      const inner = qb.selectFrom('users').select(['user_id']);
      expect(() => {
        qb.with('bad; DROP TABLE', inner);
      }).toThrow('Invalid CTE name');
    });

    it('works with tuple argMax in CTE', () => {
      const base = qb
        .selectFrom('users')
        .select(['user_id', qb.fn.argMax('name', ['user_id', 'updated_at']).as('latest_name')])
        .groupBy('user_id');
      const { sql } = qb
        .with('base', base)
        .selectFrom('base')
        .select(['user_id', 'latest_name'])
        .compile();
      expect(sql).toContain('argMax(name, (user_id, updated_at)) AS latest_name');
      expect(sql).toContain('FROM base');
    });

    it('infers CTE column types from inner query', () => {
      // Inner query selects user_id and name from users
      const inner = qb.selectFrom('users').select(['user_id', 'name']);
      // CTE columns should be typed — select() accepts them without fn.raw()
      const { sql } = qb
        .with('latest', inner)
        .selectFrom('latest')
        .select(['user_id', 'name'])
        .where('user_id', '!=', qb.param('excludeId', 'String'))
        .orderBy('name')
        .compile();
      expect(sql).toContain('SELECT user_id, name\nFROM latest');
      expect(sql).toContain('WHERE user_id != {excludeId:String}');
      expect(sql).toContain('ORDER BY name ASC');
    });

    it('CTE column inference works with WHERE and ORDER BY', () => {
      const inner = qb.selectFrom('users').select(['user_id', 'score']);
      const { sql } = qb
        .with('scored', inner)
        .selectFrom('scored')
        .select(['user_id', 'score'])
        .where('score', '>', qb.param('min', 'Float64'))
        .orderBy('score', 'DESC')
        .compile();
      expect(sql).toContain('WHERE score > {min:Float64}');
      expect(sql).toContain('ORDER BY score DESC');
    });

    it('CTE SELECT * returns all inferred columns', () => {
      const inner = qb.selectFrom('users').select(['user_id', 'name']);
      const { sql } = qb
        .with('all_users', inner)
        .selectFrom('all_users')
        .compile();
      expect(sql).toContain('SELECT *\nFROM all_users');
    });
  });

  describe('groupByTimeInterval', () => {
    it('adds toStartOfHour and groups by it', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type', fn.count()])
        .groupByTimeInterval('timestamp', 'hour')
        .compile();
      expect(sql).toContain('toStartOfHour(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfHour(timestamp)');
    });

    it('supports minute interval', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type'])
        .groupByTimeInterval('timestamp', 'minute')
        .compile();
      expect(sql).toContain('toStartOfMinute(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfMinute(timestamp)');
    });

    it('supports day interval', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type'])
        .groupByTimeInterval('timestamp', 'day')
        .compile();
      expect(sql).toContain('toStartOfDay(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfDay(timestamp)');
    });

    it('supports week interval', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type'])
        .groupByTimeInterval('timestamp', 'week')
        .compile();
      expect(sql).toContain('toStartOfWeek(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfWeek(timestamp)');
    });

    it('supports month interval', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type'])
        .groupByTimeInterval('timestamp', 'month')
        .compile();
      expect(sql).toContain('toStartOfMonth(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfMonth(timestamp)');
    });

    it('supports year interval', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type'])
        .groupByTimeInterval('timestamp', 'year')
        .compile();
      expect(sql).toContain('toStartOfYear(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfYear(timestamp)');
    });

    it('combines with explicit groupBy columns', () => {
      const { sql } = qb
        .selectFrom('events')
        .select(['type', fn.count()])
        .groupByTimeInterval('timestamp', 'hour')
        .groupBy('type')
        .compile();
      expect(sql).toContain('GROUP BY toStartOfHour(timestamp), type');
    });

    it('works when called before select()', () => {
      const { sql } = qb
        .selectFrom('events')
        .groupByTimeInterval('timestamp', 'day')
        .select(['type', fn.count()])
        .compile();
      expect(sql).toContain('toStartOfDay(timestamp)');
      expect(sql).toContain('GROUP BY toStartOfDay(timestamp)');
      expect(sql).toMatch(/SELECT.*type.*count\(\).*toStartOfDay\(timestamp\)/);
    });
  });

  describe('whereIf', () => {
    it('adds condition when truthy', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .whereIf(true, 'score', '>', qb.param('min', 'Float64'))
        .compile();
      expect(sql).toContain('WHERE score > {min:Float64}');
    });

    it('skips condition when falsy', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .whereIf(false, 'score', '>', qb.param('min', 'Float64'))
        .compile();
      expect(sql).not.toContain('WHERE');
    });

    it('skips condition when null', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .whereIf(null, 'score', '>', qb.param('min', 'Float64'))
        .compile();
      expect(sql).not.toContain('WHERE');
    });

    it('skips condition when undefined', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .whereIf(undefined, 'score', '>', qb.param('min', 'Float64'))
        .compile();
      expect(sql).not.toContain('WHERE');
    });

    it('chains with regular where()', () => {
      const { sql } = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', '!=', qb.param('name', 'String'))
        .whereIf(true, 'score', '>', qb.param('min', 'Float64'))
        .whereIf(false, 'score', '<', qb.param('max', 'Float64'))
        .compile();
      expect(sql).toContain('WHERE name != {name:String} AND score > {min:Float64}');
      expect(sql).not.toContain('max');
    });
  });

  describe('INSERT ... SELECT', () => {
    it('builds INSERT INTO with SELECT', () => {
      const select = qb
        .selectFrom('users')
        .select(['user_id', fn.argMax('name', 'updated_at').as('name')])
        .groupBy('user_id');
      const { sql, params } = qb.insertFrom('users_latest', select).compile();
      expect(sql).toBe(
        'INSERT INTO users_latest\nSELECT user_id, argMax(name, updated_at) AS name\nFROM users\nGROUP BY user_id',
      );
      expect(params).toEqual({});
    });

    it('merges params from the inner SELECT', () => {
      const select = qb
        .selectFrom('users')
        .select(['user_id', 'name'])
        .where('score', '>', qb.param('min', 'Float64'));
      const { sql, params } = qb.insertFrom('users_filtered', select).compile();
      expect(sql).toContain('INSERT INTO users_filtered');
      expect(sql).toContain('WHERE score > {min:Float64}');
      expect(params).toHaveProperty('min');
    });
  });

  describe('TRUNCATE', () => {
    it('builds TRUNCATE TABLE', () => {
      const { sql, params } = qb.truncate('users_staging').compile();
      expect(sql).toBe('TRUNCATE TABLE users_staging');
      expect(params).toEqual({});
    });

    it('rejects invalid table names', () => {
      expect(() => {
        qb.truncate('bad; DROP TABLE users');
      }).toThrow('Invalid table name');
    });
  });

  describe('EXCHANGE TABLES', () => {
    it('builds EXCHANGE TABLES ... AND ...', () => {
      const { sql, params } = qb.exchangeTables('users_latest', 'users_latest_next').compile();
      expect(sql).toBe('EXCHANGE TABLES users_latest AND users_latest_next');
      expect(params).toEqual({});
    });

    it('rejects invalid first table name', () => {
      expect(() => {
        qb.exchangeTables('bad; DROP TABLE', 'ok');
      }).toThrow('Invalid table name');
    });

    it('rejects invalid second table name', () => {
      expect(() => {
        qb.exchangeTables('ok', 'bad; DROP TABLE');
      }).toThrow('Invalid table name');
    });
  });

  describe('INSERT ... SELECT validation', () => {
    it('rejects invalid table names in insertFrom', () => {
      const select = qb.selectFrom('users').select(['user_id']);
      expect(() => {
        qb.insertFrom('bad; DROP TABLE users', select);
      }).toThrow('Invalid table name');
    });
  });
});
