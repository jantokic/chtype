import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import { Expression, fn, Subquery } from '../expressions.js';
import { Param } from '../param.js';
import { sql } from '../sql-template.js';
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

describe('sql tagged template', () => {
  const qb = createQueryBuilder<TestDB>();

  it('returns CompiledQuery with no interpolations', () => {
    const query = sql`SELECT 1`;
    expect(query.sql).toBe('SELECT 1');
    expect(query.params).toEqual({});
  });

  describe('Param interpolation', () => {
    it('renders Param as {name:Type} placeholder', () => {
      const minScore = qb.param('min', 'Float64');
      const query = sql`SELECT user_id FROM users WHERE score > ${minScore}`;
      expect(query.sql).toBe('SELECT user_id FROM users WHERE score > {min:Float64}');
      expect(query.params).toHaveProperty('min');
    });

    it('registers multiple params', () => {
      const min = qb.param('min', 'Float64');
      const name = qb.param('name', 'String');
      const query = sql`SELECT * FROM users WHERE score > ${min} AND name = ${name}`;
      expect(query.sql).toBe('SELECT * FROM users WHERE score > {min:Float64} AND name = {name:String}');
      expect(query.params).toHaveProperty('min');
      expect(query.params).toHaveProperty('name');
    });
  });

  describe('Expression interpolation', () => {
    it('renders Expression as raw SQL', () => {
      const query = sql`SELECT ${fn.count().as('total')} FROM users`;
      expect(query.sql).toBe('SELECT count() AS total FROM users');
      expect(query.params).toEqual({});
    });

    it('renders Expression without alias', () => {
      const query = sql`SELECT ${fn.count()} FROM users`;
      expect(query.sql).toBe('SELECT count() FROM users');
    });

    it('renders fn.raw()', () => {
      const query = sql`SELECT ${fn.raw('now()')} AS ts`;
      expect(query.sql).toBe('SELECT now() AS ts');
    });
  });

  describe('Subquery interpolation', () => {
    it('renders subquery with parentheses', () => {
      const inner = qb.selectFrom('events').select(['event_id']);
      const sub = qb.subquery(inner);
      const query = sql`SELECT * FROM users WHERE user_id IN ${sub}`;
      expect(query.sql).toBe('SELECT * FROM users WHERE user_id IN (SELECT event_id\nFROM events)');
    });

    it('merges params from subquery', () => {
      const inner = qb
        .selectFrom('events')
        .select(['event_id'])
        .where('type', '=', qb.param('eventType', 'String'));
      const sub = qb.subquery(inner);
      const min = qb.param('minScore', 'Float64');
      const query = sql`SELECT * FROM users WHERE user_id IN ${sub} AND score > ${min}`;
      expect(query.params).toHaveProperty('eventType');
      expect(query.params).toHaveProperty('minScore');
    });
  });

  describe('CompiledQuery interpolation', () => {
    it('embeds a compiled query inline', () => {
      const base = qb.selectFrom('users').select(['user_id', 'name']).compile();
      const min = qb.param('min', 'Float64');
      const query = sql`${base} WHERE score > ${min}`;
      expect(query.sql).toBe('SELECT user_id, name\nFROM users WHERE score > {min:Float64}');
      expect(query.params).toHaveProperty('min');
    });

    it('merges params from embedded CompiledQuery', () => {
      const base = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', '=', qb.param('name', 'String'))
        .compile();
      const min = qb.param('min', 'Float64');
      const query = sql`${base} AND score > ${min}`;
      expect(query.params).toHaveProperty('name');
      expect(query.params).toHaveProperty('min');
    });
  });

  describe('param collision detection', () => {
    it('throws on duplicate Param names', () => {
      const p1 = qb.param('val', 'Float64');
      const p2 = qb.param('val', 'String');
      expect(() => {
        sql`SELECT * FROM users WHERE score > ${p1} AND name = ${p2}`;
      }).toThrow('Param name collision');
    });

    it('throws on collision between Param and subquery param', () => {
      const inner = qb
        .selectFrom('events')
        .select(['event_id'])
        .where('type', '=', qb.param('val', 'String'));
      const sub = qb.subquery(inner);
      const p = qb.param('val', 'Float64');
      expect(() => {
        sql`SELECT * FROM users WHERE user_id IN ${sub} AND score > ${p}`;
      }).toThrow('Param name collision');
    });

    it('throws on collision between Param and CompiledQuery param', () => {
      const base = qb
        .selectFrom('users')
        .select(['user_id'])
        .where('name', '=', qb.param('val', 'String'))
        .compile();
      const p = qb.param('val', 'Float64');
      expect(() => {
        sql`${base} AND score > ${p}`;
      }).toThrow('Param name collision');
    });

    it('throws on collision between two subqueries', () => {
      const sub1 = qb.subquery(
        qb.selectFrom('events').select(['event_id']).where('type', '=', qb.param('val', 'String')),
      );
      const sub2 = qb.subquery(
        qb.selectFrom('users').select(['user_id']).where('name', '=', qb.param('val', 'String')),
      );
      expect(() => {
        sql`SELECT * FROM t WHERE a IN ${sub1} AND b IN ${sub2}`;
      }).toThrow('Param name collision');
    });
  });

  describe('mixed interpolation', () => {
    it('handles Param + Expression + Subquery together', () => {
      const col = fn.count().as('total');
      const min = qb.param('min', 'UInt32');
      const inner = qb.selectFrom('events').select(['event_id']);
      const sub = qb.subquery(inner);
      const query = sql`SELECT ${col} FROM users WHERE user_id IN ${sub} HAVING total > ${min}`;
      expect(query.sql).toBe(
        'SELECT count() AS total FROM users WHERE user_id IN (SELECT event_id\nFROM events) HAVING total > {min:UInt32}',
      );
      expect(query.params).toHaveProperty('min');
    });

    it('handles multiline template', () => {
      const min = qb.param('min', 'Float64');
      const query = sql`
        SELECT user_id, name
        FROM users
        WHERE score > ${min}
      `;
      expect(query.sql).toContain('WHERE score > {min:Float64}');
      expect(query.params).toHaveProperty('min');
    });
  });
});
