import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import { unionAll, unionDistinct, intersect, except, setOperation } from '../select-builder.js';
import type { DatabaseSchema } from '../types.js';

interface TestDB extends DatabaseSchema {
  users: {
    row: { user_id: string; name: string; score: number };
    insert: { user_id: string; name: string; score?: number };
    engine: 'MergeTree';
    versionColumn: null;
  };
  events: {
    row: { event_id: string; type: string; timestamp: string };
    insert: { event_id: string; type: string; timestamp: string };
    engine: 'MergeTree';
    versionColumn: null;
  };
}

describe('Set Operations', () => {
  const qb = createQueryBuilder<TestDB>();

  describe('UNION ALL', () => {
    it('combines two queries', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('events').select(['event_id']);
      const { sql } = unionAll(q1, q2);
      expect(sql).toBe(
        'SELECT user_id\nFROM users\nUNION ALL\nSELECT event_id\nFROM events',
      );
    });

    it('merges params from both queries', () => {
      const q1 = qb.selectFrom('users').select(['user_id']).where('score', '>', qb.param('min', 'Float64'));
      const q2 = qb.selectFrom('users').select(['user_id']).where('score', '<', qb.param('max', 'Float64'));
      const { params } = unionAll(q1, q2);
      expect(params).toHaveProperty('min');
      expect(params).toHaveProperty('max');
    });

    it('combines three or more queries', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('users').select(['user_id']);
      const q3 = qb.selectFrom('users').select(['user_id']);
      const { sql } = unionAll(q1, q2, q3);
      const parts = sql.split('UNION ALL');
      expect(parts).toHaveLength(3);
    });
  });

  describe('UNION DISTINCT', () => {
    it('combines with UNION DISTINCT', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('users').select(['user_id']);
      const { sql } = unionDistinct(q1, q2);
      expect(sql).toContain('UNION DISTINCT');
    });
  });

  describe('INTERSECT', () => {
    it('combines with INTERSECT', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('users').select(['user_id']);
      const { sql } = intersect(q1, q2);
      expect(sql).toContain('INTERSECT');
    });
  });

  describe('EXCEPT', () => {
    it('combines with EXCEPT', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('users').select(['user_id']);
      const { sql } = except(q1, q2);
      expect(sql).toContain('EXCEPT');
    });
  });

  describe('validation', () => {
    it('throws with fewer than two queries', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      expect(() => unionAll(q1)).toThrow('requires at least two queries');
    });

    it('throws on param name collision', () => {
      const q1 = qb.selectFrom('users').select(['user_id']).where('score', '>', qb.param('val', 'Float64'));
      const q2 = qb.selectFrom('users').select(['user_id']).where('score', '<', qb.param('val', 'Float64'));
      expect(() => unionAll(q1, q2)).toThrow('Param name collision');
    });
  });

  describe('setOperation generic', () => {
    it('accepts any valid operator', () => {
      const q1 = qb.selectFrom('users').select(['user_id']);
      const q2 = qb.selectFrom('users').select(['user_id']);
      const { sql } = setOperation('EXCEPT', q1, q2);
      expect(sql).toContain('EXCEPT');
    });
  });
});
