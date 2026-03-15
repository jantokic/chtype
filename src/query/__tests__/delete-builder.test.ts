import { describe, expect, it } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import { or } from '../expressions.js';
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

describe('DeleteBuilder', () => {
  const qb = createQueryBuilder<TestDB>();

  it('builds ALTER TABLE DELETE with WHERE', () => {
    const { sql, params } = qb
      .deleteFrom('users')
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe('ALTER TABLE users DELETE WHERE user_id = {id:String}');
    expect(params).toHaveProperty('id');
  });

  it('builds DELETE with multiple WHERE conditions', () => {
    const { sql } = qb
      .deleteFrom('users')
      .where('score', '<', qb.param('min', 'Float64'))
      .where('name', 'IS NULL')
      .compile();
    expect(sql).toBe('ALTER TABLE users DELETE WHERE score < {min:Float64} AND name IS NULL');
  });

  it('builds DELETE with OR group', () => {
    const { sql } = qb
      .deleteFrom('users')
      .where(or(
        ['score', '<', qb.param('min', 'Float64')],
        ['name', '=', qb.param('name', 'String')],
      ))
      .compile();
    expect(sql).toContain('DELETE WHERE (score < {min:Float64} OR name = {name:String})');
  });

  it('builds DELETE with IN (subquery)', () => {
    const inner = qb.selectFrom('events').select(['event_id']);
    const { sql } = qb
      .deleteFrom('users')
      .where('user_id', 'IN', qb.subquery(inner))
      .compile();
    expect(sql).toContain('DELETE WHERE user_id IN (SELECT event_id\nFROM events)');
  });

  it('builds DELETE with ON CLUSTER', () => {
    const { sql } = qb
      .deleteFrom('users')
      .onCluster('my_cluster')
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe('ALTER TABLE users ON CLUSTER my_cluster DELETE WHERE user_id = {id:String}');
  });

  it('throws without WHERE', () => {
    expect(() => {
      qb.deleteFrom('users').compile();
    }).toThrow('DELETE requires at least one WHERE condition');
  });

  it('rejects invalid cluster names', () => {
    expect(() => {
      qb.deleteFrom('users').onCluster('bad; DROP TABLE users');
    }).toThrow('Invalid cluster name');
  });

  it('builds DELETE with BETWEEN', () => {
    const { sql } = qb
      .deleteFrom('users')
      .where('score', 'BETWEEN', [qb.param('low', 'Float64'), qb.param('high', 'Float64')])
      .compile();
    expect(sql).toBe('ALTER TABLE users DELETE WHERE score BETWEEN {low:Float64} AND {high:Float64}');
  });
});
