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
}

describe('UpdateBuilder', () => {
  const qb = createQueryBuilder<TestDB>();

  it('builds ALTER TABLE UPDATE with SET and WHERE', () => {
    const { sql, params } = qb
      .update('users')
      .set('name', qb.param('newName', 'String'))
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe('ALTER TABLE users UPDATE name = {newName:String} WHERE user_id = {id:String}');
    expect(params).toHaveProperty('newName');
    expect(params).toHaveProperty('id');
  });

  it('builds UPDATE with multiple SET clauses', () => {
    const { sql } = qb
      .update('users')
      .set('name', qb.param('newName', 'String'))
      .set('score', qb.param('newScore', 'Float64'))
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe(
      'ALTER TABLE users UPDATE name = {newName:String}, score = {newScore:Float64} WHERE user_id = {id:String}',
    );
  });

  it('builds UPDATE with Expression value in SET', () => {
    const { sql } = qb
      .update('users')
      .set('score', qb.fn.raw('score + 1'))
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe('ALTER TABLE users UPDATE score = score + 1 WHERE user_id = {id:String}');
  });

  it('builds UPDATE with OR in WHERE', () => {
    const { sql } = qb
      .update('users')
      .set('score', qb.param('newScore', 'Float64'))
      .where(or(
        ['user_id', '=', qb.param('id1', 'String')],
        ['user_id', '=', qb.param('id2', 'String')],
      ))
      .compile();
    expect(sql).toContain('WHERE (user_id = {id1:String} OR user_id = {id2:String})');
  });

  it('builds UPDATE with ON CLUSTER', () => {
    const { sql } = qb
      .update('users')
      .onCluster('my_cluster')
      .set('name', qb.param('newName', 'String'))
      .where('user_id', '=', qb.param('id', 'String'))
      .compile();
    expect(sql).toBe(
      'ALTER TABLE users ON CLUSTER my_cluster UPDATE name = {newName:String} WHERE user_id = {id:String}',
    );
  });

  it('throws without SET', () => {
    expect(() => {
      qb.update('users').where('user_id', '=', qb.param('id', 'String')).compile();
    }).toThrow('UPDATE requires at least one SET assignment');
  });

  it('throws without WHERE', () => {
    expect(() => {
      qb.update('users').set('name', qb.param('name', 'String')).compile();
    }).toThrow('UPDATE requires at least one WHERE condition');
  });

  it('rejects invalid cluster names', () => {
    expect(() => {
      qb.update('users').onCluster('bad; DROP TABLE');
    }).toThrow('Invalid cluster name');
  });
});
