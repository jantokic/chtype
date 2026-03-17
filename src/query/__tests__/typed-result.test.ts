import { describe, expect, test } from 'bun:test';
import { createQueryBuilder } from '../query-builder.js';
import type { CompiledQuery, DatabaseSchema, SelectResult } from '../types.js';

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

const qb = createQueryBuilder<TestDB>();

/**
 * Compile-time type assertion helper.
 * If T is not assignable to U, this produces a type error.
 */
type AssertAssignable<T, U> = T extends U ? true : false;

describe('Typed result inference', () => {
  test('compile() returns typed CompiledQuery for selected columns', () => {
    const query = qb
      .selectFrom('users')
      .select(['user_id', 'name', 'score'])
      .compile();

    // Runtime: still produces correct SQL
    expect(query.sql).toBe('SELECT user_id, name, score\nFROM users');

    // Type-level: result type should be Pick<users.row, 'user_id' | 'name' | 'score'>
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string; name: string; score: number }> = true;
    const _check2: AssertAssignable<{ user_id: string; name: string; score: number }, Result> = true;
    expect(_check).toBe(true);
    expect(_check2).toBe(true);
  });

  test('compile() returns full RowType for SELECT * (no select() call)', () => {
    const query = qb.selectFrom('users').compile();

    expect(query.sql).toBe('SELECT *\nFROM users');

    // Type-level: result should be the full row type
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string; name: string; score: number; updated_at: string }> = true;
    const _check2: AssertAssignable<{ user_id: string; name: string; score: number; updated_at: string }, Result> = true;
    expect(_check).toBe(true);
    expect(_check2).toBe(true);
  });

  test('compile() includes Expression alias columns as unknown', () => {
    const query = qb
      .selectFrom('users')
      .select(['user_id', qb.fn.count().as('total')])
      .compile();

    expect(query.sql).toBe('SELECT user_id, count() AS total\nFROM users');

    // Type-level: result includes user_id (string) and total (unknown)
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string; total: unknown }> = true;
    expect(_check).toBe(true);
  });

  test('compile() with only Expression columns', () => {
    const query = qb
      .selectFrom('users')
      .select([qb.fn.count().as('total'), qb.fn.avg('score').as('avg_score')])
      .compile();

    expect(query.sql).toBe('SELECT count() AS total, avg(score) AS avg_score\nFROM users');

    // Type-level: both columns are unknown since they're expressions
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { total: unknown; avg_score: unknown }> = true;
    expect(_check).toBe(true);
  });

  test('compile() with single column', () => {
    const query = qb
      .selectFrom('users')
      .select(['user_id'])
      .compile();

    expect(query.sql).toBe('SELECT user_id\nFROM users');

    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string }> = true;
    const _check2: AssertAssignable<{ user_id: string }, Result> = true;
    expect(_check).toBe(true);
    expect(_check2).toBe(true);
  });

  test('CompiledQuery default is Record<string, unknown> for backwards compat', () => {
    const query: CompiledQuery = { sql: 'SELECT 1', params: {} };
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, Record<string, unknown>> = true;
    expect(_check).toBe(true);
  });

  test('SelectResult type helper works correctly', () => {
    // Schema columns only
    type R1 = SelectResult<TestDB, 'users', 'user_id' | 'name'>;
    const _check1: AssertAssignable<R1, { user_id: string; name: string }> = true;
    const _check1r: AssertAssignable<{ user_id: string; name: string }, R1> = true;

    // Expression alias (non-schema column)
    type R2 = SelectResult<TestDB, 'users', 'user_id' | 'total'>;
    const _check2: AssertAssignable<R2, { user_id: string; total: unknown }> = true;

    // Unnarrowed string — fallback
    type R3 = SelectResult<TestDB, 'users', string>;
    const _check3: AssertAssignable<R3, Record<string, unknown>> = true;

    expect(_check1).toBe(true);
    expect(_check1r).toBe(true);
    expect(_check2).toBe(true);
    expect(_check3).toBe(true);
  });

  test('chaining where/orderBy/limit does not affect result type', () => {
    const query = qb
      .selectFrom('users')
      .select(['user_id', 'name'])
      .where('score', '>', qb.param('min', 'Float64'))
      .orderBy('name', 'DESC')
      .limit(10)
      .compile();

    expect(query.sql).toContain('SELECT user_id, name');

    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string; name: string }> = true;
    const _check2: AssertAssignable<{ user_id: string; name: string }, Result> = true;
    expect(_check).toBe(true);
    expect(_check2).toBe(true);
  });

  test('different tables produce different result types', () => {
    const usersQuery = qb.selectFrom('users').select(['user_id', 'name']).compile();
    const eventsQuery = qb.selectFrom('events').select(['event_id', 'type']).compile();

    type UsersResult = typeof usersQuery extends CompiledQuery<infer R> ? R : never;
    type EventsResult = typeof eventsQuery extends CompiledQuery<infer R> ? R : never;

    const _checkUsers: AssertAssignable<UsersResult, { user_id: string; name: string }> = true;
    const _checkEvents: AssertAssignable<EventsResult, { event_id: string; type: string }> = true;

    expect(_checkUsers).toBe(true);
    expect(_checkEvents).toBe(true);
  });

  test('Expression.as() returns intersection type with narrowed alias', () => {
    const expr = qb.fn.count().as('total');
    // The alias should be the literal 'total', not just string
    type Alias = typeof expr extends { alias: infer A } ? A : never;
    const _check: AssertAssignable<Alias, 'total'> = true;
    expect(_check).toBe(true);
    expect(expr.alias).toBe('total');
  });

  test('fn.sum/count/avg automatically infer number type through .as()', () => {
    const query = qb
      .selectFrom('users')
      .select(['user_id', qb.fn.sum('score').as('total_score')])
      .compile();

    expect(query.sql).toContain('sum(score) AS total_score');

    // Type-level: total_score should be number automatically (fn.sum returns Expression<number>)
    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result, { user_id: string; total_score: number }> = true;
    const _check2: AssertAssignable<{ user_id: string; total_score: number }, Result> = true;
    expect(_check).toBe(true);
    expect(_check2).toBe(true);
  });

  test('Multiple typed aggregates infer correctly', () => {
    const query = qb
      .selectFrom('users')
      .select([
        qb.fn.count().as('total'),
        qb.fn.avg('score').as('avg_score'),
      ])
      .compile();

    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _checkTotal: AssertAssignable<Result['total'], number> = true;
    const _checkAvg: AssertAssignable<Result['avg_score'], number> = true;
    expect(_checkTotal).toBe(true);
    expect(_checkAvg).toBe(true);
  });

  test('fn.raw() still infers unknown', () => {
    const query = qb
      .selectFrom('users')
      .select([qb.fn.raw('custom_func(x)').as('val')])
      .compile();

    type Result = typeof query extends CompiledQuery<infer R> ? R : never;
    const _check: AssertAssignable<Result['val'], unknown> = true;
    expect(_check).toBe(true);
  });
});
