/**
 * Basic chtype example.
 *
 * Shows how the generated schema types work with the query builder
 * to provide full type safety and IDE autocomplete.
 */

import { createQueryBuilder } from '@chtype/query';
import type { Database } from './schema.generated.js';

const qb = createQueryBuilder<Database>();

// ─── Example 1: Basic SELECT with type-safe columns ──────────────────

const basicQuery = qb
  .selectFrom('users')
  .select(['user_id', 'name', 'score'])
  .where('score', '>', qb.param('minScore', 'Float64'))
  .orderBy('score', 'DESC')
  .limit(20)
  .compile();

console.log('--- Basic SELECT ---');
console.log(basicQuery.sql);
// SELECT user_id, name, score
// FROM users
// WHERE score > {minScore:Float64}
// ORDER BY score DESC
// LIMIT 20

// ─── Example 2: argMax for ReplacingMergeTree ────────────────────────

const rmtQuery = qb
  .selectFrom('users')
  .select([
    'user_id',
    qb.fn.argMax('name', 'updated_at').as('name'),
    qb.fn.argMax('score', 'updated_at').as('score'),
    qb.fn.argMax('tags', 'updated_at').as('tags'),
  ])
  .groupBy('user_id')
  .orderBy(qb.fn.argMax('score', 'updated_at'), 'DESC')
  .limit(qb.param('limit', 'UInt32'))
  .compile();

console.log('\n--- ReplacingMergeTree argMax ---');
console.log(rmtQuery.sql);

// ─── Example 3: Aggregation query ───────────────────────────────────

const aggQuery = qb
  .selectFrom('events')
  .select([
    'type',
    qb.fn.count().as('event_count'),
    qb.fn.countDistinct('user_id').as('unique_users'),
  ])
  .where('timestamp', '>', qb.param('since', 'DateTime'))
  .groupBy('type')
  .having('event_count', '>', 100)
  .orderBy('event_count', 'DESC')
  .compile();

console.log('\n--- Aggregation ---');
console.log(aggQuery.sql);

// ─── Example 4: FINAL + SETTINGS (debug mode) ──────────────────────

const debugQuery = qb
  .selectFrom('users')
  .select(['user_id', 'name', 'score'])
  .final()
  .settings({ max_execution_time: 60 })
  .compile();

console.log('\n--- Debug with FINAL ---');
console.log(debugQuery.sql);
