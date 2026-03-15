import type { CompiledQuery, DatabaseSchema, TableName } from './types.js';
import { type ClickHouseParamType, Param } from './param.js';
import { SelectBuilder } from './select-builder.js';
import { InsertBuilder } from './insert-builder.js';
import { fn, Subquery } from './expressions.js';

export interface QueryBuilder<DB extends DatabaseSchema> {
  selectFrom<T extends TableName<DB>>(table: T): SelectBuilder<DB, T>;
  insertInto<T extends TableName<DB>>(table: T): InsertBuilder<DB, T>;
  param(name: string, type: ClickHouseParamType): Param;
  /** Wrap a compiled query as a subquery expression for use in WHERE IN / NOT IN. */
  subquery(builder: { compile(): CompiledQuery }): Subquery;
  fn: typeof fn;
}

/**
 * Create a type-safe query builder for your ClickHouse database.
 *
 * @example
 * ```ts
 * import { createQueryBuilder } from 'chtype/query';
 * import type { Database } from './chtype.generated';
 *
 * const qb = createQueryBuilder<Database>();
 *
 * const query = qb
 *   .selectFrom('users')
 *   .select(['user_id', 'name'])
 *   .where('user_id', '=', qb.param('id', 'String'))
 *   .compile();
 * ```
 */
export function createQueryBuilder<DB extends DatabaseSchema>(): QueryBuilder<DB> {
  return {
    selectFrom<T extends TableName<DB>>(table: T) {
      return new SelectBuilder<DB, T>(table);
    },
    insertInto<T extends TableName<DB>>(table: T) {
      return new InsertBuilder<DB, T>(table);
    },
    param(name: string, type: ClickHouseParamType) {
      return new Param(name, type);
    },
    subquery(builder: { compile(): CompiledQuery }) {
      return new Subquery(builder.compile());
    },
    fn,
  };
}
