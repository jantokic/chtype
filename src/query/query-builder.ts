import type { CompiledQuery, DatabaseSchema, TableName } from './types.js';
import { type ClickHouseParamType, Param } from './param.js';
import { SelectBuilder } from './select-builder.js';
import { InsertBuilder } from './insert-builder.js';
import { DeleteBuilder } from './delete-builder.js';
import { UpdateBuilder } from './update-builder.js';
import { fn, Subquery } from './expressions.js';
import { VALID_IDENTIFIER } from './compile-utils.js';

/** Schema entry for CTE tables — columns are untyped (Record<string, unknown>). */
type CteSchema = {
  row: Record<string, unknown>;
  insert: Record<string, unknown>;
  engine: 'CTE';
  versionColumn: null;
};

export interface QueryBuilder<DB extends DatabaseSchema> {
  selectFrom<T extends TableName<DB>>(table: T): SelectBuilder<DB, T>;
  insertInto<T extends TableName<DB>>(table: T): InsertBuilder<DB, T>;
  deleteFrom<T extends TableName<DB>>(table: T): DeleteBuilder<DB, T>;
  update<T extends TableName<DB>>(table: T): UpdateBuilder<DB, T>;
  param(name: string, type: ClickHouseParamType): Param;
  /** Wrap a compiled query as a subquery expression for use in WHERE IN / NOT IN. */
  subquery(builder: { compile(): CompiledQuery<unknown> }): Subquery;
  /** Start a CTE chain. Returns a builder where selectFrom() accepts the CTE name. */
  with<N extends string>(
    name: N,
    builder: Subquery | { compile(): CompiledQuery<unknown> },
  ): WithBuilder<DB & Record<N, CteSchema>>;
  fn: typeof fn;
}

/** Builder returned by qb.with() — accumulates CTEs then creates a SelectBuilder. */
export interface WithBuilder<DB extends DatabaseSchema> {
  /** Add another CTE to the chain. */
  with<N extends string>(
    name: N,
    builder: Subquery | { compile(): CompiledQuery<unknown> },
  ): WithBuilder<DB & Record<N, CteSchema>>;
  /** Select from a table or CTE name. CTEs defined via with() are valid table names. */
  selectFrom<T extends TableName<DB>>(table: T): SelectBuilder<DB, T>;
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
    deleteFrom<T extends TableName<DB>>(table: T) {
      return new DeleteBuilder<DB, T>(table);
    },
    update<T extends TableName<DB>>(table: T) {
      return new UpdateBuilder<DB, T>(table);
    },
    param(name: string, type: ClickHouseParamType) {
      return new Param(name, type);
    },
    subquery(builder: { compile(): CompiledQuery<unknown> }) {
      return new Subquery(builder.compile());
    },
    with<N extends string>(
      name: N,
      builder: Subquery | { compile(): CompiledQuery<unknown> },
    ) {
      return createWithBuilder<DB & Record<N, CteSchema>>(name, builder, []);
    },
    fn,
  };
}

interface CteEntry {
  name: string;
  subquery: Subquery;
}

function createWithBuilder<DB extends DatabaseSchema>(
  name: string,
  builder: Subquery | { compile(): CompiledQuery<unknown> },
  existingCtes: CteEntry[],
): WithBuilder<DB> {
  if (!VALID_IDENTIFIER.test(name)) {
    throw new Error(`Invalid CTE name: "${name}"`);
  }
  const sub = builder instanceof Subquery ? builder : new Subquery(builder.compile());
  const ctes: CteEntry[] = [...existingCtes, { name, subquery: sub }];

  return {
    with<N extends string>(
      nextName: N,
      nextBuilder: Subquery | { compile(): CompiledQuery<unknown> },
    ) {
      return createWithBuilder<DB & Record<N, CteSchema>>(nextName, nextBuilder, ctes);
    },
    selectFrom<T extends TableName<DB>>(table: T): SelectBuilder<DB, T> {
      const sb = new SelectBuilder<DB, T>(table);
      for (const cte of ctes) {
        sb.with(cte.name, cte.subquery);
      }
      return sb;
    },
  };
}
