/**
 * Core type definitions for the query builder.
 *
 * These types enable compile-time validation of table names, column names,
 * and result types based on the generated Database schema.
 */

/** Minimal shape that a generated Database interface must satisfy. */
export interface DatabaseSchema {
  [tableName: string]: {
    row: Record<string, unknown>;
    insert: Record<string, unknown>;
    engine: string;
    versionColumn: string | null;
  };
}

/** Extract table names from a Database schema. */
export type TableName<DB extends DatabaseSchema> = keyof DB & string;

/** Extract the Row type for a given table. */
export type RowType<DB extends DatabaseSchema, T extends TableName<DB>> = DB[T]['row'];

/** Extract the Insert type for a given table. */
export type InsertType<DB extends DatabaseSchema, T extends TableName<DB>> = DB[T]['insert'];

/** Extract column names from a table's Row type. */
export type ColumnName<DB extends DatabaseSchema, T extends TableName<DB>> = keyof RowType<
  DB,
  T
> &
  string;

/** A compiled query ready for execution. */
export interface CompiledQuery<TResult = unknown> {
  /** The parameterized SQL string with {name:Type} placeholders. */
  sql: string;
  /** Parameter values to bind. */
  params: Record<string, unknown>;
  /** Phantom type marker for the expected result type. */
  _resultType?: TResult;
}

/** Sort direction. */
export type SortDirection = 'ASC' | 'DESC';

/** Comparison operators. */
export type ComparisonOp = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'IN' | 'NOT IN' | 'LIKE';
