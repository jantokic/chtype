/** Minimal shape that a generated Database interface must satisfy. */
export interface DatabaseSchema {
  [tableName: string]: {
    row: Record<string, unknown>;
    insert: Record<string, unknown>;
    engine: string;
    versionColumn: string | null;
  };
}

export type TableName<DB extends DatabaseSchema> = keyof DB & string;
export type RowType<DB extends DatabaseSchema, T extends TableName<DB>> = DB[T]['row'];
export type InsertType<DB extends DatabaseSchema, T extends TableName<DB>> = DB[T]['insert'];
export type ColumnName<DB extends DatabaseSchema, T extends TableName<DB>> = keyof RowType<DB, T> & string;

/** A compiled query ready for execution. TResult carries the inferred row type from selected columns. */
export interface CompiledQuery<TResult = Record<string, unknown>> {
  /** The parameterized SQL string with {name:Type} placeholders. */
  sql: string;
  /** Registry of parameter names that appear in the query. Values are undefined (filled at execution time). */
  params: Record<string, unknown>;
  /** Phantom field — never set at runtime, only used by TypeScript to carry the result type. */
  readonly _resultType?: TResult;
}

/** Compute the result row type from selected columns and expressions. */
export type SelectResult<DB extends DatabaseSchema, T extends TableName<DB>, TSelected extends string> =
  string extends TSelected
    ? Record<string, unknown>
    : Pick<RowType<DB, T>, TSelected & keyof RowType<DB, T>>
      & Record<Exclude<TSelected, keyof RowType<DB, T>>, unknown>;

export type SortDirection = 'ASC' | 'DESC';

/** Comparison operators for scalar values. */
export type ComparisonOp = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';

/** Set membership operators — require Array(...) param types. */
export type SetOp = 'IN' | 'NOT IN' | 'GLOBAL IN' | 'GLOBAL NOT IN';

/** Unary operators (no value argument). */
export type UnaryOp = 'IS NULL' | 'IS NOT NULL';

/** Range operators — require a [low, high] tuple. */
export type BetweenOp = 'BETWEEN' | 'NOT BETWEEN';

/** All WHERE operators. */
export type WhereOp = ComparisonOp | SetOp | UnaryOp | BetweenOp;

/** ClickHouse JOIN types. */
export type JoinType =
  | 'JOIN'
  | 'INNER JOIN'
  | 'LEFT JOIN'
  | 'RIGHT JOIN'
  | 'FULL JOIN'
  | 'CROSS JOIN'
  | 'LEFT OUTER JOIN'
  | 'RIGHT OUTER JOIN'
  | 'FULL OUTER JOIN'
  | 'ANY JOIN'
  | 'ANY LEFT JOIN'
  | 'ANY RIGHT JOIN'
  | 'ANY INNER JOIN'
  | 'ALL JOIN'
  | 'ALL LEFT JOIN'
  | 'ALL RIGHT JOIN'
  | 'ALL INNER JOIN'
  | 'ASOF JOIN'
  | 'ASOF LEFT JOIN';
