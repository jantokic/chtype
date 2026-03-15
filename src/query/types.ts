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

/** A compiled query ready for execution. */
export interface CompiledQuery {
  /** The parameterized SQL string with {name:Type} placeholders. */
  sql: string;
  /** Registry of parameter names that appear in the query. Values are undefined (filled at execution time). */
  params: Record<string, unknown>;
}

export type SortDirection = 'ASC' | 'DESC';

/** Comparison operators. IN/NOT IN require Array param types. */
export type ComparisonOp = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE';
