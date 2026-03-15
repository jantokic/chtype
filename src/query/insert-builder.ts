import type { CompiledQuery, DatabaseSchema, InsertType, TableName } from './types.js';

export class InsertBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
> {
  private _table: T;
  private _rows: InsertType<DB, T>[] = [];

  constructor(table: T) {
    this._table = table;
  }

  values(rows: InsertType<DB, T>[]): this {
    this._rows = rows;
    return this;
  }

  /**
   * Compile the insert metadata.
   *
   * For bulk inserts, prefer using the client's `insert()` method
   * which uses ClickHouse's native insert protocol.
   */
  compile(): CompiledQuery & { table: string; rows: InsertType<DB, T>[] } {
    const table = this._table as string;

    if (this._rows.length === 0) {
      return { sql: `INSERT INTO ${table}`, params: {}, table, rows: [] };
    }

    const columns = Object.keys(this._rows[0] as Record<string, unknown>);
    const sql = `INSERT INTO ${table} (${columns.join(', ')})`;

    return { sql, params: {}, table, rows: this._rows };
  }
}
