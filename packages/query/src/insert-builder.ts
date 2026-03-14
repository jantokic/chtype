/**
 * Type-safe INSERT query builder for ClickHouse.
 */

import type {
  CompiledQuery,
  DatabaseSchema,
  InsertType,
  TableName,
} from './types.js';

/**
 * INSERT query builder.
 *
 * Uses the generated Insert type to validate that:
 * - Required columns are present
 * - DEFAULT columns are optional
 * - MATERIALIZED/ALIAS columns are not included
 *
 * @example
 * ```ts
 * const insert = new InsertBuilder<MyDB>('users')
 *   .values([{ user_id: '123', name: 'Alice' }])
 *   .compile();
 * ```
 */
export class InsertBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
> {
  private _table: T;
  private _rows: InsertType<DB, T>[] = [];
  private _settings: Record<string, string | number | boolean> = {};

  constructor(table: T) {
    this._table = table;
  }

  /** Set the rows to insert. Type-checked against the table's Insert type. */
  values(rows: InsertType<DB, T>[]): this {
    this._rows = rows;
    return this;
  }

  /** Add SETTINGS clause. */
  settings(s: Record<string, string | number | boolean>): this {
    Object.assign(this._settings, s);
    return this;
  }

  /**
   * Compile the insert.
   *
   * Note: For bulk inserts, prefer using the client's `insert()` method
   * which uses ClickHouse's native insert protocol. This compile method
   * is mainly useful for generating INSERT ... SELECT or INSERT ... FORMAT queries.
   */
  compile(): CompiledQuery & { rows: InsertType<DB, T>[] } {
    const table = this._table as string;

    if (this._rows.length === 0) {
      return { sql: `INSERT INTO ${table}`, params: {}, rows: [] };
    }

    // Get column names from the first row
    const columns = Object.keys(this._rows[0] as Record<string, unknown>);

    let sql = `INSERT INTO ${table} (${columns.join(', ')})`;

    const settingsEntries = Object.entries(this._settings);
    if (settingsEntries.length > 0) {
      const settingsStr = settingsEntries
        .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
        .join(', ');
      sql += ` SETTINGS ${settingsStr}`;
    }

    return { sql, params: {}, rows: this._rows };
  }
}
