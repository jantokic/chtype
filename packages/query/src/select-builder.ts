/**
 * Type-safe SELECT query builder for ClickHouse.
 *
 * Provides a fluent API that validates table names, column names,
 * and result types at compile time using the generated Database schema.
 */

import type {
  ColumnName,
  CompiledQuery,
  ComparisonOp,
  DatabaseSchema,
  SortDirection,
  TableName,
} from './types.js';
import { Expression } from './expressions.js';
import { Param } from './param.js';

interface WhereClause {
  column: string;
  op: ComparisonOp;
  value: string | number | Param | Expression;
}

interface OrderByClause {
  expr: string;
  direction: SortDirection;
}

/**
 * SELECT query builder.
 *
 * @example
 * ```ts
 * const query = new SelectBuilder<MyDB>('users')
 *   .select(['user_id', 'name'])
 *   .where('user_id', '=', param('id', 'String'))
 *   .orderBy('name', 'ASC')
 *   .limit(10)
 *   .compile();
 * ```
 */
export class SelectBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
  TSelected extends string = string,
> {
  private _table: T;
  private _columns: (string | Expression)[] = [];
  private _wheres: WhereClause[] = [];
  private _groupBy: string[] = [];
  private _havings: WhereClause[] = [];
  private _orderBys: OrderByClause[] = [];
  private _limit: number | Param | null = null;
  private _offset: number | Param | null = null;
  private _final = false;
  private _settings: Record<string, string | number | boolean> = {};

  constructor(table: T) {
    this._table = table;
  }

  /** Select specific columns. Column names are type-checked against the schema. */
  select<C extends ColumnName<DB, T>>(
    columns: (C | Expression)[],
  ): SelectBuilder<DB, T, C> {
    this._columns = columns;
    return this as unknown as SelectBuilder<DB, T, C>;
  }

  /** Add a WHERE condition. */
  where(
    column: ColumnName<DB, T> | Expression,
    op: ComparisonOp,
    value: string | number | Param | Expression,
  ): this {
    const col = column instanceof Expression ? column.sql : column;
    this._wheres.push({ column: col, op, value });
    return this;
  }

  /** Add a GROUP BY clause. */
  groupBy(...columns: (ColumnName<DB, T> | string)[]): this {
    this._groupBy.push(...columns);
    return this;
  }

  /** Add a HAVING condition (for use after GROUP BY). */
  having(
    column: string | Expression,
    op: ComparisonOp,
    value: string | number | Param | Expression,
  ): this {
    const col = column instanceof Expression ? column.sql : column;
    this._havings.push({ column: col, op, value });
    return this;
  }

  /** Add an ORDER BY clause. */
  orderBy(column: ColumnName<DB, T> | Expression | string, direction: SortDirection = 'ASC'): this {
    const expr = column instanceof Expression ? column.sql : column;
    this._orderBys.push({ expr, direction });
    return this;
  }

  /** Set the LIMIT. */
  limit(n: number | Param): this {
    this._limit = n;
    return this;
  }

  /** Set the OFFSET. */
  offset(n: number | Param): this {
    this._offset = n;
    return this;
  }

  /** Add the FINAL modifier (use sparingly — only for debug/audit). */
  final(): this {
    this._final = true;
    return this;
  }

  /** Add SETTINGS clause. */
  settings(s: Record<string, string | number | boolean>): this {
    Object.assign(this._settings, s);
    return this;
  }

  /** Compile the builder into a parameterized SQL string. */
  compile(): CompiledQuery {
    const params: Record<string, unknown> = {};
    const parts: string[] = [];

    // SELECT
    const selectList =
      this._columns.length > 0
        ? this._columns.map((c) => (c instanceof Expression ? c.toString() : c)).join(', ')
        : '*';
    parts.push(`SELECT ${selectList}`);

    // FROM
    const finalMod = this._final ? ' FINAL' : '';
    parts.push(`FROM ${this._table as string}${finalMod}`);

    // WHERE
    if (this._wheres.length > 0) {
      const conditions = this._wheres.map((w) => {
        const val = renderValue(w.value, params);
        return `${w.column} ${w.op} ${val}`;
      });
      parts.push(`WHERE ${conditions.join(' AND ')}`);
    }

    // GROUP BY
    if (this._groupBy.length > 0) {
      parts.push(`GROUP BY ${this._groupBy.join(', ')}`);
    }

    // HAVING
    if (this._havings.length > 0) {
      const conditions = this._havings.map((h) => {
        const val = renderValue(h.value, params);
        return `${h.column} ${h.op} ${val}`;
      });
      parts.push(`HAVING ${conditions.join(' AND ')}`);
    }

    // ORDER BY
    if (this._orderBys.length > 0) {
      const clauses = this._orderBys.map((o) => `${o.expr} ${o.direction}`);
      parts.push(`ORDER BY ${clauses.join(', ')}`);
    }

    // LIMIT
    if (this._limit !== null) {
      parts.push(`LIMIT ${renderValue(this._limit, params)}`);
    }

    // OFFSET
    if (this._offset !== null) {
      parts.push(`OFFSET ${renderValue(this._offset, params)}`);
    }

    // SETTINGS
    const settingsEntries = Object.entries(this._settings);
    if (settingsEntries.length > 0) {
      const settingsStr = settingsEntries
        .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
        .join(', ');
      parts.push(`SETTINGS ${settingsStr}`);
    }

    return { sql: parts.join('\n'), params };
  }
}

/** Render a value as a SQL fragment, collecting params as needed. */
function renderValue(
  value: string | number | Param | Expression,
  params: Record<string, unknown>,
): string {
  if (value instanceof Param) {
    params[value.name] = undefined; // placeholder — filled at execution time
    return value.toString();
  }
  if (value instanceof Expression) {
    return value.sql;
  }
  if (typeof value === 'number') {
    return String(value);
  }
  // String literal — use as-is (for enum/constant values validated before calling)
  return `'${value}'`;
}
