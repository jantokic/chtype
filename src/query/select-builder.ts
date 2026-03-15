/**
 * Type-safe SELECT query builder for ClickHouse.
 *
 * All values in WHERE/HAVING must be Param or Expression — raw string/number
 * literals are NOT allowed to prevent SQL injection. Use `qb.param()` to create
 * typed parameter placeholders.
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

/** Valid ClickHouse setting name pattern. */
const VALID_SETTING_KEY = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

interface WhereClause {
  column: string;
  op: ComparisonOp;
  value: Param | Expression;
}

interface OrderByClause {
  expr: string;
  direction: SortDirection;
}

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

  select<C extends ColumnName<DB, T>>(
    columns: (C | Expression)[],
  ): SelectBuilder<DB, T, C> {
    this._columns = columns;
    return this as unknown as SelectBuilder<DB, T, C>;
  }

  /** Add a WHERE condition. Values must be Param or Expression — no raw strings. */
  where(
    column: ColumnName<DB, T> | Expression,
    op: ComparisonOp,
    value: Param | Expression,
  ): this {
    const col = column instanceof Expression ? column.sql : column;
    this._wheres.push({ column: col, op, value });
    return this;
  }

  groupBy(...columns: (ColumnName<DB, T> | Expression)[]): this {
    this._groupBy.push(...columns.map((c) => (c instanceof Expression ? c.sql : c)));
    return this;
  }

  having(column: string | Expression, op: ComparisonOp, value: Param | Expression): this {
    const col = column instanceof Expression ? column.sql : column;
    this._havings.push({ column: col, op, value });
    return this;
  }

  orderBy(column: ColumnName<DB, T> | Expression | string, direction: SortDirection = 'ASC'): this {
    const expr = column instanceof Expression ? column.sql : column;
    this._orderBys.push({ expr, direction });
    return this;
  }

  limit(n: number | Param): this {
    this._limit = n;
    return this;
  }

  offset(n: number | Param): this {
    this._offset = n;
    return this;
  }

  /** Add the FINAL modifier (use sparingly — only for debug/audit). */
  final(): this {
    this._final = true;
    return this;
  }

  /** Add SETTINGS clause. Keys are validated to prevent injection. */
  settings(s: Record<string, string | number | boolean>): this {
    for (const key of Object.keys(s)) {
      if (!VALID_SETTING_KEY.test(key)) {
        throw new Error(`Invalid ClickHouse setting name: "${key}"`);
      }
    }
    Object.assign(this._settings, s);
    return this;
  }

  compile(): CompiledQuery {
    const params: Record<string, unknown> = {};
    const parts: string[] = [];

    const selectList =
      this._columns.length > 0
        ? this._columns.map((c) => (c instanceof Expression ? c.toString() : c)).join(', ')
        : '*';
    parts.push(`SELECT ${selectList}`);

    const finalMod = this._final ? ' FINAL' : '';
    parts.push(`FROM ${this._table as string}${finalMod}`);

    if (this._wheres.length > 0) {
      const conditions = this._wheres.map((w) => {
        const val = renderValue(w.value, params);
        return `${w.column} ${w.op} ${val}`;
      });
      parts.push(`WHERE ${conditions.join(' AND ')}`);
    }

    if (this._groupBy.length > 0) {
      parts.push(`GROUP BY ${this._groupBy.join(', ')}`);
    }

    if (this._havings.length > 0) {
      const conditions = this._havings.map((h) => {
        const val = renderValue(h.value, params);
        return `${h.column} ${h.op} ${val}`;
      });
      parts.push(`HAVING ${conditions.join(' AND ')}`);
    }

    if (this._orderBys.length > 0) {
      const clauses = this._orderBys.map((o) => `${o.expr} ${o.direction}`);
      parts.push(`ORDER BY ${clauses.join(', ')}`);
    }

    if (this._limit !== null) {
      parts.push(`LIMIT ${renderValue(this._limit, params)}`);
    }

    if (this._offset !== null) {
      parts.push(`OFFSET ${renderValue(this._offset, params)}`);
    }

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

function renderValue(value: number | Param | Expression, params: Record<string, unknown>): string {
  if (value instanceof Param) {
    params[value.name] = undefined;
    return value.toString();
  }
  if (value instanceof Expression) {
    return value.sql;
  }
  // Only numbers reach here (from limit/offset)
  return String(value);
}
