/**
 * Type-safe SELECT query builder for ClickHouse.
 *
 * All values in WHERE/HAVING must be Param or Expression — raw string/number
 * literals are NOT allowed to prevent SQL injection. Use `qb.param()` to create
 * typed parameter placeholders.
 */

import type {
  BetweenOp,
  ColumnName,
  CompiledQuery,
  ComparisonOp,
  DatabaseSchema,
  JoinType,
  RowType,
  SelectResult,
  SetOp,
  SortDirection,
  TableName,
  UnaryOp,
  WhereOp,
} from './types.js';
import { Expression, Subquery } from './expressions.js';
import { Param } from './param.js';
import {
  type WhereClause,
  createCompileContext,
  mergeParams,
  renderValue,
  renderWhereClause,
  VALID_IDENTIFIER,
} from './compile-utils.js';

interface JoinClause {
  type: JoinType;
  table: string;
  alias?: string;
  onLeft: string;
  onRight: string;
}

interface OrderByClause {
  expr: string;
  direction: SortDirection;
}

interface CteClause {
  name: string;
  subquery: Subquery;
}

export class SelectBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
  TSelected extends string = string,
> {
  private _table: T;
  private _tableAlias?: string;
  private _columns: (string | Expression)[] = [];
  private _distinct = false;
  private _ctes: CteClause[] = [];
  private _wheres: WhereClause[] = [];
  private _prewheres: WhereClause[] = [];
  private _joins: JoinClause[] = [];
  private _groupBy: string[] = [];
  private _havings: WhereClause[] = [];
  private _orderBys: OrderByClause[] = [];
  private _limit: number | Param | null = null;
  private _offset: number | Param | null = null;
  private _final = false;
  private _sample: number | null = null;
  private _sampleOffset: number | null = null;
  private _settings: Record<string, string | number | boolean> = {};

  constructor(table: T) {
    this._table = table;
  }

  /** Alias the main table (useful with JOINs). */
  as(alias: string): this {
    this._tableAlias = alias;
    return this;
  }

  /** Add DISTINCT to the SELECT clause. */
  distinct(): this {
    this._distinct = true;
    return this;
  }

  /** Add a WITH (CTE) clause. The subquery is compiled and prepended to the query. */
  with(name: string, subquery: Subquery | { compile(): CompiledQuery<unknown> }): this {
    if (!VALID_IDENTIFIER.test(name)) {
      throw new Error(`Invalid CTE name: "${name}"`);
    }
    const sub = subquery instanceof Subquery ? subquery : new Subquery(subquery.compile());
    this._ctes.push({ name, subquery: sub });
    return this;
  }

  /** Add a PREWHERE condition (ClickHouse-specific optimization, applied before data is read from disk). */
  prewhere(
    column: ColumnName<DB, T> | Expression | string,
    op: ComparisonOp,
    value: Param | Expression,
  ): this;
  prewhere(
    column: ColumnName<DB, T> | Expression | string,
    op: SetOp,
    value: Param | Expression,
  ): this;
  prewhere(
    column: ColumnName<DB, T> | Expression | string,
    op: UnaryOp,
  ): this;
  prewhere(
    column: ColumnName<DB, T> | Expression | string,
    op: BetweenOp,
    value: [Param | Expression, Param | Expression],
  ): this;
  prewhere(condition: Expression): this;
  prewhere(
    columnOrCondition: ColumnName<DB, T> | Expression | string,
    op?: WhereOp,
    value?: Param | Expression | [Param | Expression, Param | Expression],
  ): this {
    this._prewheres.push(buildWhereClause(columnOrCondition, op, value));
    return this;
  }

  select<C extends ColumnName<DB, T>, E extends Expression & { alias: string }>(
    columns: (C | E)[],
  ): SelectBuilder<DB, T, C | (E extends { alias: infer A extends string } ? A : never)> {
    this._columns = columns;
    return this as unknown as SelectBuilder<DB, T, C | (E extends { alias: infer A extends string } ? A : never)>;
  }

  /** Add a WHERE condition. Values must be Param or Expression — no raw strings. */
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: ComparisonOp,
    value: Param | Expression,
  ): this;
  /** Add a WHERE IN / NOT IN condition. Value must be an Array(...) Param. */
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: SetOp,
    value: Param | Expression,
  ): this;
  /** Add a WHERE IS NULL / IS NOT NULL condition (no value). */
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: UnaryOp,
  ): this;
  /** Add a WHERE BETWEEN condition with a [low, high] range. */
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: BetweenOp,
    value: [Param | Expression, Param | Expression],
  ): this;
  /** Add a pre-built condition group (from `or()` / `and()`). */
  where(condition: Expression): this;
  where(
    columnOrCondition: ColumnName<DB, T> | Expression | string,
    op?: WhereOp,
    value?: Param | Expression | [Param | Expression, Param | Expression],
  ): this {
    this._wheres.push(buildWhereClause(columnOrCondition, op, value));
    return this;
  }

  /**
   * Add a JOIN clause.
   *
   * @example
   * ```ts
   * qb.selectFrom('orders')
   *   .join('INNER JOIN', 'users', 'u', 'orders.user_id', 'u.user_id')
   *   .select([...])
   * ```
   */
  join(
    type: JoinType,
    table: string,
    alias: string | undefined,
    onLeft: string,
    onRight: string,
  ): this {
    this._joins.push({ type, table, alias, onLeft, onRight });
    return this;
  }

  /** Shorthand for INNER JOIN. */
  innerJoin(table: string, alias: string | undefined, onLeft: string, onRight: string): this {
    return this.join('INNER JOIN', table, alias, onLeft, onRight);
  }

  /** Shorthand for LEFT JOIN. */
  leftJoin(table: string, alias: string | undefined, onLeft: string, onRight: string): this {
    return this.join('LEFT JOIN', table, alias, onLeft, onRight);
  }

  /** Shorthand for RIGHT JOIN. */
  rightJoin(table: string, alias: string | undefined, onLeft: string, onRight: string): this {
    return this.join('RIGHT JOIN', table, alias, onLeft, onRight);
  }

  /** Shorthand for CROSS JOIN (no ON clause — onLeft/onRight are ignored). */
  crossJoin(table: string, alias?: string): this {
    this._joins.push({ type: 'CROSS JOIN', table, alias, onLeft: '', onRight: '' });
    return this;
  }

  /** Shorthand for ANY LEFT JOIN (ClickHouse-specific). */
  anyLeftJoin(table: string, alias: string | undefined, onLeft: string, onRight: string): this {
    return this.join('ANY LEFT JOIN', table, alias, onLeft, onRight);
  }

  groupBy(...columns: (ColumnName<DB, T> | Expression | string)[]): this {
    this._groupBy.push(...columns.map((c) => (c instanceof Expression ? c.sql : c)));
    return this;
  }

  having(column: string | Expression, op: ComparisonOp, value: Param | Expression): this;
  having(condition: Expression): this;
  having(
    columnOrCondition: string | Expression,
    op?: ComparisonOp,
    value?: Param | Expression,
  ): this {
    if (columnOrCondition instanceof Expression && op === undefined) {
      this._havings.push({ kind: 'expression', expr: columnOrCondition });
      return this;
    }
    const col = columnOrCondition instanceof Expression ? columnOrCondition.sql : columnOrCondition;
    this._havings.push({ kind: 'comparison', column: col, op: op!, value: value! });
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

  /**
   * Add SAMPLE clause (ClickHouse-specific approximate query optimization).
   * @param ratio — fraction (0-1) or absolute row count (>= 1)
   * @param offset — optional OFFSET for reproducible sampling (0-1)
   */
  sample(ratio: number, offset?: number): this {
    this._sample = ratio;
    if (offset !== undefined) {
      this._sampleOffset = offset;
    }
    return this;
  }

  /** Add SETTINGS clause. Keys and string values are validated to prevent injection. */
  settings(s: Record<string, string | number | boolean>): this {
    for (const [key, val] of Object.entries(s)) {
      if (!VALID_IDENTIFIER.test(key)) {
        throw new Error(`Invalid ClickHouse setting name: "${key}"`);
      }
      if (typeof val === 'string' && val.includes("'")) {
        throw new Error(`Setting value for "${key}" contains invalid character: '`);
      }
    }
    Object.assign(this._settings, s);
    return this;
  }

  compile(): CompiledQuery<string extends TSelected ? RowType<DB, T> : SelectResult<DB, T, TSelected>> {
    const ctx = createCompileContext();
    const parts: string[] = [];

    // WITH (CTE) clauses
    if (this._ctes.length > 0) {
      const cteParts = this._ctes.map((cte) => {
        mergeParams(ctx, cte.subquery.subqueryParams);
        return `${cte.name} AS ${cte.subquery.sql}`;
      });
      parts.push(`WITH ${cteParts.join(',\n')}`);
    }

    const selectList =
      this._columns.length > 0
        ? this._columns.map((c) => (c instanceof Expression ? c.toString() : c)).join(', ')
        : '*';
    const distinctMod = this._distinct ? 'DISTINCT ' : '';
    parts.push(`SELECT ${distinctMod}${selectList}`);

    const tableName = this._table as string;
    const tableRef = this._tableAlias ? `${tableName} AS ${this._tableAlias}` : tableName;
    const finalMod = this._final ? ' FINAL' : '';
    const sampleMod = this._sample !== null
      ? this._sampleOffset !== null
        ? ` SAMPLE ${this._sample} OFFSET ${this._sampleOffset}`
        : ` SAMPLE ${this._sample}`
      : '';
    parts.push(`FROM ${tableRef}${finalMod}${sampleMod}`);

    for (const j of this._joins) {
      const joinTable = j.alias ? `${j.table} AS ${j.alias}` : j.table;
      if (j.type === 'CROSS JOIN') {
        parts.push(`CROSS JOIN ${joinTable}`);
      } else {
        parts.push(`${j.type} ${joinTable} ON ${j.onLeft} = ${j.onRight}`);
      }
    }

    // PREWHERE (ClickHouse-specific, before WHERE)
    if (this._prewheres.length > 0) {
      const conditions = this._prewheres.map((w) => renderWhereClause(w, ctx));
      parts.push(`PREWHERE ${conditions.join(' AND ')}`);
    }

    if (this._wheres.length > 0) {
      const conditions = this._wheres.map((w) => renderWhereClause(w, ctx));
      parts.push(`WHERE ${conditions.join(' AND ')}`);
    }

    if (this._groupBy.length > 0) {
      parts.push(`GROUP BY ${this._groupBy.join(', ')}`);
    }

    if (this._havings.length > 0) {
      const conditions = this._havings.map((h) => renderWhereClause(h, ctx));
      parts.push(`HAVING ${conditions.join(' AND ')}`);
    }

    if (this._orderBys.length > 0) {
      const clauses = this._orderBys.map((o) => `${o.expr} ${o.direction}`);
      parts.push(`ORDER BY ${clauses.join(', ')}`);
    }

    if (this._limit !== null) {
      parts.push(`LIMIT ${renderValue(this._limit, ctx)}`);
    }

    if (this._offset !== null) {
      parts.push(`OFFSET ${renderValue(this._offset, ctx)}`);
    }

    const settingsEntries = Object.entries(this._settings);
    if (settingsEntries.length > 0) {
      const settingsStr = settingsEntries
        .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
        .join(', ');
      parts.push(`SETTINGS ${settingsStr}`);
    }

    return { sql: parts.join('\n'), params: ctx.params };
  }
}

type SetOperator = 'UNION ALL' | 'UNION DISTINCT' | 'INTERSECT' | 'EXCEPT';

/** Combine multiple SELECT queries with a set operator (UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT). */
export function setOperation<T = Record<string, unknown>>(
  operator: SetOperator,
  ...queries: { compile(): CompiledQuery<T> }[]
): CompiledQuery<T> {
  if (queries.length < 2) {
    throw new Error(`${operator} requires at least two queries`);
  }

  const compiled = queries.map((q) => q.compile());
  const params: Record<string, unknown> = {};
  const seen = new Set<string>();

  for (const c of compiled) {
    for (const key of Object.keys(c.params)) {
      if (seen.has(key)) {
        throw new Error(`Param name collision: "${key}" appears in multiple ${operator} branches`);
      }
      seen.add(key);
      params[key] = c.params[key];
    }
  }

  const sql = compiled.map((c) => c.sql).join(`\n${operator}\n`);
  return { sql, params };
}

/** Shorthand for UNION ALL. */
export function unionAll<T = Record<string, unknown>>(...queries: { compile(): CompiledQuery<T> }[]): CompiledQuery<T> {
  return setOperation('UNION ALL', ...queries);
}

/** Shorthand for UNION DISTINCT. */
export function unionDistinct<T = Record<string, unknown>>(...queries: { compile(): CompiledQuery<T> }[]): CompiledQuery<T> {
  return setOperation('UNION DISTINCT', ...queries);
}

/** Shorthand for INTERSECT. */
export function intersect<T = Record<string, unknown>>(...queries: { compile(): CompiledQuery<T> }[]): CompiledQuery<T> {
  return setOperation('INTERSECT', ...queries);
}

/** Shorthand for EXCEPT. */
export function except<T = Record<string, unknown>>(...queries: { compile(): CompiledQuery<T> }[]): CompiledQuery<T> {
  return setOperation('EXCEPT', ...queries);
}

/** Parse where() arguments into a WhereClause. Shared by where() and prewhere(). */
function buildWhereClause(
  columnOrCondition: Expression | string,
  op?: WhereOp,
  value?: Param | Expression | [Param | Expression, Param | Expression],
): WhereClause {
  if (columnOrCondition instanceof Expression && op === undefined) {
    return { kind: 'expression', expr: columnOrCondition };
  }
  const col = columnOrCondition instanceof Expression ? columnOrCondition.sql : (columnOrCondition as string);
  if (op === 'IS NULL' || op === 'IS NOT NULL') {
    return { kind: 'unary', column: col, op };
  }
  if (op === 'BETWEEN' || op === 'NOT BETWEEN') {
    if (!Array.isArray(value) || value.length < 2) {
      throw new Error(`${op} requires a [low, high] tuple`);
    }
    return { kind: 'between', column: col, op, low: value[0]!, high: value[1]! };
  }
  return { kind: 'comparison', column: col, op: op as ComparisonOp | SetOp, value: value as Param | Expression };
}
