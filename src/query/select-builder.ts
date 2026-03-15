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
  SetOp,
  SortDirection,
  TableName,
  UnaryOp,
  WhereOp,
} from './types.js';
import { ConditionGroup, Expression, Subquery } from './expressions.js';
import { Param } from './param.js';

/** Valid SQL identifier pattern (settings keys, CTE names, etc.). */
const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

type WhereClause =
  | { kind: 'comparison'; column: string; op: ComparisonOp | SetOp; value: Param | Expression }
  | { kind: 'unary'; column: string; op: UnaryOp }
  | { kind: 'between'; column: string; op: BetweenOp; low: Param | Expression; high: Param | Expression }
  | { kind: 'expression'; expr: Expression };

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
  with(name: string, subquery: Subquery | { compile(): CompiledQuery }): this {
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
    // Reuse the same logic as where(), but push to _prewheres
    if (columnOrCondition instanceof Expression && op === undefined) {
      this._prewheres.push({ kind: 'expression', expr: columnOrCondition });
      return this;
    }
    const col = columnOrCondition instanceof Expression ? columnOrCondition.sql : (columnOrCondition as string);
    if (op === 'IS NULL' || op === 'IS NOT NULL') {
      this._prewheres.push({ kind: 'unary', column: col, op });
      return this;
    }
    if (op === 'BETWEEN' || op === 'NOT BETWEEN') {
      if (!Array.isArray(value) || value.length < 2) {
        throw new Error(`${op} requires a [low, high] tuple`);
      }
      this._prewheres.push({ kind: 'between', column: col, op, low: value[0]!, high: value[1]! });
      return this;
    }
    this._prewheres.push({ kind: 'comparison', column: col, op: op as ComparisonOp | SetOp, value: value as Param | Expression });
    return this;
  }

  select<C extends ColumnName<DB, T>>(
    columns: (C | Expression)[],
  ): SelectBuilder<DB, T, C> {
    this._columns = columns;
    return this as unknown as SelectBuilder<DB, T, C>;
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
    // Expression-only overload: where(or(...)) or where(and(...))
    if (columnOrCondition instanceof Expression && op === undefined) {
      this._wheres.push({ kind: 'expression', expr: columnOrCondition });
      return this;
    }

    const col = columnOrCondition instanceof Expression ? columnOrCondition.sql : (columnOrCondition as string);

    // Unary: IS NULL / IS NOT NULL
    if (op === 'IS NULL' || op === 'IS NOT NULL') {
      this._wheres.push({ kind: 'unary', column: col, op });
      return this;
    }

    // Between: BETWEEN / NOT BETWEEN
    if (op === 'BETWEEN' || op === 'NOT BETWEEN') {
      if (!Array.isArray(value) || value.length < 2) {
        throw new Error(`${op} requires a [low, high] tuple`);
      }
      this._wheres.push({ kind: 'between', column: col, op, low: value[0]!, high: value[1]! });
      return this;
    }

    // Standard comparison or set op
    this._wheres.push({ kind: 'comparison', column: col, op: op as ComparisonOp | SetOp, value: value as Param | Expression });
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

  compile(): CompiledQuery {
    const ctx: CompileContext = { params: {}, externalParams: new Set() };
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
    parts.push(`FROM ${tableRef}${finalMod}`);

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

/** Tracks params and their origin during compilation. */
interface CompileContext {
  params: Record<string, unknown>;
  /** Param names that came from subqueries/CTEs (external sources). */
  externalParams: Set<string>;
}

function mergeParams(ctx: CompileContext, source: Record<string, unknown>): void {
  for (const key of Object.keys(source)) {
    if (key in ctx.params) {
      throw new Error(`Param name collision: "${key}" is used in both the subquery/CTE and outer query`);
    }
    ctx.params[key] = source[key];
    ctx.externalParams.add(key);
  }
}

function renderValue(value: number | Param | Expression, ctx: CompileContext): string {
  if (value instanceof Subquery) {
    mergeParams(ctx, value.subqueryParams);
    return value.sql;
  }
  if (value instanceof Param) {
    if (ctx.externalParams.has(value.name)) {
      throw new Error(`Param name collision: "${value.name}" is used in both the subquery/CTE and outer query`);
    }
    ctx.params[value.name] = undefined;
    return value.toString();
  }
  if (value instanceof Expression) {
    return value.sql;
  }
  // Only numbers reach here (from limit/offset)
  return String(value);
}

function renderWhereClause(w: WhereClause, ctx: CompileContext): string {
  switch (w.kind) {
    case 'comparison': {
      const val = renderValue(w.value, ctx);
      return `${w.column} ${w.op} ${val}`;
    }
    case 'unary':
      return `${w.column} ${w.op}`;
    case 'between': {
      const low = renderValue(w.low, ctx);
      const high = renderValue(w.high, ctx);
      return `${w.column} ${w.op} ${low} AND ${high}`;
    }
    case 'expression': {
      if (w.expr instanceof ConditionGroup) {
        for (const p of w.expr.params) {
          if (ctx.externalParams.has(p.name)) {
            throw new Error(`Param name collision: "${p.name}" is used in both the subquery/CTE and outer query`);
          }
          ctx.params[p.name] = undefined;
        }
      }
      return w.expr.sql;
    }
  }
}
