/**
 * Type-safe DELETE builder for ClickHouse.
 *
 * ClickHouse uses ALTER TABLE ... DELETE WHERE syntax.
 * All values in WHERE must be Param or Expression — no raw strings.
 */

import type {
  BetweenOp,
  ColumnName,
  CompiledQuery,
  ComparisonOp,
  DatabaseSchema,
  SetOp,
  TableName,
  UnaryOp,
  WhereOp,
} from './types.js';
import { ConditionGroup, Expression, Subquery } from './expressions.js';
import { Param } from './param.js';

/** Valid SQL identifier pattern. */
const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

type WhereClause =
  | { kind: 'comparison'; column: string; op: ComparisonOp | SetOp; value: Param | Expression }
  | { kind: 'unary'; column: string; op: UnaryOp }
  | { kind: 'between'; column: string; op: BetweenOp; low: Param | Expression; high: Param | Expression }
  | { kind: 'expression'; expr: Expression };

interface CompileContext {
  params: Record<string, unknown>;
  externalParams: Set<string>;
}

export class DeleteBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
> {
  private _table: T;
  private _cluster?: string;
  private _wheres: WhereClause[] = [];

  constructor(table: T) {
    this._table = table;
  }

  /** Specify the cluster for distributed deletes: ALTER TABLE ... ON CLUSTER ... */
  onCluster(cluster: string): this {
    if (!VALID_IDENTIFIER.test(cluster)) {
      throw new Error(`Invalid cluster name: "${cluster}"`);
    }
    this._cluster = cluster;
    return this;
  }

  where(
    column: ColumnName<DB, T> | Expression | string,
    op: ComparisonOp,
    value: Param | Expression,
  ): this;
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: SetOp,
    value: Param | Expression,
  ): this;
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: UnaryOp,
  ): this;
  where(
    column: ColumnName<DB, T> | Expression | string,
    op: BetweenOp,
    value: [Param | Expression, Param | Expression],
  ): this;
  where(condition: Expression): this;
  where(
    columnOrCondition: ColumnName<DB, T> | Expression | string,
    op?: WhereOp,
    value?: Param | Expression | [Param | Expression, Param | Expression],
  ): this {
    if (columnOrCondition instanceof Expression && op === undefined) {
      this._wheres.push({ kind: 'expression', expr: columnOrCondition });
      return this;
    }
    const col = columnOrCondition instanceof Expression ? columnOrCondition.sql : (columnOrCondition as string);
    if (op === 'IS NULL' || op === 'IS NOT NULL') {
      this._wheres.push({ kind: 'unary', column: col, op });
      return this;
    }
    if (op === 'BETWEEN' || op === 'NOT BETWEEN') {
      if (!Array.isArray(value) || value.length < 2) {
        throw new Error(`${op} requires a [low, high] tuple`);
      }
      this._wheres.push({ kind: 'between', column: col, op, low: value[0]!, high: value[1]! });
      return this;
    }
    this._wheres.push({ kind: 'comparison', column: col, op: op as ComparisonOp | SetOp, value: value as Param | Expression });
    return this;
  }

  compile(): CompiledQuery {
    if (this._wheres.length === 0) {
      throw new Error('DELETE requires at least one WHERE condition');
    }

    const ctx: CompileContext = { params: {}, externalParams: new Set() };
    const table = this._table as string;
    const clusterClause = this._cluster ? ` ON CLUSTER ${this._cluster}` : '';
    const conditions = this._wheres.map((w) => renderWhereClause(w, ctx));
    const sql = `ALTER TABLE ${table}${clusterClause} DELETE WHERE ${conditions.join(' AND ')}`;

    return { sql, params: ctx.params };
  }
}

function mergeParams(ctx: CompileContext, source: Record<string, unknown>): void {
  for (const key of Object.keys(source)) {
    if (key in ctx.params) {
      throw new Error(`Param name collision: "${key}" is used in both the subquery and outer query`);
    }
    ctx.params[key] = source[key];
    ctx.externalParams.add(key);
  }
}

function renderValue(value: Param | Expression, ctx: CompileContext): string {
  if (value instanceof Subquery) {
    mergeParams(ctx, value.subqueryParams);
    return value.sql;
  }
  if (value instanceof Param) {
    if (ctx.externalParams.has(value.name)) {
      throw new Error(`Param name collision: "${value.name}" is used in both the subquery and outer query`);
    }
    ctx.params[value.name] = undefined;
    return value.toString();
  }
  return value.sql;
}

function renderWhereClause(w: WhereClause, ctx: CompileContext): string {
  switch (w.kind) {
    case 'comparison':
      return `${w.column} ${w.op} ${renderValue(w.value, ctx)}`;
    case 'unary':
      return `${w.column} ${w.op}`;
    case 'between':
      return `${w.column} ${w.op} ${renderValue(w.low, ctx)} AND ${renderValue(w.high, ctx)}`;
    case 'expression': {
      if (w.expr instanceof ConditionGroup) {
        for (const p of w.expr.params) {
          if (ctx.externalParams.has(p.name)) {
            throw new Error(`Param name collision: "${p.name}" is used in both the subquery and outer query`);
          }
          ctx.params[p.name] = undefined;
        }
      }
      return w.expr.sql;
    }
  }
}
