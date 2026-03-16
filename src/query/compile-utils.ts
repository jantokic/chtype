/**
 * Shared compilation utilities for SELECT, DELETE, and UPDATE builders.
 */

import type { BetweenOp, ComparisonOp, SetOp, UnaryOp, WhereOp } from './types.js';
import { Expression, Subquery } from './expressions.js';
import { Param } from './param.js';

export type WhereClause =
  | { kind: 'comparison'; column: string; op: ComparisonOp | SetOp; value: Param | Expression }
  | { kind: 'unary'; column: string; op: UnaryOp }
  | { kind: 'between'; column: string; op: BetweenOp; low: Param | Expression; high: Param | Expression }
  | { kind: 'expression'; expr: Expression };

export interface CompileContext {
  params: Record<string, unknown>;
  /** Param names that came from subqueries/CTEs (external sources). */
  externalParams: Set<string>;
}

export function createCompileContext(): CompileContext {
  return { params: {}, externalParams: new Set() };
}

export function mergeParams(ctx: CompileContext, source: Record<string, unknown>): void {
  for (const key of Object.keys(source)) {
    if (key in ctx.params) {
      throw new Error(`Param name collision: "${key}" is used in both the subquery/CTE and outer query`);
    }
    ctx.params[key] = source[key];
    ctx.externalParams.add(key);
  }
}

export function renderValue(value: number | Param | Expression, ctx: CompileContext): string {
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
    registerExpressionParams(value, ctx);
    return value.sql;
  }
  // Only numbers reach here (from limit/offset)
  return String(value);
}

export function renderWhereClause(w: WhereClause, ctx: CompileContext): string {
  switch (w.kind) {
    case 'comparison':
      return `${w.column} ${w.op} ${renderValue(w.value, ctx)}`;
    case 'unary':
      return `${w.column} ${w.op}`;
    case 'between':
      return `${w.column} ${w.op} ${renderValue(w.low, ctx)} AND ${renderValue(w.high, ctx)}`;
    case 'expression': {
      registerExpressionParams(w.expr, ctx);
      return w.expr.sql;
    }
  }
}

/** Register params carried by an Expression (from fn.raw() with Params, or ConditionGroup). */
export function registerExpressionParams(expr: Expression, ctx: CompileContext): void {
  for (const p of expr.params) {
    if (ctx.externalParams.has(p.name)) {
      throw new Error(`Param name collision: "${p.name}" is used in both the subquery/CTE and outer query`);
    }
    ctx.params[p.name] = undefined;
  }
}

/** Valid SQL identifier pattern (settings keys, CTE names, cluster names, column names). */
export const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/** Parse where() arguments into a WhereClause. Shared by SELECT, DELETE, and UPDATE builders. */
export function buildWhereClause(
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
