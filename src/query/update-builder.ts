/**
 * Type-safe UPDATE builder for ClickHouse.
 *
 * ClickHouse uses ALTER TABLE ... UPDATE ... WHERE syntax.
 * All values in SET and WHERE must be Param or Expression — no raw strings.
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
import { Expression } from './expressions.js';
import { Param } from './param.js';
import {
  type WhereClause,
  createCompileContext,
  renderValue,
  renderWhereClause,
  VALID_IDENTIFIER,
} from './compile-utils.js';

interface SetClause {
  column: string;
  value: Param | Expression;
}

export class UpdateBuilder<
  DB extends DatabaseSchema,
  T extends TableName<DB> = TableName<DB>,
> {
  private _table: T;
  private _cluster?: string;
  private _sets: SetClause[] = [];
  private _wheres: WhereClause[] = [];

  constructor(table: T) {
    this._table = table;
  }

  /** Specify the cluster for distributed updates: ALTER TABLE ... ON CLUSTER ... */
  onCluster(cluster: string): this {
    if (!VALID_IDENTIFIER.test(cluster)) {
      throw new Error(`Invalid cluster name: "${cluster}"`);
    }
    this._cluster = cluster;
    return this;
  }

  /** Set a column to a parameterized value or expression. */
  set(column: ColumnName<DB, T>, value: Param | Expression): this {
    if (!VALID_IDENTIFIER.test(column)) {
      throw new Error(`Invalid column name: "${column}"`);
    }
    this._sets.push({ column, value });
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
    this._wheres.push(buildWhereClause(columnOrCondition, op, value));
    return this;
  }

  compile(): CompiledQuery {
    if (this._sets.length === 0) {
      throw new Error('UPDATE requires at least one SET assignment');
    }
    if (this._wheres.length === 0) {
      throw new Error('UPDATE requires at least one WHERE condition');
    }

    const ctx = createCompileContext();
    const table = this._table as string;
    const clusterClause = this._cluster ? ` ON CLUSTER ${this._cluster}` : '';

    const setClauses = this._sets.map((s) => {
      const val = renderValue(s.value, ctx);
      return `${s.column} = ${val}`;
    });

    const conditions = this._wheres.map((w) => renderWhereClause(w, ctx));
    const sql = `ALTER TABLE ${table}${clusterClause} UPDATE ${setClauses.join(', ')} WHERE ${conditions.join(' AND ')}`;

    return { sql, params: ctx.params };
  }
}

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
