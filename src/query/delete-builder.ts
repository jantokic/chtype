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
import { Expression } from './expressions.js';
import { Param } from './param.js';
import {
  type WhereClause,
  buildWhereClause,
  createCompileContext,
  renderWhereClause,
  VALID_IDENTIFIER,
} from './compile-utils.js';

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
    this._wheres.push(buildWhereClause(columnOrCondition, op, value));
    return this;
  }

  whereIf(
    condition: unknown,
    column: ColumnName<DB, T> | Expression | string,
    op: WhereOp,
    value?: Param | Expression | [Param | Expression, Param | Expression],
  ): this {
    if (condition) {
      this._wheres.push(buildWhereClause(column, op, value));
    }
    return this;
  }

  compile(): CompiledQuery {
    if (this._wheres.length === 0) {
      throw new Error('DELETE requires at least one WHERE condition');
    }

    const ctx = createCompileContext();
    const table = this._table as string;
    const clusterClause = this._cluster ? ` ON CLUSTER ${this._cluster}` : '';
    const conditions = this._wheres.map((w) => renderWhereClause(w, ctx));
    const sql = `ALTER TABLE ${table}${clusterClause} DELETE WHERE ${conditions.join(' AND ')}`;

    return { sql, params: ctx.params, paramTypes: ctx.paramTypes };
  }
}

