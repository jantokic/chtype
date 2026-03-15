import type { CompiledQuery } from './types.js';
import { Param } from './param.js';

/** Represents a raw SQL expression with an optional alias. */
export class Expression {
  constructor(
    public readonly sql: string,
    public readonly alias?: string,
  ) {}

  as(alias: string): Expression {
    return new Expression(this.sql, alias);
  }

  toString(): string {
    return this.alias ? `${this.sql} AS ${this.alias}` : this.sql;
  }
}

/** A condition tuple: [column, operator, value]. */
type ConditionTuple = [column: string, op: string, value: Param | Expression];

/** An expression that carries param references for registration during compile. */
export class ConditionGroup extends Expression {
  readonly params: Param[];

  constructor(conditions: (ConditionTuple | Expression)[], operator: 'AND' | 'OR') {
    const params: Param[] = [];
    const parts = conditions.map((c) => {
      if (c instanceof ConditionGroup) {
        params.push(...c.params);
        return c.sql;
      }
      if (c instanceof Expression) {
        return c.sql;
      }
      const [col, op, val] = c;
      if (val instanceof ConditionGroup) {
        params.push(...val.params);
        return `${col} ${op} ${val.sql}`;
      }
      if (val instanceof Param) {
        params.push(val);
        return `${col} ${op} ${val.toString()}`;
      }
      return `${col} ${op} ${val.sql}`;
    });
    super(`(${parts.join(` ${operator} `)})`);
    this.params = params;
  }
}

/** Group conditions with OR. */
export function or(...conditions: (ConditionTuple | Expression)[]): ConditionGroup {
  return new ConditionGroup(conditions, 'OR');
}

/** Group conditions with AND. */
export function and(...conditions: (ConditionTuple | Expression)[]): ConditionGroup {
  return new ConditionGroup(conditions, 'AND');
}

/** A subquery expression — wraps a compiled SELECT in parentheses and carries its params. */
export class Subquery extends Expression {
  readonly subqueryParams: Record<string, unknown>;

  constructor(compiled: CompiledQuery) {
    super(`(${compiled.sql})`);
    this.subqueryParams = compiled.params;
  }
}

/** ClickHouse function builders. */
export const fn = {
  argMax(column: string, versionColumn: string): Expression {
    return new Expression(`argMax(${column}, ${versionColumn})`);
  },
  argMin(column: string, versionColumn: string): Expression {
    return new Expression(`argMin(${column}, ${versionColumn})`);
  },
  count(column?: string): Expression {
    return new Expression(column ? `count(${column})` : 'count()');
  },
  countDistinct(column: string): Expression {
    return new Expression(`count(DISTINCT ${column})`);
  },
  sum(column: string): Expression {
    return new Expression(`sum(${column})`);
  },
  avg(column: string): Expression {
    return new Expression(`avg(${column})`);
  },
  min(column: string): Expression {
    return new Expression(`min(${column})`);
  },
  max(column: string): Expression {
    return new Expression(`max(${column})`);
  },
  groupArray(column: string): Expression {
    return new Expression(`groupArray(${column})`);
  },
  arrayJoin(column: string): Expression {
    return new Expression(`arrayJoin(${column})`);
  },
  uniq(column: string): Expression {
    return new Expression(`uniq(${column})`);
  },
  uniqExact(column: string): Expression {
    return new Expression(`uniqExact(${column})`);
  },
  toStartOfDay(column: string): Expression {
    return new Expression(`toStartOfDay(${column})`);
  },
  toStartOfHour(column: string): Expression {
    return new Expression(`toStartOfHour(${column})`);
  },
  /** Raw SQL expression — escape hatch for anything not covered. */
  raw(sql: string): Expression {
    return new Expression(sql);
  },
};
