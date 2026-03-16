import type { CompiledQuery } from './types.js';
import { Param } from './param.js';

/** Represents a raw SQL expression with an optional alias. TType carries the result type for typed expressions. */
export class Expression<TType = unknown> {
  readonly params: Param[];

  constructor(
    public readonly sql: string,
    public readonly alias?: string,
    params?: Param[],
  ) {
    this.params = params ?? [];
  }

  as<A extends string>(alias: A): Expression<TType> & { alias: A } {
    return new Expression<TType>(this.sql, alias, this.params) as Expression<TType> & { alias: A };
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
        params.push(...c.params);
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
      params.push(...val.params);
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

  constructor(compiled: CompiledQuery<unknown>) {
    super(`(${compiled.sql})`);
    this.subqueryParams = compiled.params;
  }
}

/** ClickHouse function builders. */
export const fn = {
  argMax(column: string, versionColumn: string | string[]): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    return new Expression(`argMax(${column}, ${ver})`);
  },
  argMin(column: string, versionColumn: string | string[]): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    return new Expression(`argMin(${column}, ${ver})`);
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
  // --- Array functions ---

  arrayMap(lambda: string, column: string): Expression {
    return new Expression(`arrayMap(${lambda}, ${column})`);
  },
  arrayFilter(lambda: string, column: string): Expression {
    return new Expression(`arrayFilter(${lambda}, ${column})`);
  },
  arrayExists(lambda: string, column: string): Expression {
    return new Expression(`arrayExists(${lambda}, ${column})`);
  },
  arrayAll(lambda: string, column: string): Expression {
    return new Expression(`arrayAll(${lambda}, ${column})`);
  },
  arraySort(column: string): Expression {
    return new Expression(`arraySort(${column})`);
  },
  arrayReverse(column: string): Expression {
    return new Expression(`arrayReverse(${column})`);
  },
  arrayDistinct(column: string): Expression {
    return new Expression(`arrayDistinct(${column})`);
  },
  arrayFlatten(column: string): Expression {
    return new Expression(`arrayFlatten(${column})`);
  },
  arrayConcat(...columns: string[]): Expression {
    return new Expression(`arrayConcat(${columns.join(', ')})`);
  },
  arraySlice(column: string, offset: number, length?: number): Expression {
    const args = length !== undefined ? `${column}, ${offset}, ${length}` : `${column}, ${offset}`;
    return new Expression(`arraySlice(${args})`);
  },
  length(column: string): Expression {
    return new Expression(`length(${column})`);
  },
  has(column: string, element: string): Expression {
    return new Expression(`has(${column}, ${element})`);
  },
  indexOf(column: string, element: string): Expression {
    return new Expression(`indexOf(${column}, ${element})`);
  },

  // --- Map functions ---

  mapKeys(column: string): Expression {
    return new Expression(`mapKeys(${column})`);
  },
  mapValues(column: string): Expression {
    return new Expression(`mapValues(${column})`);
  },
  mapContains(column: string, key: string): Expression {
    return new Expression(`mapContains(${column}, ${key})`);
  },

  // --- Tuple functions ---

  tupleElement(column: string, index: number): Expression {
    return new Expression(`tupleElement(${column}, ${index})`);
  },

  // --- Date/time functions ---

  toStartOfWeek(column: string): Expression {
    return new Expression(`toStartOfWeek(${column})`);
  },
  toStartOfMonth(column: string): Expression {
    return new Expression(`toStartOfMonth(${column})`);
  },
  toStartOfYear(column: string): Expression {
    return new Expression(`toStartOfYear(${column})`);
  },
  toStartOfMinute(column: string): Expression {
    return new Expression(`toStartOfMinute(${column})`);
  },
  toDate(column: string): Expression {
    return new Expression(`toDate(${column})`);
  },
  toDateTime(column: string): Expression {
    return new Expression(`toDateTime(${column})`);
  },
  now(): Expression {
    return new Expression('now()');
  },
  today(): Expression {
    return new Expression('today()');
  },
  dateDiff(unit: string, start: string, end: string): Expression {
    return new Expression(`dateDiff('${unit}', ${start}, ${end})`);
  },

  // --- String functions ---

  lower(column: string): Expression {
    return new Expression(`lower(${column})`);
  },
  upper(column: string): Expression {
    return new Expression(`upper(${column})`);
  },
  trim(column: string): Expression {
    return new Expression(`trimBoth(${column})`);
  },
  concat(...columns: string[]): Expression {
    return new Expression(`concat(${columns.join(', ')})`);
  },
  substring(column: string, offset: number, length?: number): Expression {
    const args = length !== undefined ? `${column}, ${offset}, ${length}` : `${column}, ${offset}`;
    return new Expression(`substring(${args})`);
  },

  // --- Conditional ---

  if_(condition: string, then: string, else_: string): Expression {
    return new Expression(`if(${condition}, ${then}, ${else_})`);
  },
  multiIf(...args: string[]): Expression {
    return new Expression(`multiIf(${args.join(', ')})`);
  },
  coalesce(...columns: string[]): Expression {
    return new Expression(`coalesce(${columns.join(', ')})`);
  },

  // --- Type conversion ---

  toUInt32(column: string): Expression {
    return new Expression(`toUInt32(${column})`);
  },
  toUInt64(column: string): Expression {
    return new Expression(`toUInt64(${column})`);
  },
  toInt32(column: string): Expression {
    return new Expression(`toInt32(${column})`);
  },
  toFloat64(column: string): Expression {
    return new Expression(`toFloat64(${column})`);
  },
  toString_(column: string): Expression {
    return new Expression(`toString(${column})`);
  },

  // --- Aggregate (additional) ---

  quantile(level: number, column: string): Expression {
    return new Expression(`quantile(${level})(${column})`);
  },
  median(column: string): Expression {
    return new Expression(`median(${column})`);
  },
  any(column: string): Expression {
    return new Expression(`any(${column})`);
  },
  anyLast(column: string): Expression {
    return new Expression(`anyLast(${column})`);
  },
  sumIf(column: string, condition: string): Expression {
    return new Expression(`sumIf(${column}, ${condition})`);
  },
  countIf(condition: string): Expression {
    return new Expression(`countIf(${condition})`);
  },
  avgIf(column: string, condition: string): Expression {
    return new Expression(`avgIf(${column}, ${condition})`);
  },
  argMaxIf(column: string, versionColumn: string | string[], condition: Expression): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    return new Expression(`argMaxIf(${column}, ${ver}, ${condition.sql})`, undefined, [...condition.params]);
  },
  argMinIf(column: string, versionColumn: string | string[], condition: Expression): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    return new Expression(`argMinIf(${column}, ${ver}, ${condition.sql})`, undefined, [...condition.params]);
  },

  // --- Aggregate -State combinators (for writing to AggregatingMergeTree) ---

  sumState(column: string): Expression {
    return new Expression(`sumState(${column})`);
  },
  countState(column?: string): Expression {
    return new Expression(column ? `countState(${column})` : 'countState()');
  },
  avgState(column: string): Expression {
    return new Expression(`avgState(${column})`);
  },
  minState(column: string): Expression {
    return new Expression(`minState(${column})`);
  },
  maxState(column: string): Expression {
    return new Expression(`maxState(${column})`);
  },
  uniqState(column: string): Expression {
    return new Expression(`uniqState(${column})`);
  },
  anyState(column: string): Expression {
    return new Expression(`anyState(${column})`);
  },
  quantileState(level: number, column: string): Expression {
    return new Expression(`quantileState(${level})(${column})`);
  },

  // --- Aggregate -Merge combinators (for reading from AggregatingMergeTree) ---

  sumMerge(column: string): Expression {
    return new Expression(`sumMerge(${column})`);
  },
  countMerge(column: string): Expression {
    return new Expression(`countMerge(${column})`);
  },
  avgMerge(column: string): Expression {
    return new Expression(`avgMerge(${column})`);
  },
  minMerge(column: string): Expression {
    return new Expression(`minMerge(${column})`);
  },
  maxMerge(column: string): Expression {
    return new Expression(`maxMerge(${column})`);
  },
  uniqMerge(column: string): Expression {
    return new Expression(`uniqMerge(${column})`);
  },
  anyMerge(column: string): Expression {
    return new Expression(`anyMerge(${column})`);
  },
  quantileMerge(level: number, column: string): Expression {
    return new Expression(`quantileMerge(${level})(${column})`);
  },

  // --- Arithmetic / interval helpers ---

  interval(n: number, unit: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR'): Expression {
    return new Expression(`INTERVAL ${n} ${unit}`);
  },
  ago(n: number, unit: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR'): Expression {
    return new Expression(`now() - INTERVAL ${n} ${unit}`);
  },
  sub(left: Expression, right: Expression): Expression {
    return new Expression(`(${left.sql}) - (${right.sql})`, undefined, [...left.params, ...right.params]);
  },
  add(left: Expression, right: Expression): Expression {
    return new Expression(`(${left.sql}) + (${right.sql})`, undefined, [...left.params, ...right.params]);
  },

  /** Raw SQL expression — escape hatch for anything not covered.
   *  Accepts variadic args to interpolate Params: fn.raw('INTERVAL ', param, ' HOUR')
   */
  raw(...parts: (string | Param | Expression)[]): Expression {
    if (parts.length === 1 && typeof parts[0] === 'string') {
      return new Expression(parts[0]);
    }
    const params: Param[] = [];
    const sql = parts
      .map((p) => {
        if (typeof p === 'string') return p;
        if (p instanceof Param) {
          params.push(p);
          return p.toString();
        }
        params.push(...p.params);
        return p.sql;
      })
      .join('');
    return new Expression(sql, undefined, params);
  },
};
