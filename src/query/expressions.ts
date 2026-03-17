import type { CompiledQuery } from './types.js';
import { Param } from './param.js';

/** Represents a raw SQL expression with an optional alias. TType carries the result type for typed expressions. */
export class Expression<TType = unknown> {
  declare readonly _type: TType;
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
  readonly paramTypes: Map<string, string>;

  constructor(compiled: CompiledQuery<unknown>) {
    super(`(${compiled.sql})`);
    this.subqueryParams = compiled.params;
    this.paramTypes = compiled.paramTypes ?? new Map();
  }
}

/** ClickHouse function builders. */
export const fn = {
  argMax(column: string | Expression, versionColumn: string | string[]): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    if (column instanceof Expression) {
      return new Expression(`argMax(${column.sql}, ${ver})`, undefined, [...column.params]);
    }
    return new Expression(`argMax(${column}, ${ver})`);
  },
  argMin(column: string | Expression, versionColumn: string | string[]): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    if (column instanceof Expression) {
      return new Expression(`argMin(${column.sql}, ${ver})`, undefined, [...column.params]);
    }
    return new Expression(`argMin(${column}, ${ver})`);
  },
  count(column?: string): Expression<number> {
    return new Expression<number>(column ? `count(${column})` : 'count()');
  },
  countDistinct(column: string): Expression<number> {
    return new Expression<number>(`count(DISTINCT ${column})`);
  },
  sum(column: string): Expression<number> {
    return new Expression<number>(`sum(${column})`);
  },
  avg(column: string): Expression<number> {
    return new Expression<number>(`avg(${column})`);
  },
  min(column: string): Expression<number> {
    return new Expression<number>(`min(${column})`);
  },
  max(column: string): Expression<number> {
    return new Expression<number>(`max(${column})`);
  },
  groupArray(column: string): Expression {
    return new Expression(`groupArray(${column})`);
  },
  arrayJoin(column: string): Expression {
    return new Expression(`arrayJoin(${column})`);
  },
  uniq(column: string): Expression<number> {
    return new Expression<number>(`uniq(${column})`);
  },
  uniqExact(column: string): Expression<number> {
    return new Expression<number>(`uniqExact(${column})`);
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
  length(column: string): Expression<number> {
    return new Expression<number>(`length(${column})`);
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
  now(): Expression<string> {
    return new Expression<string>('now()');
  },
  now64(precision?: number): Expression<string> {
    return new Expression<string>(precision !== undefined ? `now64(${precision})` : 'now64()');
  },
  today(): Expression<string> {
    return new Expression<string>('today()');
  },
  dateDiff(unit: string, start: string, end: string): Expression<number> {
    return new Expression<number>(`dateDiff('${unit}', ${start}, ${end})`);
  },

  // --- String functions ---

  lower(column: string): Expression<string> {
    return new Expression<string>(`lower(${column})`);
  },
  upper(column: string): Expression<string> {
    return new Expression<string>(`upper(${column})`);
  },
  trim(column: string): Expression<string> {
    return new Expression<string>(`trimBoth(${column})`);
  },
  concat(...columns: string[]): Expression<string> {
    return new Expression<string>(`concat(${columns.join(', ')})`);
  },
  substring(column: string, offset: number, length?: number): Expression<string> {
    const args = length !== undefined ? `${column}, ${offset}, ${length}` : `${column}, ${offset}`;
    return new Expression<string>(`substring(${args})`);
  },

  // --- Conditional ---

  if_(condition: string, then: string, else_: string): Expression {
    return new Expression(`if(${condition}, ${then}, ${else_})`);
  },
  multiIf(...args: string[]): Expression {
    return new Expression(`multiIf(${args.join(', ')})`);
  },
  coalesce(...args: (string | number | Expression | Param)[]): Expression {
    const params: Param[] = [];
    const parts = args.map((a) => {
      if (typeof a === 'number') return String(a);
      if (typeof a === 'string') return a;
      if (a instanceof Param) {
        params.push(a);
        return a.toString();
      }
      params.push(...a.params);
      return a.sql;
    });
    return new Expression(`coalesce(${parts.join(', ')})`, undefined, params);
  },
  ifNull(col: string, defaultValue: string | number | Param | Expression): Expression {
    if (defaultValue instanceof Param) {
      return new Expression(`ifNull(${col}, ${defaultValue.toString()})`, undefined, [defaultValue]);
    }
    if (defaultValue instanceof Expression) {
      return new Expression(`ifNull(${col}, ${defaultValue.sql})`, undefined, [...defaultValue.params]);
    }
    return new Expression(`ifNull(${col}, ${defaultValue})`);
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

  quantile(level: number, column: string): Expression<number> {
    return new Expression<number>(`quantile(${level})(${column})`);
  },
  median(column: string): Expression<number> {
    return new Expression<number>(`median(${column})`);
  },
  any(column: string): Expression {
    return new Expression(`any(${column})`);
  },
  anyLast(column: string): Expression {
    return new Expression(`anyLast(${column})`);
  },
  sumIf(column: string, condition: string | Expression): Expression<number> {
    if (condition instanceof Expression) {
      return new Expression<number>(`sumIf(${column}, ${condition.sql})`, undefined, [...condition.params]);
    }
    return new Expression<number>(`sumIf(${column}, ${condition})`);
  },
  countIf(condition: string | Expression): Expression<number> {
    if (condition instanceof Expression) {
      return new Expression<number>(`countIf(${condition.sql})`, undefined, [...condition.params]);
    }
    return new Expression<number>(`countIf(${condition})`);
  },
  avgIf(column: string, condition: string | Expression): Expression<number> {
    if (condition instanceof Expression) {
      return new Expression<number>(`avgIf(${column}, ${condition.sql})`, undefined, [...condition.params]);
    }
    return new Expression<number>(`avgIf(${column}, ${condition})`);
  },
  argMaxIf(column: string | Expression, versionColumn: string | string[], condition: Expression): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    const colSql = column instanceof Expression ? column.sql : column;
    const colParams = column instanceof Expression ? column.params : [];
    return new Expression(`argMaxIf(${colSql}, ${ver}, ${condition.sql})`, undefined, [...colParams, ...condition.params]);
  },
  argMinIf(column: string | Expression, versionColumn: string | string[], condition: Expression): Expression {
    const ver = Array.isArray(versionColumn) ? `(${versionColumn.join(', ')})` : versionColumn;
    const colSql = column instanceof Expression ? column.sql : column;
    const colParams = column instanceof Expression ? column.params : [];
    return new Expression(`argMinIf(${colSql}, ${ver}, ${condition.sql})`, undefined, [...colParams, ...condition.params]);
  },

  // --- Aggregate -State combinators (for writing to AggregatingMergeTree) ---

  sumState(column: string): Expression<number> {
    return new Expression<number>(`sumState(${column})`);
  },
  countState(column?: string): Expression<number> {
    return new Expression<number>(column ? `countState(${column})` : 'countState()');
  },
  avgState(column: string): Expression<number> {
    return new Expression<number>(`avgState(${column})`);
  },
  minState(column: string): Expression<number> {
    return new Expression<number>(`minState(${column})`);
  },
  maxState(column: string): Expression<number> {
    return new Expression<number>(`maxState(${column})`);
  },
  uniqState(column: string): Expression<number> {
    return new Expression<number>(`uniqState(${column})`);
  },
  anyState(column: string): Expression {
    return new Expression(`anyState(${column})`);
  },
  quantileState(level: number, column: string): Expression<number> {
    return new Expression<number>(`quantileState(${level})(${column})`);
  },

  // --- Aggregate -Merge combinators (for reading from AggregatingMergeTree) ---

  sumMerge(column: string): Expression<number> {
    return new Expression<number>(`sumMerge(${column})`);
  },
  countMerge(column: string): Expression<number> {
    return new Expression<number>(`countMerge(${column})`);
  },
  avgMerge(column: string): Expression<number> {
    return new Expression<number>(`avgMerge(${column})`);
  },
  minMerge(column: string): Expression<number> {
    return new Expression<number>(`minMerge(${column})`);
  },
  maxMerge(column: string): Expression<number> {
    return new Expression<number>(`maxMerge(${column})`);
  },
  uniqMerge(column: string): Expression<number> {
    return new Expression<number>(`uniqMerge(${column})`);
  },
  anyMerge(column: string): Expression {
    return new Expression(`anyMerge(${column})`);
  },
  quantileMerge(level: number, column: string): Expression {
    return new Expression(`quantileMerge(${level})(${column})`);
  },

  // --- Arithmetic / interval helpers ---

  interval(n: number | Param, unit: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR'): Expression {
    if (n instanceof Param) {
      return new Expression(`INTERVAL ${n.toString()} ${unit}`, undefined, [n]);
    }
    return new Expression(`INTERVAL ${n} ${unit}`);
  },
  ago(n: number | Param, unit: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR'): Expression {
    if (n instanceof Param) {
      return new Expression(`now() - INTERVAL ${n.toString()} ${unit}`, undefined, [n]);
    }
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
