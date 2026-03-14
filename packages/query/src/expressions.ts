/**
 * ClickHouse-specific SQL expressions and aggregate functions.
 *
 * These are first-class citizens in chtype, unlike generic ORMs that
 * don't understand ClickHouse's analytical function library.
 */

/** Represents a raw SQL expression with an optional alias. */
export class Expression {
  constructor(
    public readonly sql: string,
    public readonly alias?: string,
  ) {}

  /** Alias this expression in the SELECT list. */
  as(alias: string): Expression {
    return new Expression(this.sql, alias);
  }

  /** Render the expression with optional alias. */
  toString(): string {
    return this.alias ? `${this.sql} AS ${this.alias}` : this.sql;
  }
}

/**
 * ClickHouse function builders.
 *
 * Usage:
 * ```ts
 * fn.argMax('price', 'updated_at').as('latest_price')
 * fn.count().as('total')
 * fn.arrayJoin('tags').as('tag')
 * ```
 */
export const fn = {
  /** argMax(column, version_column) — get the value of `column` at the max `version`. */
  argMax(column: string, versionColumn: string): Expression {
    return new Expression(`argMax(${column}, ${versionColumn})`);
  },

  /** argMin(column, version_column) — get the value of `column` at the min `version`. */
  argMin(column: string, versionColumn: string): Expression {
    return new Expression(`argMin(${column}, ${versionColumn})`);
  },

  /** count() or count(column) */
  count(column?: string): Expression {
    return new Expression(column ? `count(${column})` : 'count()');
  },

  /** countDistinct(column) */
  countDistinct(column: string): Expression {
    return new Expression(`count(DISTINCT ${column})`);
  },

  /** sum(column) */
  sum(column: string): Expression {
    return new Expression(`sum(${column})`);
  },

  /** avg(column) */
  avg(column: string): Expression {
    return new Expression(`avg(${column})`);
  },

  /** min(column) */
  min(column: string): Expression {
    return new Expression(`min(${column})`);
  },

  /** max(column) */
  max(column: string): Expression {
    return new Expression(`max(${column})`);
  },

  /** groupArray(column) — collect values into an array. */
  groupArray(column: string): Expression {
    return new Expression(`groupArray(${column})`);
  },

  /** arrayJoin(column) — unfold an array column into rows. */
  arrayJoin(column: string): Expression {
    return new Expression(`arrayJoin(${column})`);
  },

  /** uniq(column) — approximate unique count (HyperLogLog). */
  uniq(column: string): Expression {
    return new Expression(`uniq(${column})`);
  },

  /** uniqExact(column) — exact unique count. */
  uniqExact(column: string): Expression {
    return new Expression(`uniqExact(${column})`);
  },

  /** toStartOfDay(column) */
  toStartOfDay(column: string): Expression {
    return new Expression(`toStartOfDay(${column})`);
  },

  /** toStartOfHour(column) */
  toStartOfHour(column: string): Expression {
    return new Expression(`toStartOfHour(${column})`);
  },

  /** Raw SQL expression — escape hatch for anything not covered. */
  raw(sql: string): Expression {
    return new Expression(sql);
  },
};
