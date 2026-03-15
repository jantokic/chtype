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
