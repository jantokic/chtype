/**
 * Typed parameter placeholder for ClickHouse queries.
 *
 * ClickHouse uses {name:Type} syntax for parameterized queries.
 * This module provides type-safe parameter creation.
 */

/** ClickHouse parameter types that can be used in {name:Type} placeholders. */
export type ClickHouseParamType =
  | 'String'
  | 'UInt8'
  | 'UInt16'
  | 'UInt32'
  | 'UInt64'
  | 'Int8'
  | 'Int16'
  | 'Int32'
  | 'Int64'
  | 'Float32'
  | 'Float64'
  | 'Bool'
  | 'Date'
  | 'DateTime'
  | 'DateTime64'
  | 'UUID'
  | `Array(${string})`;

/** A parameter reference that compiles to {name:Type} in SQL. */
export class Param {
  constructor(
    public readonly name: string,
    public readonly type: ClickHouseParamType,
  ) {}

  /** Render as ClickHouse parameter placeholder. */
  toString(): string {
    return `{${this.name}:${this.type}}`;
  }
}

/** Create a typed parameter placeholder. */
export function param(name: string, type: ClickHouseParamType): Param {
  return new Param(name, type);
}
