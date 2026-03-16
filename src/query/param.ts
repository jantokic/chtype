/** ClickHouse parameter types for {name:Type} placeholders. */
export type ClickHouseParamType =
  | 'String'
  | 'UInt8'
  | 'UInt16'
  | 'UInt32'
  | 'UInt64'
  | 'UInt128'
  | 'UInt256'
  | 'Int8'
  | 'Int16'
  | 'Int32'
  | 'Int64'
  | 'Int128'
  | 'Int256'
  | 'Float32'
  | 'Float64'
  | 'Bool'
  | 'Date'
  | 'Date32'
  | 'DateTime'
  | 'DateTime64'
  | `DateTime64(${number})`
  | 'UUID'
  | `Decimal32(${number})`
  | `Decimal64(${number})`
  | `Decimal128(${number})`
  | `Decimal256(${number})`
  | `Decimal(${number}, ${number})`
  | `FixedString(${number})`
  | `Array(${string})`
  | `Nullable(${string})`
  | `LowCardinality(${string})`;

/** A parameter reference that compiles to {name:Type} in SQL. */
export class Param {
  constructor(
    public readonly name: string,
    public readonly type: ClickHouseParamType,
  ) {}

  toString(): string {
    return `{${this.name}:${this.type}}`;
  }
}

export function param(name: string, type: ClickHouseParamType): Param {
  return new Param(name, type);
}
