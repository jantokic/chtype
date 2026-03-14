/**
 * Maps ClickHouse column types to TypeScript types.
 *
 * Handles all ClickHouse type constructors: Nullable, LowCardinality, Array, Map,
 * Tuple, Enum, AggregateFunction, Decimal, DateTime64, FixedString, and all scalars.
 */

export interface TypeMapperOptions {
  /** Map UInt64/Int64/UInt128/Int128/UInt256/Int256 to bigint instead of string */
  bigints?: boolean;
}

const SCALAR_MAP: Record<string, string> = {
  String: 'string',
  UUID: 'string',
  IPv4: 'string',
  IPv6: 'string',
  Date: 'string',
  Date32: 'string',
  DateTime: 'string',
  Bool: 'boolean',
  Boolean: 'boolean',
  UInt8: 'number',
  UInt16: 'number',
  UInt32: 'number',
  Int8: 'number',
  Int16: 'number',
  Int32: 'number',
  Float32: 'number',
  Float64: 'number',
};

const LARGE_INT_TYPES = new Set([
  'UInt64',
  'UInt128',
  'UInt256',
  'Int64',
  'Int128',
  'Int256',
]);

export function mapClickHouseType(chType: string, options: TypeMapperOptions = {}): string {
  // Normalize whitespace — CH returns multi-line types for complex nested types
  const t = chType.replace(/\s+/g, ' ').trim();

  // Nullable(T) → mapped(T) | null
  const nullableMatch = t.match(/^Nullable\((.+)\)$/);
  if (nullableMatch) {
    return `${mapClickHouseType(nullableMatch[1]!, options)} | null`;
  }

  // LowCardinality(T) → mapped(T) (transparent wrapper)
  const lcMatch = t.match(/^LowCardinality\((.+)\)$/);
  if (lcMatch) {
    return mapClickHouseType(lcMatch[1]!, options);
  }

  // Array(T) → mapped(T)[]
  const arrayMatch = t.match(/^Array\((.+)\)$/);
  if (arrayMatch) {
    const inner = mapClickHouseType(arrayMatch[1]!, options);
    return inner.includes('|') ? `(${inner})[]` : `${inner}[]`;
  }

  // Map(K, V) → Record<mapped(K), mapped(V)>
  const mapMatch = t.match(/^Map\((.+)\)$/);
  if (mapMatch) {
    const inner = mapMatch[1]!;
    const commaIdx = findTopLevelComma(inner);
    if (commaIdx !== -1) {
      const keyType = mapClickHouseType(inner.slice(0, commaIdx).trim(), options);
      const valType = mapClickHouseType(inner.slice(commaIdx + 1).trim(), options);
      return `Record<${keyType}, ${valType}>`;
    }
  }

  // Tuple(T1, T2, ...) → [mapped(T1), mapped(T2), ...]
  const tupleMatch = t.match(/^Tuple\((.+)\)$/);
  if (tupleMatch) {
    const inner = tupleMatch[1]!;
    const parts = splitTopLevel(inner);
    const mapped = parts.map((p) => {
      // Named tuples: "name Type" — extract the type part
      const namedMatch = p.trim().match(/^\w+\s+(.+)$/);
      return mapClickHouseType(namedMatch ? namedMatch[1]! : p.trim(), options);
    });
    return `[${mapped.join(', ')}]`;
  }

  // Enum8/Enum16 → string
  if (t.startsWith('Enum8(') || t.startsWith('Enum16(')) {
    return 'string';
  }

  // SimpleAggregateFunction(func, T) / AggregateFunction(func, T) → mapped(T)
  const aggMatch = t.match(/^(?:SimpleAggregateFunction|AggregateFunction)\(.+?,\s*(.+)\)$/);
  if (aggMatch) {
    return mapClickHouseType(aggMatch[1]!, options);
  }

  // Decimal types → string (CH returns decimals as strings in JSONEachRow)
  if (t.startsWith('Decimal')) {
    return 'string';
  }

  // DateTime64(...) → string
  if (t.startsWith('DateTime64')) {
    return 'string';
  }

  // FixedString(N) → string
  if (t.startsWith('FixedString')) {
    return 'string';
  }

  // Large integer types — optionally map to bigint
  if (LARGE_INT_TYPES.has(t)) {
    return options.bigints ? 'bigint' : 'string';
  }

  // Scalar types
  return SCALAR_MAP[t] ?? 'unknown';
}

/** Find the index of the first top-level comma (not inside parentheses). */
export function findTopLevelComma(s: string): number {
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '(') depth++;
    else if (s[i] === ')') depth--;
    else if (s[i] === ',' && depth === 0) return i;
  }
  return -1;
}

/** Split a string by top-level commas (not inside parentheses). */
export function splitTopLevel(s: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '(') depth++;
    else if (s[i] === ')') depth--;
    else if (s[i] === ',' && depth === 0) {
      parts.push(s.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(s.slice(start));
  return parts;
}
