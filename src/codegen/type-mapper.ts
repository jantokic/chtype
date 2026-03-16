/**
 * Maps ClickHouse column types to TypeScript types.
 *
 * Handles all ClickHouse type constructors: Nullable, LowCardinality, Array, Map,
 * Tuple, Enum, AggregateFunction, Decimal, DateTime64, FixedString, and all scalars.
 */

export interface TypeMapperOptions {
  /** Map UInt64/Int64/UInt128/Int128/UInt256/Int256 to bigint instead of string */
  bigints?: boolean;
  /**
   * Emit coercible union types for Insert interfaces.
   * ClickHouse auto-coerces number values for Decimal, DateTime, DateTime64,
   * Date, Date32, UInt64/Int64/etc. columns. When true, these map to
   * `number | string` instead of just `string`.
   */
  insertCoerce?: boolean;
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
  const t = chType.replace(/\s+/g, ' ').trim();

  const nullableMatch = t.match(/^Nullable\((.+)\)$/);
  if (nullableMatch) {
    return `${mapClickHouseType(nullableMatch[1]!, options)} | null`;
  }

  const lcMatch = t.match(/^LowCardinality\((.+)\)$/);
  if (lcMatch) {
    return mapClickHouseType(lcMatch[1]!, options);
  }

  const arrayMatch = t.match(/^Array\((.+)\)$/);
  if (arrayMatch) {
    const inner = mapClickHouseType(arrayMatch[1]!, options);
    return inner.includes('|') ? `(${inner})[]` : `${inner}[]`;
  }

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

  const tupleMatch = t.match(/^Tuple\((.+)\)$/);
  if (tupleMatch) {
    const inner = tupleMatch[1]!;
    const parts = splitTopLevel(inner);
    const mapped = parts.map((p) => {
      const namedMatch = p.trim().match(/^\w+\s+(.+)$/);
      return mapClickHouseType(namedMatch ? namedMatch[1]! : p.trim(), options);
    });
    return `[${mapped.join(', ')}]`;
  }

  if (t.startsWith('Enum8(') || t.startsWith('Enum16(')) {
    return parseEnumLiterals(t);
  }

  const aggMatch = t.match(/^(?:SimpleAggregateFunction|AggregateFunction)\(.+?,\s*(.+)\)$/);
  if (aggMatch) {
    return mapClickHouseType(aggMatch[1]!, options);
  }

  if (t.startsWith('Decimal')) return options.insertCoerce ? 'number | string' : 'string';
  if (t.startsWith('DateTime64')) return options.insertCoerce ? 'number | string' : 'string';
  if (t.startsWith('FixedString')) return 'string';

  if (LARGE_INT_TYPES.has(t)) {
    if (options.bigints) return options.insertCoerce ? 'number | bigint' : 'bigint';
    return options.insertCoerce ? 'number | string' : 'string';
  }

  // DateTime and Date scalars — accept unix timestamps on insert
  if (options.insertCoerce && (t === 'DateTime' || t === 'Date' || t === 'Date32')) {
    return 'number | string';
  }

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

function parseEnumLiterals(t: string): string {
  const inner = t.slice(t.indexOf('(') + 1, t.lastIndexOf(')'));
  const values: string[] = [];
  const re = /'([^']*(?:''[^']*)*)'/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(inner)) !== null) {
    values.push(m[1]!.replace(/''/g, "'"));
    // skip past the " = N" part to avoid matching numbers
    const eq = inner.indexOf('=', re.lastIndex);
    if (eq !== -1) {
      const comma = inner.indexOf(',', eq);
      re.lastIndex = comma !== -1 ? comma + 1 : inner.length;
    }
  }
  if (values.length === 0) return 'string';
  return values.map((v) => `'${v}'`).join(' | ');
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
