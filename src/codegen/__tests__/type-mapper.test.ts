import { describe, expect, it } from 'bun:test';
import { mapClickHouseType } from '../type-mapper.js';

describe('mapClickHouseType', () => {
  describe('scalar types', () => {
    it.each([
      ['String', 'string'],
      ['UUID', 'string'],
      ['IPv4', 'string'],
      ['IPv6', 'string'],
      ['Date', 'string'],
      ['Date32', 'string'],
      ['DateTime', 'string'],
      ['Bool', 'boolean'],
      ['Boolean', 'boolean'],
      ['UInt8', 'number'],
      ['UInt16', 'number'],
      ['UInt32', 'number'],
      ['Int8', 'number'],
      ['Int16', 'number'],
      ['Int32', 'number'],
      ['Float32', 'number'],
      ['Float64', 'number'],
    ])('maps %s to %s', (input, expected) => {
      expect(mapClickHouseType(input)).toBe(expected);
    });
  });

  describe('large integer types', () => {
    const types = ['UInt64', 'UInt128', 'UInt256', 'Int64', 'Int128', 'Int256'];

    it.each(types.map((t) => [t]))('maps %s to string by default', (type) => {
      expect(mapClickHouseType(type)).toBe('string');
    });

    it.each(types.map((t) => [t]))('maps %s to bigint with option', (type) => {
      expect(mapClickHouseType(type, { bigints: true })).toBe('bigint');
    });
  });

  describe('wrappers', () => {
    it('Nullable(String) → string | null', () => {
      expect(mapClickHouseType('Nullable(String)')).toBe('string | null');
    });
    it('LowCardinality(String) → string', () => {
      expect(mapClickHouseType('LowCardinality(String)')).toBe('string');
    });
    it('LowCardinality(Nullable(String)) → string | null', () => {
      expect(mapClickHouseType('LowCardinality(Nullable(String))')).toBe('string | null');
    });
  });

  describe('containers', () => {
    it('Array(String) → string[]', () => {
      expect(mapClickHouseType('Array(String)')).toBe('string[]');
    });
    it('Array(Nullable(String)) → (string | null)[]', () => {
      expect(mapClickHouseType('Array(Nullable(String))')).toBe('(string | null)[]');
    });
    it('Array(Array(String)) → string[][]', () => {
      expect(mapClickHouseType('Array(Array(String))')).toBe('string[][]');
    });
    it('Map(String, UInt32) → Record<string, number>', () => {
      expect(mapClickHouseType('Map(String, UInt32)')).toBe('Record<string, number>');
    });
    it('Tuple(String, UInt32) → [string, number]', () => {
      expect(mapClickHouseType('Tuple(String, UInt32)')).toBe('[string, number]');
    });
    it('named tuples', () => {
      expect(mapClickHouseType('Tuple(name String, age UInt32)')).toBe('[string, number]');
    });
  });

  describe('enum types', () => {
    it("Enum8 → union literals", () => {
      expect(mapClickHouseType("Enum8('a' = 1, 'b' = 2)")).toBe('"a" | "b"');
    });
    it("Enum16 → union literals", () => {
      expect(mapClickHouseType("Enum16('active' = 1, 'inactive' = 2, 'banned' = 3)")).toBe('"active" | "inactive" | "banned"');
    });
    it("Enum8 single value", () => {
      expect(mapClickHouseType("Enum8('only' = 0)")).toBe('"only"');
    });
    it("Nullable(Enum8) → union | null", () => {
      expect(mapClickHouseType("Nullable(Enum8('yes' = 1, 'no' = 2))")).toBe('"yes" | "no" | null');
    });
    it("LowCardinality(Enum8) → union", () => {
      expect(mapClickHouseType("LowCardinality(Enum8('x' = 1, 'y' = 2))")).toBe('"x" | "y"');
    });
    it("Enum8 with special characters", () => {
      expect(mapClickHouseType("Enum8('foo\\bar' = 1, 'it''s' = 2)")).toBe('"foo\\\\bar" | "it\'s"');
    });
  });

  describe('special types', () => {
    it('Decimal(18, 8) → string', () => {
      expect(mapClickHouseType('Decimal(18, 8)')).toBe('string');
    });
    it('DateTime64(3) → string', () => {
      expect(mapClickHouseType('DateTime64(3)')).toBe('string');
    });
    it('FixedString(32) → string', () => {
      expect(mapClickHouseType('FixedString(32)')).toBe('string');
    });
    it('SimpleAggregateFunction(sum, Float64) → number', () => {
      expect(mapClickHouseType('SimpleAggregateFunction(sum, Float64)')).toBe('number');
    });
  });

  describe('insertCoerce option', () => {
    const opts = { insertCoerce: true };

    it('Decimal(18, 8) → number | string', () => {
      expect(mapClickHouseType('Decimal(18, 8)', opts)).toBe('number | string');
    });
    it('DateTime64(3) → number | string', () => {
      expect(mapClickHouseType('DateTime64(3)', opts)).toBe('number | string');
    });
    it('DateTime → number | string', () => {
      expect(mapClickHouseType('DateTime', opts)).toBe('number | string');
    });
    it('Date → number | string', () => {
      expect(mapClickHouseType('Date', opts)).toBe('number | string');
    });
    it('Date32 → number | string', () => {
      expect(mapClickHouseType('Date32', opts)).toBe('number | string');
    });
    it('UInt64 → number | string', () => {
      expect(mapClickHouseType('UInt64', opts)).toBe('number | string');
    });
    it('Int64 → number | string', () => {
      expect(mapClickHouseType('Int64', opts)).toBe('number | string');
    });
    it('UInt64 with bigints → number | bigint', () => {
      expect(mapClickHouseType('UInt64', { insertCoerce: true, bigints: true })).toBe(
        'number | bigint',
      );
    });
    it('String is unchanged', () => {
      expect(mapClickHouseType('String', opts)).toBe('string');
    });
    it('UInt32 is unchanged (already number)', () => {
      expect(mapClickHouseType('UInt32', opts)).toBe('number');
    });
    it('Bool is unchanged', () => {
      expect(mapClickHouseType('Bool', opts)).toBe('boolean');
    });
    it('FixedString is unchanged', () => {
      expect(mapClickHouseType('FixedString(32)', opts)).toBe('string');
    });
    it('Nullable(Decimal(18, 8)) → number | string | null', () => {
      expect(mapClickHouseType('Nullable(Decimal(18, 8))', opts)).toBe('number | string | null');
    });
    it('Array(DateTime) → (number | string)[]', () => {
      expect(mapClickHouseType('Array(DateTime)', opts)).toBe('(number | string)[]');
    });
  });

  describe('JSON and Dynamic types', () => {
    it('JSON → Record<string, unknown>', () => {
      expect(mapClickHouseType('JSON')).toBe('Record<string, unknown>');
    });
    it("Object('json') → Record<string, unknown>", () => {
      expect(mapClickHouseType("Object('json')")).toBe('Record<string, unknown>');
    });
    it('Nullable(JSON) → Record<string, unknown> | null', () => {
      expect(mapClickHouseType('Nullable(JSON)')).toBe('Record<string, unknown> | null');
    });
    it('Array(JSON) → Record<string, unknown>[]', () => {
      expect(mapClickHouseType('Array(JSON)')).toBe('Record<string, unknown>[]');
    });
    it('Dynamic → unknown', () => {
      expect(mapClickHouseType('Dynamic')).toBe('unknown');
    });
  });

  describe('edge cases', () => {
    it('normalizes whitespace', () => {
      expect(mapClickHouseType('  Array( String )  ')).toBe('string[]');
    });
    it('returns unknown for unrecognized types', () => {
      expect(mapClickHouseType('SomeFutureType')).toBe('unknown');
    });
  });
});
