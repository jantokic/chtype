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

  describe('special types', () => {
    it("Enum8 → string", () => {
      expect(mapClickHouseType("Enum8('a' = 1, 'b' = 2)")).toBe('string');
    });
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

  describe('edge cases', () => {
    it('normalizes whitespace', () => {
      expect(mapClickHouseType('  Array( String )  ')).toBe('string[]');
    });
    it('returns unknown for unrecognized types', () => {
      expect(mapClickHouseType('SomeFutureType')).toBe('unknown');
    });
  });
});
