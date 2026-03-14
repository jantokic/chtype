import { describe, expect, it } from 'vitest';
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
    it.each(['UInt64', 'UInt128', 'UInt256', 'Int64', 'Int128', 'Int256'])(
      'maps %s to string by default',
      (type) => {
        expect(mapClickHouseType(type)).toBe('string');
      },
    );

    it.each(['UInt64', 'UInt128', 'UInt256', 'Int64', 'Int128', 'Int256'])(
      'maps %s to bigint when bigints option is true',
      (type) => {
        expect(mapClickHouseType(type, { bigints: true })).toBe('bigint');
      },
    );
  });

  describe('Nullable', () => {
    it('maps Nullable(String) to string | null', () => {
      expect(mapClickHouseType('Nullable(String)')).toBe('string | null');
    });

    it('maps Nullable(UInt32) to number | null', () => {
      expect(mapClickHouseType('Nullable(UInt32)')).toBe('number | null');
    });
  });

  describe('LowCardinality', () => {
    it('maps LowCardinality(String) to string', () => {
      expect(mapClickHouseType('LowCardinality(String)')).toBe('string');
    });

    it('maps LowCardinality(Nullable(String)) to string | null', () => {
      expect(mapClickHouseType('LowCardinality(Nullable(String))')).toBe('string | null');
    });
  });

  describe('Array', () => {
    it('maps Array(String) to string[]', () => {
      expect(mapClickHouseType('Array(String)')).toBe('string[]');
    });

    it('maps Array(UInt32) to number[]', () => {
      expect(mapClickHouseType('Array(UInt32)')).toBe('number[]');
    });

    it('maps Array(Nullable(String)) to (string | null)[]', () => {
      expect(mapClickHouseType('Array(Nullable(String))')).toBe('(string | null)[]');
    });

    it('maps Array(Array(String)) to string[][]', () => {
      expect(mapClickHouseType('Array(Array(String))')).toBe('string[][]');
    });
  });

  describe('Map', () => {
    it('maps Map(String, UInt32) to Record<string, number>', () => {
      expect(mapClickHouseType('Map(String, UInt32)')).toBe('Record<string, number>');
    });

    it('maps Map(String, Array(String)) to Record<string, string[]>', () => {
      expect(mapClickHouseType('Map(String, Array(String))')).toBe('Record<string, string[]>');
    });
  });

  describe('Tuple', () => {
    it('maps Tuple(String, UInt32) to [string, number]', () => {
      expect(mapClickHouseType('Tuple(String, UInt32)')).toBe('[string, number]');
    });

    it('handles named tuples', () => {
      expect(mapClickHouseType('Tuple(name String, age UInt32)')).toBe('[string, number]');
    });
  });

  describe('Enum', () => {
    it("maps Enum8('a' = 1, 'b' = 2) to string", () => {
      expect(mapClickHouseType("Enum8('a' = 1, 'b' = 2)")).toBe('string');
    });

    it("maps Enum16('x' = 1) to string", () => {
      expect(mapClickHouseType("Enum16('x' = 1)")).toBe('string');
    });
  });

  describe('Decimal', () => {
    it('maps Decimal(18, 8) to string', () => {
      expect(mapClickHouseType('Decimal(18, 8)')).toBe('string');
    });

    it('maps Decimal128(8) to string', () => {
      expect(mapClickHouseType('Decimal128(8)')).toBe('string');
    });
  });

  describe('DateTime64', () => {
    it('maps DateTime64(3) to string', () => {
      expect(mapClickHouseType('DateTime64(3)')).toBe('string');
    });

    it("maps DateTime64(3, 'UTC') to string", () => {
      expect(mapClickHouseType("DateTime64(3, 'UTC')")).toBe('string');
    });
  });

  describe('FixedString', () => {
    it('maps FixedString(32) to string', () => {
      expect(mapClickHouseType('FixedString(32)')).toBe('string');
    });
  });

  describe('AggregateFunction', () => {
    it('maps SimpleAggregateFunction(sum, Float64) to number', () => {
      expect(mapClickHouseType('SimpleAggregateFunction(sum, Float64)')).toBe('number');
    });

    it('maps AggregateFunction(uniq, String) to string', () => {
      expect(mapClickHouseType('AggregateFunction(uniq, String)')).toBe('string');
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
