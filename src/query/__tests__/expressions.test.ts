import { describe, expect, it } from 'bun:test';
import { fn } from '../expressions.js';

describe('fn — function helpers', () => {
  describe('Array functions', () => {
    it('arrayMap', () => {
      expect(fn.arrayMap('x -> x * 2', 'nums').sql).toBe('arrayMap(x -> x * 2, nums)');
    });

    it('arrayFilter', () => {
      expect(fn.arrayFilter('x -> x > 0', 'nums').sql).toBe('arrayFilter(x -> x > 0, nums)');
    });

    it('arrayExists', () => {
      expect(fn.arrayExists('x -> x = 1', 'nums').sql).toBe('arrayExists(x -> x = 1, nums)');
    });

    it('arrayAll', () => {
      expect(fn.arrayAll('x -> x > 0', 'nums').sql).toBe('arrayAll(x -> x > 0, nums)');
    });

    it('arraySort', () => {
      expect(fn.arraySort('nums').sql).toBe('arraySort(nums)');
    });

    it('arrayReverse', () => {
      expect(fn.arrayReverse('nums').sql).toBe('arrayReverse(nums)');
    });

    it('arrayDistinct', () => {
      expect(fn.arrayDistinct('tags').sql).toBe('arrayDistinct(tags)');
    });

    it('arrayFlatten', () => {
      expect(fn.arrayFlatten('nested').sql).toBe('arrayFlatten(nested)');
    });

    it('arrayConcat', () => {
      expect(fn.arrayConcat('a', 'b', 'c').sql).toBe('arrayConcat(a, b, c)');
    });

    it('arraySlice with length', () => {
      expect(fn.arraySlice('arr', 1, 5).sql).toBe('arraySlice(arr, 1, 5)');
    });

    it('arraySlice without length', () => {
      expect(fn.arraySlice('arr', 2).sql).toBe('arraySlice(arr, 2)');
    });

    it('length', () => {
      expect(fn.length('tags').sql).toBe('length(tags)');
    });

    it('has', () => {
      expect(fn.has('tags', "'foo'").sql).toBe("has(tags, 'foo')");
    });

    it('indexOf', () => {
      expect(fn.indexOf('tags', "'bar'").sql).toBe("indexOf(tags, 'bar')");
    });
  });

  describe('Map functions', () => {
    it('mapKeys', () => {
      expect(fn.mapKeys('metadata').sql).toBe('mapKeys(metadata)');
    });

    it('mapValues', () => {
      expect(fn.mapValues('metadata').sql).toBe('mapValues(metadata)');
    });

    it('mapContains', () => {
      expect(fn.mapContains('metadata', "'key'").sql).toBe("mapContains(metadata, 'key')");
    });
  });

  describe('Tuple functions', () => {
    it('tupleElement', () => {
      expect(fn.tupleElement('t', 1).sql).toBe('tupleElement(t, 1)');
    });
  });

  describe('Date/time functions', () => {
    it('toStartOfWeek', () => {
      expect(fn.toStartOfWeek('dt').sql).toBe('toStartOfWeek(dt)');
    });

    it('toStartOfMonth', () => {
      expect(fn.toStartOfMonth('dt').sql).toBe('toStartOfMonth(dt)');
    });

    it('toStartOfYear', () => {
      expect(fn.toStartOfYear('dt').sql).toBe('toStartOfYear(dt)');
    });

    it('toStartOfMinute', () => {
      expect(fn.toStartOfMinute('dt').sql).toBe('toStartOfMinute(dt)');
    });

    it('now', () => {
      expect(fn.now().sql).toBe('now()');
    });

    it('today', () => {
      expect(fn.today().sql).toBe('today()');
    });

    it('dateDiff', () => {
      expect(fn.dateDiff('day', 'start_dt', 'end_dt').sql).toBe("dateDiff('day', start_dt, end_dt)");
    });

    it('toDate', () => {
      expect(fn.toDate('dt').sql).toBe('toDate(dt)');
    });

    it('toDateTime', () => {
      expect(fn.toDateTime('dt').sql).toBe('toDateTime(dt)');
    });
  });

  describe('String functions', () => {
    it('lower', () => {
      expect(fn.lower('name').sql).toBe('lower(name)');
    });

    it('upper', () => {
      expect(fn.upper('name').sql).toBe('upper(name)');
    });

    it('trim', () => {
      expect(fn.trim('name').sql).toBe('trimBoth(name)');
    });

    it('concat', () => {
      expect(fn.concat('first', "' '", 'last').sql).toBe("concat(first, ' ', last)");
    });

    it('substring with length', () => {
      expect(fn.substring('name', 1, 3).sql).toBe('substring(name, 1, 3)');
    });

    it('substring without length', () => {
      expect(fn.substring('name', 2).sql).toBe('substring(name, 2)');
    });
  });

  describe('Conditional functions', () => {
    it('if_', () => {
      expect(fn.if_('score > 0', 'score', '0').sql).toBe('if(score > 0, score, 0)');
    });

    it('multiIf', () => {
      expect(fn.multiIf('x > 10', "'high'", 'x > 5', "'mid'", "'low'").sql)
        .toBe("multiIf(x > 10, 'high', x > 5, 'mid', 'low')");
    });

    it('coalesce', () => {
      expect(fn.coalesce('a', 'b', 'c').sql).toBe('coalesce(a, b, c)');
    });
  });

  describe('Type conversion functions', () => {
    it('toUInt32', () => {
      expect(fn.toUInt32('val').sql).toBe('toUInt32(val)');
    });

    it('toFloat64', () => {
      expect(fn.toFloat64('val').sql).toBe('toFloat64(val)');
    });

    it('toString_', () => {
      expect(fn.toString_('val').sql).toBe('toString(val)');
    });
  });

  describe('Aggregate functions', () => {
    it('quantile', () => {
      expect(fn.quantile(0.95, 'latency').sql).toBe('quantile(0.95)(latency)');
    });

    it('median', () => {
      expect(fn.median('score').sql).toBe('median(score)');
    });

    it('any', () => {
      expect(fn.any('name').sql).toBe('any(name)');
    });

    it('anyLast', () => {
      expect(fn.anyLast('name').sql).toBe('anyLast(name)');
    });

    it('sumIf', () => {
      expect(fn.sumIf('amount', 'status = 1').sql).toBe('sumIf(amount, status = 1)');
    });

    it('countIf', () => {
      expect(fn.countIf('status = 1').sql).toBe('countIf(status = 1)');
    });

    it('avgIf', () => {
      expect(fn.avgIf('score', 'active = 1').sql).toBe('avgIf(score, active = 1)');
    });
  });

  describe('Expression aliasing', () => {
    it('as() works on all function results', () => {
      const expr = fn.arrayMap('x -> x * 2', 'nums').as('doubled');
      expect(expr.toString()).toBe('arrayMap(x -> x * 2, nums) AS doubled');
    });
  });
});
