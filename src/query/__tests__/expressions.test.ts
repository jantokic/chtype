import { describe, expect, it } from 'bun:test';
import { fn, or, and, Expression } from '../expressions.js';
import { Param } from '../param.js';

describe('fn — function helpers', () => {
  describe('argMax / argMin', () => {
    it('argMax with single version column', () => {
      expect(fn.argMax('amount', 'updated_at').sql).toBe('argMax(amount, updated_at)');
    });

    it('argMax with tuple version columns', () => {
      expect(fn.argMax('amount', ['vid', 'updated_at']).sql).toBe('argMax(amount, (vid, updated_at))');
    });

    it('argMin with single version column', () => {
      expect(fn.argMin('amount', 'updated_at').sql).toBe('argMin(amount, updated_at)');
    });

    it('argMin with tuple version columns', () => {
      expect(fn.argMin('amount', ['vid', 'updated_at']).sql).toBe('argMin(amount, (vid, updated_at))');
    });

    it('argMax with tuple and alias', () => {
      expect(fn.argMax('amount', ['vid', 'updated_at']).as('latest_amount').toString())
        .toBe('argMax(amount, (vid, updated_at)) AS latest_amount');
    });
  });

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

  describe('-State combinators', () => {
    it('sumState', () => {
      expect(fn.sumState('amount').sql).toBe('sumState(amount)');
    });
    it('countState without column', () => {
      expect(fn.countState().sql).toBe('countState()');
    });
    it('countState with column', () => {
      expect(fn.countState('user_id').sql).toBe('countState(user_id)');
    });
    it('avgState', () => {
      expect(fn.avgState('amount').sql).toBe('avgState(amount)');
    });
    it('minState', () => {
      expect(fn.minState('amount').sql).toBe('minState(amount)');
    });
    it('maxState', () => {
      expect(fn.maxState('amount').sql).toBe('maxState(amount)');
    });
    it('uniqState', () => {
      expect(fn.uniqState('user_id').sql).toBe('uniqState(user_id)');
    });
    it('anyState', () => {
      expect(fn.anyState('col').sql).toBe('anyState(col)');
    });
    it('quantileState', () => {
      expect(fn.quantileState(0.95, 'latency').sql).toBe('quantileState(0.95)(latency)');
    });
    it('sumState with alias', () => {
      expect(fn.sumState('amount').as('amount_sum').toString()).toBe('sumState(amount) AS amount_sum');
    });
  });

  describe('-Merge combinators', () => {
    it('sumMerge', () => {
      expect(fn.sumMerge('amount_sum').sql).toBe('sumMerge(amount_sum)');
    });
    it('countMerge', () => {
      expect(fn.countMerge('event_count').sql).toBe('countMerge(event_count)');
    });
    it('avgMerge', () => {
      expect(fn.avgMerge('avg_col').sql).toBe('avgMerge(avg_col)');
    });
    it('minMerge', () => {
      expect(fn.minMerge('min_col').sql).toBe('minMerge(min_col)');
    });
    it('maxMerge', () => {
      expect(fn.maxMerge('max_col').sql).toBe('maxMerge(max_col)');
    });
    it('uniqMerge', () => {
      expect(fn.uniqMerge('uniq_col').sql).toBe('uniqMerge(uniq_col)');
    });
    it('anyMerge', () => {
      expect(fn.anyMerge('any_col').sql).toBe('anyMerge(any_col)');
    });
    it('quantileMerge', () => {
      expect(fn.quantileMerge(0.99, 'latency_quantile').sql).toBe('quantileMerge(0.99)(latency_quantile)');
    });
    it('sumMerge with alias', () => {
      expect(fn.sumMerge('amount_sum').as('total_amount').toString()).toBe('sumMerge(amount_sum) AS total_amount');
    });
  });

  describe('Expression aliasing', () => {
    it('as() works on all function results', () => {
      const expr = fn.arrayMap('x -> x * 2', 'nums').as('doubled');
      expect(expr.toString()).toBe('arrayMap(x -> x * 2, nums) AS doubled');
    });
  });

  describe('fn.raw() with param interpolation', () => {
    it('plain string (backwards compat)', () => {
      const expr = fn.raw('now()');
      expect(expr.sql).toBe('now()');
      expect(expr.params).toHaveLength(0);
    });

    it('interpolates Param into SQL and tracks it', () => {
      const p = new Param('n', 'UInt32');
      const expr = fn.raw('now() - INTERVAL ', p, ' HOUR');
      expect(expr.sql).toBe('now() - INTERVAL {n:UInt32} HOUR');
      expect(expr.params).toHaveLength(1);
      expect(expr.params[0]!.name).toBe('n');
    });

    it('interpolates multiple Params', () => {
      const p1 = new Param('start', 'DateTime');
      const p2 = new Param('end', 'DateTime');
      const expr = fn.raw('dateDiff(\'hour\', ', p1, ', ', p2, ')');
      expect(expr.sql).toBe("dateDiff('hour', {start:DateTime}, {end:DateTime})");
      expect(expr.params).toHaveLength(2);
    });

    it('interpolates Expression args (no params)', () => {
      const inner = fn.now();
      const expr = fn.raw('dateDiff(\'day\', created_at, ', inner, ')');
      expect(expr.sql).toBe("dateDiff('day', created_at, now())");
      expect(expr.params).toHaveLength(0);
    });

    it('interpolates Expression args that carry params', () => {
      const p = new Param('n', 'UInt32');
      const inner = fn.raw('INTERVAL ', p, ' DAY');
      const expr = fn.raw('col > now() - ', inner);
      expect(expr.sql).toBe('col > now() - INTERVAL {n:UInt32} DAY');
      expect(expr.params).toHaveLength(1);
      expect(expr.params[0]!.name).toBe('n');
    });

    it('as() preserves params', () => {
      const p = new Param('n', 'UInt32');
      const expr = fn.raw('INTERVAL ', p, ' HOUR').as('time_offset');
      expect(expr.toString()).toBe('INTERVAL {n:UInt32} HOUR AS time_offset');
      expect(expr.params).toHaveLength(1);
    });
  });

  describe('argMaxIf / argMinIf', () => {
    it('argMaxIf with single version column', () => {
      const condition = fn.raw('captured_at BETWEEN now() - INTERVAL 10 MINUTE AND now()');
      const expr = fn.argMaxIf('volume', 'captured_at', condition);
      expect(expr.sql).toBe(
        'argMaxIf(volume, captured_at, captured_at BETWEEN now() - INTERVAL 10 MINUTE AND now())',
      );
    });

    it('argMinIf with single version column', () => {
      const condition = fn.raw('active = 1');
      const expr = fn.argMinIf('price', 'updated_at', condition);
      expect(expr.sql).toBe('argMinIf(price, updated_at, active = 1)');
    });

    it('argMaxIf with tuple version columns', () => {
      const condition = fn.raw('status = 1');
      const expr = fn.argMaxIf('name', ['vid', 'updated_at'], condition);
      expect(expr.sql).toBe('argMaxIf(name, (vid, updated_at), status = 1)');
    });

    it('argMaxIf with alias', () => {
      const condition = fn.raw('active = 1');
      expect(fn.argMaxIf('volume', 'ts', condition).as('vol').toString())
        .toBe('argMaxIf(volume, ts, active = 1) AS vol');
    });

    it('argMaxIf propagates params from condition Expression', () => {
      const p = new Param('n', 'UInt32');
      const condition = fn.raw('captured_at > now() - INTERVAL ', p, ' MINUTE');
      const expr = fn.argMaxIf('volume', 'captured_at', condition);
      expect(expr.params).toHaveLength(1);
      expect(expr.params[0]!.name).toBe('n');
      expect(expr.sql).toBe('argMaxIf(volume, captured_at, captured_at > now() - INTERVAL {n:UInt32} MINUTE)');
    });
  });

  describe('interval / ago / sub / add', () => {
    it('fn.interval()', () => {
      expect(fn.interval(5, 'MINUTE').sql).toBe('INTERVAL 5 MINUTE');
    });

    it('fn.interval with alias', () => {
      expect(fn.interval(1, 'HOUR').as('offset').toString()).toBe('INTERVAL 1 HOUR AS offset');
    });

    it('fn.ago()', () => {
      expect(fn.ago(5, 'MINUTE').sql).toBe('now() - INTERVAL 5 MINUTE');
    });

    it('fn.ago with DAY', () => {
      expect(fn.ago(7, 'DAY').sql).toBe('now() - INTERVAL 7 DAY');
    });

    it('fn.sub()', () => {
      const expr = fn.sub(fn.now(), fn.interval(5, 'MINUTE'));
      expect(expr.sql).toBe('(now()) - (INTERVAL 5 MINUTE)');
    });

    it('fn.add()', () => {
      const expr = fn.add(fn.now(), fn.interval(1, 'HOUR'));
      expect(expr.sql).toBe('(now()) + (INTERVAL 1 HOUR)');
    });

    it('fn.sub propagates params', () => {
      const p = new Param('n', 'UInt32');
      const left = fn.raw('col_a + ', p);
      const right = fn.now();
      const expr = fn.sub(left, right);
      expect(expr.params).toHaveLength(1);
      expect(expr.params[0]!.name).toBe('n');
    });

    it('fn.add propagates params from both sides', () => {
      const p1 = new Param('a', 'UInt32');
      const p2 = new Param('b', 'UInt32');
      const expr = fn.add(fn.raw('x + ', p1), fn.raw('y + ', p2));
      expect(expr.params).toHaveLength(2);
    });

    it('fn.sub composes with fn.now and fn.interval', () => {
      const expr = fn.sub(fn.now(), fn.interval(10, 'SECOND'));
      expect(expr.sql).toBe('(now()) - (INTERVAL 10 SECOND)');
    });

    it('supports all interval units', () => {
      const units = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'] as const;
      for (const unit of units) {
        expect(fn.interval(1, unit).sql).toBe(`INTERVAL 1 ${unit}`);
      }
    });
  });

  describe('ConditionGroup param propagation', () => {
    it('collects params from plain Expression passed as condition', () => {
      const p = new Param('n', 'UInt32');
      const expr = fn.raw('col > now() - INTERVAL ', p, ' HOUR');
      const group = or(expr);
      expect(group.params).toHaveLength(1);
      expect(group.params[0]!.name).toBe('n');
    });

    it('collects params from Expression value in condition tuple', () => {
      const p = new Param('n', 'UInt32');
      const expr = fn.raw('now() - INTERVAL ', p, ' DAY');
      const group = or(['col', '>', expr]);
      expect(group.params).toHaveLength(1);
      expect(group.params[0]!.name).toBe('n');
    });

    it('collects params from multiple Expression conditions', () => {
      const p1 = new Param('a', 'UInt32');
      const p2 = new Param('b', 'String');
      const expr1 = fn.raw('x > ', p1);
      const expr2 = fn.raw('y = ', p2);
      const group = and(expr1, expr2);
      expect(group.params).toHaveLength(2);
      expect(group.params.map((p) => p.name)).toEqual(['a', 'b']);
    });
  });
});
