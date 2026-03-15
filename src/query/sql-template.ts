import type { CompiledQuery } from './types.js';
import { ConditionGroup, Expression, Subquery } from './expressions.js';
import { Param } from './param.js';

/**
 * Allowed interpolation types for the `sql` tagged template.
 * Raw strings and numbers are intentionally excluded to prevent SQL injection.
 */
export type SqlInterpolation = Param | Expression | Subquery | CompiledQuery;

/**
 * Type-level guard that rejects raw string/number interpolation.
 * Produces a compile-time error with a helpful message.
 */
type RejectRaw<T> = T extends string
  ? never & { error: 'Raw strings are not allowed in sql`...` — use Param, Expression, or fn.raw() instead' }
  : T extends number
    ? never & { error: 'Raw numbers are not allowed in sql`...` — use Param or Expression instead' }
    : T extends SqlInterpolation
      ? T
      : never & { error: 'Unsupported interpolation type — use Param, Expression, Subquery, or CompiledQuery' };

/** Check if a value is a CompiledQuery (duck-type check). */
function isCompiledQuery(value: unknown): value is CompiledQuery {
  return (
    typeof value === 'object' &&
    value !== null &&
    'sql' in value &&
    'params' in value &&
    typeof (value as CompiledQuery).sql === 'string' &&
    typeof (value as CompiledQuery).params === 'object' &&
    !(value instanceof Expression) &&
    !(value instanceof Param) &&
    !(value instanceof Subquery)
  );
}

/**
 * Tagged template literal for raw SQL with type-safe interpolation.
 *
 * Accepts `Param`, `Expression`, `Subquery`, and `CompiledQuery` interpolations.
 * Raw strings and numbers are rejected at the TypeScript type level.
 *
 * @example
 * ```ts
 * const minScore = qb.param('min', 'Float64');
 * const query = sql`SELECT user_id FROM users WHERE score > ${minScore}`;
 * // query.sql === 'SELECT user_id FROM users WHERE score > {min:Float64}'
 * ```
 */
export function sql<T extends SqlInterpolation[]>(
  strings: TemplateStringsArray,
  ...values: { [K in keyof T]: RejectRaw<T[K]> }
): CompiledQuery {
  const params: Record<string, unknown> = {};
  const registeredParams = new Set<string>();

  function registerParam(p: Param): void {
    if (registeredParams.has(p.name)) {
      throw new Error(`Param name collision: "${p.name}" is already used in this query`);
    }
    registeredParams.add(p.name);
    params[p.name] = undefined;
  }

  function mergeParams(source: Record<string, unknown>, label: string): void {
    for (const key of Object.keys(source)) {
      if (registeredParams.has(key)) {
        throw new Error(`Param name collision: "${key}" is used in both the ${label} and outer query`);
      }
      registeredParams.add(key);
      params[key] = source[key];
    }
  }

  const sqlParts: string[] = [strings[0]!];

  for (let i = 0; i < values.length; i++) {
    const value = values[i] as SqlInterpolation;
    const nextString = strings[i + 1]!;

    if (value instanceof Param) {
      registerParam(value);
      sqlParts.push(value.toString(), nextString);
    } else if (value instanceof Subquery) {
      mergeParams(value.subqueryParams, 'subquery');
      sqlParts.push(value.sql, nextString);
    } else if (value instanceof ConditionGroup) {
      for (const p of value.params) registerParam(p);
      sqlParts.push(value.toString(), nextString);
    } else if (value instanceof Expression) {
      sqlParts.push(value.toString(), nextString);
    } else if (isCompiledQuery(value)) {
      mergeParams(value.params, 'embedded query');
      sqlParts.push(value.sql, nextString);
    } else {
      throw new Error('Unsupported interpolation type in sql template — use Param, Expression, Subquery, or CompiledQuery');
    }
  }

  return {
    sql: sqlParts.join(''),
    params,
  };
}
