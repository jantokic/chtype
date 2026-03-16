/**
 * ClickHouse schema introspection.
 *
 * Queries system.tables and system.columns to build a complete picture
 * of the database schema, including engine metadata and column flags.
 */

import { createHash } from 'node:crypto';
import type { ClickHouseClient } from '@clickhouse/client';

export interface IntrospectedColumn {
  name: string;
  type: string;
  defaultKind: 'DEFAULT' | 'MATERIALIZED' | 'ALIAS' | '';
  defaultExpression: string;
  comment: string;
  isInSortingKey: boolean;
  isInPrimaryKey: boolean;
  isInPartitionKey: boolean;
}

export interface IntrospectedTable {
  name: string;
  engine: string;
  engineFull: string;
  versionColumn: string | null;
  sortingKey: string;
  partitionKey: string;
  primaryKey: string;
  comment: string;
  columns: IntrospectedColumn[];
  source?: string;
}

export interface IntrospectOptions {
  include?: string[];
  exclude?: string[];
}

interface SystemTableRow {
  name: string;
  engine: string;
  engine_full: string;
  sorting_key: string;
  partition_key: string;
  primary_key: string;
  comment: string;
  as_select: string;
}

interface SystemColumnRow {
  name: string;
  type: string;
  default_kind: string;
  default_expression: string;
  comment: string;
  is_in_sorting_key: number;
  is_in_primary_key: number;
  is_in_partition_key: number;
}

/**
 * Introspect all tables in a ClickHouse database.
 *
 * Note: Fetches columns per-table (N+1 queries). This is fine for codegen
 * (one-shot CLI tool) but may be slow for databases with 100+ tables.
 */
export async function introspect(
  client: ClickHouseClient,
  database: string,
  options: IntrospectOptions = {},
): Promise<IntrospectedTable[]> {
  const tablesResult = await client.query({
    query: `
      SELECT name, engine, engine_full, sorting_key, partition_key, primary_key, comment, as_select
      FROM system.tables
      WHERE database = {database:String}
        AND name NOT LIKE '.inner.%'
        AND name NOT LIKE '.inner_id.%'
      ORDER BY name
    `,
    query_params: { database },
    format: 'JSONEachRow',
  });

  const tableRows = await tablesResult.json<SystemTableRow>();
  const filteredTables = filterTables(tableRows, options.include ?? [], options.exclude ?? []);

  const tables: IntrospectedTable[] = [];

  for (const table of filteredTables) {
    const columnsResult = await client.query({
      query: `
        SELECT name, type, default_kind, default_expression, comment,
               is_in_sorting_key, is_in_primary_key, is_in_partition_key
        FROM system.columns
        WHERE database = {database:String} AND table = {table:String}
        ORDER BY position
      `,
      query_params: { database, table: table.name },
      format: 'JSONEachRow',
    });

    const columnRows = await columnsResult.json<SystemColumnRow>();

    const entry: IntrospectedTable = {
      name: table.name,
      engine: table.engine,
      engineFull: table.engine_full,
      versionColumn: parseVersionColumn(table.engine, table.engine_full),
      sortingKey: table.sorting_key,
      partitionKey: table.partition_key,
      primaryKey: table.primary_key,
      comment: table.comment,
      columns: columnRows.map((col) => ({
        name: col.name,
        type: col.type,
        defaultKind: (col.default_kind || '') as IntrospectedColumn['defaultKind'],
        defaultExpression: col.default_expression,
        comment: col.comment,
        isInSortingKey: col.is_in_sorting_key === 1,
        isInPrimaryKey: col.is_in_primary_key === 1,
        isInPartitionKey: col.is_in_partition_key === 1,
      })),
    };

    if (table.engine === 'MaterializedView') {
      const src = parseSourceTable(table.as_select);
      if (src) entry.source = src;
    }

    tables.push(entry);
  }

  return tables;
}

/**
 * Extract the version column from a ReplacingMergeTree engine_full string.
 *
 * Walks characters to find balanced parentheses instead of using a greedy regex,
 * which handles edge cases like nested parens in replication paths.
 */
export function parseVersionColumn(engine: string, engineFull: string): string | null {
  if (!engine.includes('ReplacingMergeTree')) return null;

  const idx = engineFull.indexOf('ReplacingMergeTree(');
  if (idx === -1) return null;

  const start = engineFull.indexOf('(', idx);
  if (start === -1) return null;

  // Walk to find matching closing paren
  let depth = 1;
  let end = start + 1;
  while (end < engineFull.length && depth > 0) {
    if (engineFull[end] === '(') depth++;
    else if (engineFull[end] === ')') depth--;
    end++;
  }
  if (depth !== 0) return null;

  const args = engineFull.slice(start + 1, end - 1);
  if (!args.trim()) return null;

  // The version column is the last argument that isn't a quoted string
  const parts = args.split(',').map((s) => s.trim());
  const lastArg = parts[parts.length - 1];
  if (!lastArg) return null;

  if (lastArg.startsWith("'") || lastArg.startsWith('"')) return null;

  return lastArg;
}

export function parseSourceTable(asSelect: string): string | null {
  if (!asSelect) return null;
  const match = asSelect.match(/\bFROM\s+([\s\S]+?)(?:\s+(?:WHERE|GROUP|ORDER|LIMIT|HAVING|PREWHERE|UNION|INTERSECT|EXCEPT|SETTINGS|FORMAT|INTO|;)|$)/i);
  if (!match) return null;
  const raw = match[1]!.trim().replace(/[`"]/g, '');
  const parts = raw.split('.');
  return parts[parts.length - 1]!;
}

export function schemaHash(tables: IntrospectedTable[]): string {
  const h = createHash('sha256');
  for (const t of tables) {
    h.update(t.name);
    h.update(t.engine);
    h.update(t.engineFull);
    for (const c of t.columns) {
      h.update(c.name);
      h.update(c.type);
      h.update(c.defaultKind);
      h.update(c.defaultExpression);
    }
  }
  return h.digest('hex');
}

/** Match table names against glob-like patterns (supports * wildcard). */
export function matchesPattern(name: string, pattern: string): boolean {
  const regex = new RegExp(`^${pattern.replace(/\./g, '\\.').replace(/\*/g, '.*')}$`);
  return regex.test(name);
}

export function filterTables(
  tables: SystemTableRow[],
  include: string[],
  exclude: string[],
): SystemTableRow[] {
  return tables.filter((t) => {
    if (include.length > 0 && !include.some((p) => matchesPattern(t.name, p))) return false;
    if (exclude.length > 0 && exclude.some((p) => matchesPattern(t.name, p))) return false;
    return true;
  });
}
