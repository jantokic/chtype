/**
 * ClickHouse schema introspection.
 *
 * Queries system.tables and system.columns to build a complete picture
 * of the database schema, including engine metadata and column flags.
 */

import type { ClickHouseClient } from '@clickhouse/client';

// ── Public types ──────────────────────────────────────────────────────

export interface IntrospectedColumn {
  name: string;
  /** Raw ClickHouse type string (e.g. "Nullable(String)", "Array(UInt64)") */
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
  /** Engine name (e.g. "ReplacingMergeTree", "MergeTree") */
  engine: string;
  /** Full engine expression (e.g. "ReplacingMergeTree(updated_at)") */
  engineFull: string;
  /** Version column for ReplacingMergeTree, if any */
  versionColumn: string | null;
  sortingKey: string;
  partitionKey: string;
  primaryKey: string;
  comment: string;
  columns: IntrospectedColumn[];
}

export interface IntrospectOptions {
  /** Glob patterns to include (e.g. ["market_*", "events"]). Empty = include all. */
  include?: string[];
  /** Glob patterns to exclude (e.g. [".inner.*"]). */
  exclude?: string[];
}

// ── Raw query row types ──────────────────────────────────────────────

interface SystemTableRow {
  name: string;
  engine: string;
  engine_full: string;
  sorting_key: string;
  partition_key: string;
  primary_key: string;
  comment: string;
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

// ── Implementation ───────────────────────────────────────────────────

/**
 * Introspect all tables in a ClickHouse database.
 * Returns structured metadata for each table and its columns.
 */
export async function introspect(
  client: ClickHouseClient,
  database: string,
  options: IntrospectOptions = {},
): Promise<IntrospectedTable[]> {
  // Fetch all tables with engine metadata
  const tablesResult = await client.query({
    query: `
      SELECT
        name,
        engine,
        engine_full,
        sorting_key,
        partition_key,
        primary_key,
        comment
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

  // Filter tables by include/exclude patterns
  const filteredTables = filterTables(
    tableRows,
    options.include ?? [],
    options.exclude ?? [],
  );

  // Fetch columns for each table
  const tables: IntrospectedTable[] = [];

  for (const table of filteredTables) {
    const columnsResult = await client.query({
      query: `
        SELECT
          name,
          type,
          default_kind,
          default_expression,
          comment,
          is_in_sorting_key,
          is_in_primary_key,
          is_in_partition_key
        FROM system.columns
        WHERE database = {database:String}
          AND table = {table:String}
        ORDER BY position
      `,
      query_params: { database, table: table.name },
      format: 'JSONEachRow',
    });

    const columnRows = await columnsResult.json<SystemColumnRow>();

    const columns: IntrospectedColumn[] = columnRows.map((col) => ({
      name: col.name,
      type: col.type,
      defaultKind: (col.default_kind || '') as IntrospectedColumn['defaultKind'],
      defaultExpression: col.default_expression,
      comment: col.comment,
      isInSortingKey: col.is_in_sorting_key === 1,
      isInPrimaryKey: col.is_in_primary_key === 1,
      isInPartitionKey: col.is_in_partition_key === 1,
    }));

    tables.push({
      name: table.name,
      engine: table.engine,
      engineFull: table.engine_full,
      versionColumn: parseVersionColumn(table.engine, table.engine_full),
      sortingKey: table.sorting_key,
      partitionKey: table.partition_key,
      primaryKey: table.primary_key,
      comment: table.comment,
      columns,
    });
  }

  return tables;
}

/**
 * Extract the version column from a ReplacingMergeTree engine_full string.
 *
 * Examples:
 *   "ReplacingMergeTree(updated_at)" → "updated_at"
 *   "ReplicatedReplacingMergeTree('/path', '{replica}', updated_at)" → "updated_at"
 *   "MergeTree" → null
 *   "ReplacingMergeTree" → null (no version column)
 */
export function parseVersionColumn(engine: string, engineFull: string): string | null {
  if (!engine.includes('ReplacingMergeTree')) return null;

  // Match the arguments inside the outermost parentheses
  const match = engineFull.match(/ReplacingMergeTree\((.+)\)/);
  if (!match) return null;

  const args = match[1]!;

  // The version column is the last argument that isn't a quoted string
  // (quoted strings are replication path and replica name)
  const parts = args.split(',').map((s) => s.trim());
  const lastArg = parts[parts.length - 1];
  if (!lastArg) return null;

  // If it's a quoted string, there's no version column in this position
  if (lastArg.startsWith("'") || lastArg.startsWith('"')) return null;

  return lastArg;
}

/** Match table names against glob-like patterns (supports * wildcard). */
function matchesPattern(name: string, pattern: string): boolean {
  const regex = new RegExp(
    `^${pattern.replace(/\./g, '\\.').replace(/\*/g, '.*')}$`,
  );
  return regex.test(name);
}

function filterTables(
  tables: SystemTableRow[],
  include: string[],
  exclude: string[],
): SystemTableRow[] {
  return tables.filter((t) => {
    // If include patterns specified, table must match at least one
    if (include.length > 0) {
      const included = include.some((p) => matchesPattern(t.name, p));
      if (!included) return false;
    }

    // If exclude patterns specified, table must not match any
    if (exclude.length > 0) {
      const excluded = exclude.some((p) => matchesPattern(t.name, p));
      if (excluded) return false;
    }

    return true;
  });
}
