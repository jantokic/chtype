/**
 * Schema differ — compares two schemas (arrays of IntrospectedTable)
 * and produces a structured diff describing all changes.
 */

import type { IntrospectedColumn, IntrospectedTable } from '../codegen/introspect.js';

export type DiffAction = 'add' | 'drop' | 'modify';

export interface ColumnDiff {
  action: DiffAction;
  column: IntrospectedColumn;
  /** Present only when action is 'modify' — the column definition before the change. */
  previous?: IntrospectedColumn;
}

export interface TableDiff {
  action: DiffAction;
  table: string;
  /** Full table definition — present for 'add' (the new table) and 'drop' (the dropped table). */
  definition?: IntrospectedTable;
  /** Column-level changes — present only when action is 'modify'. */
  columns?: ColumnDiff[];
}

export interface SchemaDiff {
  tables: TableDiff[];
  /** True when the two schemas are identical. */
  isEmpty: boolean;
}

/**
 * Compare two IntrospectedColumn values and return true if they differ
 * in type, default kind, default expression, or comment.
 *
 * Key membership flags (isInSortingKey, isInPrimaryKey, isInPartitionKey) are
 * intentionally excluded — changing ORDER BY / PARTITION BY requires table
 * recreation in ClickHouse, which is out of scope for column-level diffing.
 */
export function columnsEqual(a: IntrospectedColumn, b: IntrospectedColumn): boolean {
  return (
    a.type === b.type &&
    a.defaultKind === b.defaultKind &&
    a.defaultExpression === b.defaultExpression &&
    a.comment === b.comment
  );
}

/**
 * Diff two schemas and produce a list of table-level and column-level changes.
 *
 * @param from - The current / source schema (e.g. production snapshot).
 * @param to   - The desired / target schema (e.g. dev database).
 * @returns A SchemaDiff describing every change needed to go from `from` to `to`.
 */
export function diffSchemas(from: IntrospectedTable[], to: IntrospectedTable[]): SchemaDiff {
  const fromMap = new Map(from.map((t) => [t.name, t]));
  const toMap = new Map(to.map((t) => [t.name, t]));

  const tables: TableDiff[] = [];

  // Detect dropped tables (in `from` but not in `to`)
  for (const [name, table] of fromMap) {
    if (!toMap.has(name)) {
      tables.push({ action: 'drop', table: name, definition: table });
    }
  }

  // Detect new tables and modified tables
  for (const [name, toTable] of toMap) {
    const fromTable = fromMap.get(name);

    if (!fromTable) {
      tables.push({ action: 'add', table: name, definition: toTable });
      continue;
    }

    // Table exists in both — diff columns
    const columnDiffs = diffColumns(fromTable.columns, toTable.columns);
    if (columnDiffs.length > 0) {
      tables.push({ action: 'modify', table: name, columns: columnDiffs });
    }
  }

  return { tables, isEmpty: tables.length === 0 };
}

/**
 * Diff two column lists and return the set of column-level changes.
 */
function diffColumns(from: IntrospectedColumn[], to: IntrospectedColumn[]): ColumnDiff[] {
  const fromMap = new Map(from.map((c) => [c.name, c]));
  const toMap = new Map(to.map((c) => [c.name, c]));

  const diffs: ColumnDiff[] = [];

  // Dropped columns
  for (const [name, col] of fromMap) {
    if (!toMap.has(name)) {
      diffs.push({ action: 'drop', column: col });
    }
  }

  // New and modified columns
  for (const [name, toCol] of toMap) {
    const fromCol = fromMap.get(name);

    if (!fromCol) {
      diffs.push({ action: 'add', column: toCol });
      continue;
    }

    if (!columnsEqual(fromCol, toCol)) {
      diffs.push({ action: 'modify', column: toCol, previous: fromCol });
    }
  }

  return diffs;
}
