/**
 * SQL generator — converts schema diffs into valid ClickHouse SQL statements.
 *
 * Produces CREATE TABLE, DROP TABLE, and ALTER TABLE ADD/DROP/MODIFY COLUMN statements.
 */

import type { IntrospectedTable } from '../codegen/introspect.js';
import type { ColumnDiff, SchemaDiff, TableDiff } from './differ.js';

/**
 * Generate ClickHouse SQL statements from a SchemaDiff.
 *
 * Returns an array of SQL strings, one per statement.
 */
export function generateSQL(diff: SchemaDiff): string[] {
  const statements: string[] = [];

  for (const tableDiff of diff.tables) {
    switch (tableDiff.action) {
      case 'add':
        if (tableDiff.definition) {
          statements.push(generateCreateTable(tableDiff.definition));
        }
        break;
      case 'drop':
        statements.push(generateDropTable(tableDiff.table));
        break;
      case 'modify':
        if (tableDiff.columns) {
          statements.push(...generateAlterTable(tableDiff.table, tableDiff.columns));
        }
        break;
    }
  }

  return statements;
}

/**
 * Generate a CREATE TABLE statement from a full table definition.
 *
 * Note: sortingKey, partitionKey, defaultExpression, and other values are
 * trusted inputs from introspect.ts (sourced from system tables), not
 * user-supplied strings, so they are emitted as-is without sanitization.
 */
export function generateCreateTable(table: IntrospectedTable): string {
  const columns = table.columns.map((col) => {
    let def = `  ${quoteIdentifier(col.name)} ${col.type}`;
    if (col.defaultKind && col.defaultExpression) {
      def += ` ${col.defaultKind} ${col.defaultExpression}`;
    }
    if (col.comment) {
      def += ` COMMENT ${quoteSingleString(col.comment)}`;
    }
    return def;
  });

  let sql = `CREATE TABLE ${quoteIdentifier(table.name)}\n(\n${columns.join(',\n')}\n)`;
  sql += `\nENGINE = ${table.engineFull || table.engine}`;

  // ORDER BY, PARTITION BY, and PRIMARY KEY are only valid for MergeTree-family engines.
  // Emitting them for Log, Memory, Kafka, etc. produces invalid DDL.
  const isMergeTree = table.engine.includes('MergeTree');
  if (isMergeTree && table.partitionKey) {
    sql += `\nPARTITION BY ${table.partitionKey}`;
  }
  if (isMergeTree && table.sortingKey) {
    sql += `\nORDER BY (${table.sortingKey})`;
  }
  if (isMergeTree && table.primaryKey && table.primaryKey !== table.sortingKey) {
    sql += `\nPRIMARY KEY (${table.primaryKey})`;
  }
  if (table.comment) {
    sql += `\nCOMMENT ${quoteSingleString(table.comment)}`;
  }

  return sql;
}

/**
 * Generate a DROP TABLE statement.
 */
export function generateDropTable(tableName: string): string {
  return `DROP TABLE ${quoteIdentifier(tableName)}`;
}

/**
 * Generate ALTER TABLE statements for column-level changes.
 *
 * Each column change produces a separate ALTER TABLE statement for clarity
 * and to allow partial application.
 */
export function generateAlterTable(tableName: string, columns: ColumnDiff[]): string[] {
  const statements: string[] = [];
  const quoted = quoteIdentifier(tableName);

  for (const diff of columns) {
    switch (diff.action) {
      case 'add': {
        let clause = `ALTER TABLE ${quoted} ADD COLUMN ${quoteIdentifier(diff.column.name)} ${diff.column.type}`;
        if (diff.column.defaultKind && diff.column.defaultExpression) {
          clause += ` ${diff.column.defaultKind} ${diff.column.defaultExpression}`;
        }
        if (diff.column.comment) {
          clause += ` COMMENT ${quoteSingleString(diff.column.comment)}`;
        }
        statements.push(clause);
        break;
      }
      case 'drop':
        statements.push(`ALTER TABLE ${quoted} DROP COLUMN ${quoteIdentifier(diff.column.name)}`);
        break;
      case 'modify': {
        let clause = `ALTER TABLE ${quoted} MODIFY COLUMN ${quoteIdentifier(diff.column.name)} ${diff.column.type}`;
        if (diff.column.defaultKind && diff.column.defaultExpression) {
          clause += ` ${diff.column.defaultKind} ${diff.column.defaultExpression}`;
        }
        if (diff.column.comment) {
          clause += ` COMMENT ${quoteSingleString(diff.column.comment)}`;
        }
        statements.push(clause);
        break;
      }
    }
  }

  return statements;
}

/** Quote a ClickHouse identifier with backticks if it contains special characters. */
function quoteIdentifier(name: string): string {
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) return name;
  return `\`${name.replace(/`/g, '\\`')}\``;
}

/** Escape and quote a string for use in SQL (single quotes). */
function quoteSingleString(value: string): string {
  return `'${value.replace(/'/g, "\\'")}'`;
}
