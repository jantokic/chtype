import { describe, expect, it } from 'bun:test';
import type { IntrospectedColumn, IntrospectedTable } from '../../codegen/introspect.js';
import type { ColumnDiff, SchemaDiff } from '../differ.js';
import { generateAlterTable, generateCreateTable, generateDropTable, generateSQL } from '../generator.js';

function makeColumn(overrides: Partial<IntrospectedColumn> = {}): IntrospectedColumn {
  return {
    name: 'id',
    type: 'String',
    defaultKind: '',
    defaultExpression: '',
    comment: '',
    isInSortingKey: false,
    isInPrimaryKey: false,
    isInPartitionKey: false,
    ...overrides,
  };
}

function makeTable(overrides: Partial<IntrospectedTable> & { name: string }): IntrospectedTable {
  return {
    engine: 'MergeTree',
    engineFull: 'MergeTree()',
    versionColumn: null,
    sortingKey: 'id',
    partitionKey: '',
    primaryKey: 'id',
    comment: '',
    columns: [makeColumn()],
    ...overrides,
  };
}

describe('generateCreateTable', () => {
  it('generates basic CREATE TABLE', () => {
    const table = makeTable({
      name: 'events',
      columns: [
        makeColumn({ name: 'event_id', type: 'String' }),
        makeColumn({ name: 'timestamp', type: 'DateTime' }),
      ],
      sortingKey: 'event_id',
    });
    const sql = generateCreateTable(table);

    expect(sql).toContain('CREATE TABLE events');
    expect(sql).toContain('event_id String');
    expect(sql).toContain('timestamp DateTime');
    expect(sql).toContain('ENGINE = MergeTree()');
    expect(sql).toContain('ORDER BY (event_id)');
  });

  it('includes PARTITION BY when present', () => {
    const table = makeTable({ name: 'logs', partitionKey: 'toYYYYMM(timestamp)' });
    const sql = generateCreateTable(table);
    expect(sql).toContain('PARTITION BY toYYYYMM(timestamp)');
  });

  it('includes PRIMARY KEY when different from sorting key', () => {
    const table = makeTable({ name: 'data', sortingKey: 'a, b', primaryKey: 'a' });
    const sql = generateCreateTable(table);
    expect(sql).toContain('ORDER BY (a, b)');
    expect(sql).toContain('PRIMARY KEY (a)');
  });

  it('omits PRIMARY KEY when same as sorting key', () => {
    const table = makeTable({ name: 'data', sortingKey: 'id', primaryKey: 'id' });
    const sql = generateCreateTable(table);
    expect(sql).not.toContain('PRIMARY KEY');
  });

  it('includes column defaults', () => {
    const table = makeTable({
      name: 'metrics',
      columns: [makeColumn({ name: 'created_at', type: 'DateTime', defaultKind: 'DEFAULT', defaultExpression: 'now()' })],
    });
    const sql = generateCreateTable(table);
    expect(sql).toContain('created_at DateTime DEFAULT now()');
  });

  it('includes column comments', () => {
    const table = makeTable({
      name: 'metrics',
      columns: [makeColumn({ name: 'value', type: 'Float64', comment: 'metric value' })],
    });
    const sql = generateCreateTable(table);
    expect(sql).toContain("value Float64 COMMENT 'metric value'");
  });

  it('includes table comment', () => {
    const table = makeTable({ name: 'metrics', comment: 'Stores raw metrics' });
    const sql = generateCreateTable(table);
    expect(sql).toContain("COMMENT 'Stores raw metrics'");
  });

  it('uses ReplacingMergeTree engine', () => {
    const table = makeTable({
      name: 'users',
      engine: 'ReplacingMergeTree',
      engineFull: 'ReplacingMergeTree(updated_at)',
    });
    const sql = generateCreateTable(table);
    expect(sql).toContain('ENGINE = ReplacingMergeTree(updated_at)');
  });

  it('quotes identifiers with special characters', () => {
    const table = makeTable({
      name: 'my-table',
      columns: [makeColumn({ name: 'hyphen-col', type: 'String' })],
      sortingKey: '`hyphen-col`',
    });
    const sql = generateCreateTable(table);
    expect(sql).toContain('CREATE TABLE `my-table`');
    expect(sql).toContain('`hyphen-col` String');
  });

  it('omits ORDER BY / PARTITION BY / PRIMARY KEY for non-MergeTree engines', () => {
    const table = makeTable({
      name: 'buffer',
      engine: 'Memory',
      engineFull: 'Memory()',
      sortingKey: 'id',
      partitionKey: 'toYYYYMM(ts)',
      primaryKey: 'id',
      columns: [makeColumn({ name: 'id', type: 'String' })],
    });
    const sql = generateCreateTable(table);
    expect(sql).toContain('ENGINE = Memory()');
    expect(sql).not.toContain('ORDER BY');
    expect(sql).not.toContain('PARTITION BY');
    expect(sql).not.toContain('PRIMARY KEY');
  });

  it('handles a table with zero columns', () => {
    const table = makeTable({ name: 'empty', columns: [] });
    const sql = generateCreateTable(table);
    expect(sql).toContain('CREATE TABLE empty');
    expect(sql).toContain('(\n\n)');
  });
});

describe('generateDropTable', () => {
  it('generates DROP TABLE', () => {
    expect(generateDropTable('old_table')).toBe('DROP TABLE old_table');
  });

  it('quotes identifiers with special characters', () => {
    expect(generateDropTable('my-table')).toBe('DROP TABLE `my-table`');
  });
});

describe('generateAlterTable', () => {
  it('generates ADD COLUMN', () => {
    const columns: ColumnDiff[] = [
      { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
    ];
    const stmts = generateAlterTable('users', columns);
    expect(stmts).toHaveLength(1);
    expect(stmts[0]).toBe('ALTER TABLE users ADD COLUMN email String');
  });

  it('generates ADD COLUMN with default', () => {
    const columns: ColumnDiff[] = [
      {
        action: 'add',
        column: makeColumn({
          name: 'status',
          type: 'String',
          defaultKind: 'DEFAULT',
          defaultExpression: "'active'",
        }),
      },
    ];
    const stmts = generateAlterTable('users', columns);
    expect(stmts[0]).toBe("ALTER TABLE users ADD COLUMN status String DEFAULT 'active'");
  });

  it('generates ADD COLUMN with comment', () => {
    const columns: ColumnDiff[] = [
      { action: 'add', column: makeColumn({ name: 'score', type: 'Float64', comment: 'user score' }) },
    ];
    const stmts = generateAlterTable('users', columns);
    expect(stmts[0]).toBe("ALTER TABLE users ADD COLUMN score Float64 COMMENT 'user score'");
  });

  it('generates DROP COLUMN', () => {
    const columns: ColumnDiff[] = [
      { action: 'drop', column: makeColumn({ name: 'legacy_field' }) },
    ];
    const stmts = generateAlterTable('users', columns);
    expect(stmts).toHaveLength(1);
    expect(stmts[0]).toBe('ALTER TABLE users DROP COLUMN legacy_field');
  });

  it('generates MODIFY COLUMN for type change', () => {
    const columns: ColumnDiff[] = [
      {
        action: 'modify',
        column: makeColumn({ name: 'count', type: 'UInt64' }),
        previous: makeColumn({ name: 'count', type: 'UInt32' }),
      },
    ];
    const stmts = generateAlterTable('events', columns);
    expect(stmts).toHaveLength(1);
    expect(stmts[0]).toBe('ALTER TABLE events MODIFY COLUMN count UInt64');
  });

  it('generates MODIFY COLUMN with default', () => {
    const columns: ColumnDiff[] = [
      {
        action: 'modify',
        column: makeColumn({ name: 'status', type: 'String', defaultKind: 'DEFAULT', defaultExpression: "'inactive'" }),
        previous: makeColumn({ name: 'status', type: 'String', defaultKind: 'DEFAULT', defaultExpression: "'active'" }),
      },
    ];
    const stmts = generateAlterTable('events', columns);
    expect(stmts[0]).toBe("ALTER TABLE events MODIFY COLUMN status String DEFAULT 'inactive'");
  });

  it('generates multiple statements for multiple column changes', () => {
    const columns: ColumnDiff[] = [
      { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
      { action: 'drop', column: makeColumn({ name: 'old_field' }) },
      {
        action: 'modify',
        column: makeColumn({ name: 'count', type: 'UInt64' }),
        previous: makeColumn({ name: 'count', type: 'UInt32' }),
      },
    ];
    const stmts = generateAlterTable('users', columns);
    expect(stmts).toHaveLength(3);
    expect(stmts[0]).toContain('ADD COLUMN email');
    expect(stmts[1]).toContain('DROP COLUMN old_field');
    expect(stmts[2]).toContain('MODIFY COLUMN count UInt64');
  });
});

describe('generateSQL', () => {
  it('returns empty array for empty diff', () => {
    const diff: SchemaDiff = { tables: [], isEmpty: true };
    expect(generateSQL(diff)).toEqual([]);
  });

  it('generates statements for a mixed diff', () => {
    const diff: SchemaDiff = {
      isEmpty: false,
      tables: [
        { action: 'add', table: 'events', definition: makeTable({ name: 'events' }) },
        { action: 'drop', table: 'legacy' },
        {
          action: 'modify',
          table: 'users',
          columns: [
            { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
          ],
        },
      ],
    };
    const stmts = generateSQL(diff);
    expect(stmts).toHaveLength(3);
    expect(stmts[0]).toContain('CREATE TABLE events');
    expect(stmts[1]).toBe('DROP TABLE legacy');
    expect(stmts[2]).toContain('ALTER TABLE users ADD COLUMN email String');
  });

  it('passes options through to all generators', () => {
    const diff: SchemaDiff = {
      isEmpty: false,
      tables: [
        { action: 'add', table: 'events', definition: makeTable({ name: 'events' }) },
        { action: 'drop', table: 'legacy' },
        {
          action: 'modify',
          table: 'users',
          columns: [
            { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
            { action: 'drop', column: makeColumn({ name: 'old' }) },
          ],
        },
      ],
    };
    const stmts = generateSQL(diff, { idempotent: true, cluster: "'{cluster}'" });
    expect(stmts[0]).toContain("CREATE TABLE IF NOT EXISTS events ON CLUSTER '{cluster}'");
    expect(stmts[1]).toBe("DROP TABLE IF EXISTS legacy ON CLUSTER '{cluster}'");
    expect(stmts[2]).toContain("ON CLUSTER '{cluster}' ADD COLUMN IF NOT EXISTS email");
    expect(stmts[3]).toContain("ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS old");
  });
});

describe('idempotent option', () => {
  it('CREATE TABLE IF NOT EXISTS', () => {
    const table = makeTable({ name: 'events' });
    const sql = generateCreateTable(table, { idempotent: true });
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS events');
  });

  it('DROP TABLE IF EXISTS', () => {
    const sql = generateDropTable('old_table', { idempotent: true });
    expect(sql).toBe('DROP TABLE IF EXISTS old_table');
  });

  it('ADD COLUMN IF NOT EXISTS', () => {
    const columns: ColumnDiff[] = [
      { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
    ];
    const stmts = generateAlterTable('users', columns, { idempotent: true });
    expect(stmts[0]).toBe('ALTER TABLE users ADD COLUMN IF NOT EXISTS email String');
  });

  it('DROP COLUMN IF EXISTS', () => {
    const columns: ColumnDiff[] = [
      { action: 'drop', column: makeColumn({ name: 'legacy_field' }) },
    ];
    const stmts = generateAlterTable('users', columns, { idempotent: true });
    expect(stmts[0]).toBe('ALTER TABLE users DROP COLUMN IF EXISTS legacy_field');
  });

  it('MODIFY COLUMN is unchanged (no IF EXISTS needed)', () => {
    const columns: ColumnDiff[] = [
      {
        action: 'modify',
        column: makeColumn({ name: 'count', type: 'UInt64' }),
        previous: makeColumn({ name: 'count', type: 'UInt32' }),
      },
    ];
    const stmts = generateAlterTable('events', columns, { idempotent: true });
    expect(stmts[0]).toBe('ALTER TABLE events MODIFY COLUMN count UInt64');
  });
});

describe('ON CLUSTER option', () => {
  it('CREATE TABLE ON CLUSTER', () => {
    const table = makeTable({ name: 'events' });
    const sql = generateCreateTable(table, { cluster: "'{cluster}'" });
    expect(sql).toContain("CREATE TABLE events ON CLUSTER '{cluster}'");
  });

  it('DROP TABLE ON CLUSTER', () => {
    const sql = generateDropTable('old_table', { cluster: "'{cluster}'" });
    expect(sql).toBe("DROP TABLE old_table ON CLUSTER '{cluster}'");
  });

  it('ALTER TABLE ON CLUSTER', () => {
    const columns: ColumnDiff[] = [
      { action: 'add', column: makeColumn({ name: 'email', type: 'String' }) },
    ];
    const stmts = generateAlterTable('users', columns, { cluster: "'{cluster}'" });
    expect(stmts[0]).toBe("ALTER TABLE users ON CLUSTER '{cluster}' ADD COLUMN email String");
  });

  it('combines idempotent and cluster options', () => {
    const table = makeTable({ name: 'events' });
    const sql = generateCreateTable(table, { idempotent: true, cluster: "'{cluster}'" });
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS events ON CLUSTER '{cluster}'");
  });
});
