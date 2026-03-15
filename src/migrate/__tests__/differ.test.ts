import { describe, expect, it } from 'bun:test';
import type { IntrospectedColumn, IntrospectedTable } from '../../codegen/introspect.js';
import { columnsEqual, diffSchemas } from '../differ.js';

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

describe('columnsEqual', () => {
  it('returns true for identical columns', () => {
    const a = makeColumn();
    const b = makeColumn();
    expect(columnsEqual(a, b)).toBe(true);
  });

  it('returns false when type differs', () => {
    const a = makeColumn({ type: 'String' });
    const b = makeColumn({ type: 'UInt64' });
    expect(columnsEqual(a, b)).toBe(false);
  });

  it('returns false when defaultKind differs', () => {
    const a = makeColumn({ defaultKind: '' });
    const b = makeColumn({ defaultKind: 'DEFAULT' });
    expect(columnsEqual(a, b)).toBe(false);
  });

  it('returns false when defaultExpression differs', () => {
    const a = makeColumn({ defaultKind: 'DEFAULT', defaultExpression: '0' });
    const b = makeColumn({ defaultKind: 'DEFAULT', defaultExpression: '1' });
    expect(columnsEqual(a, b)).toBe(false);
  });

  it('returns false when comment differs', () => {
    const a = makeColumn({ comment: '' });
    const b = makeColumn({ comment: 'new comment' });
    expect(columnsEqual(a, b)).toBe(false);
  });
});

describe('diffSchemas', () => {
  it('returns empty diff for identical schemas', () => {
    const schema = [makeTable({ name: 'users' })];
    const diff = diffSchemas(schema, schema);
    expect(diff.isEmpty).toBe(true);
    expect(diff.tables).toHaveLength(0);
  });

  it('detects new tables', () => {
    const from: IntrospectedTable[] = [];
    const to = [makeTable({ name: 'events' })];
    const diff = diffSchemas(from, to);

    expect(diff.isEmpty).toBe(false);
    expect(diff.tables).toHaveLength(1);
    expect(diff.tables[0]!.action).toBe('add');
    expect(diff.tables[0]!.table).toBe('events');
    expect(diff.tables[0]!.definition).toBeDefined();
  });

  it('detects dropped tables', () => {
    const from = [makeTable({ name: 'legacy' })];
    const to: IntrospectedTable[] = [];
    const diff = diffSchemas(from, to);

    expect(diff.isEmpty).toBe(false);
    expect(diff.tables).toHaveLength(1);
    expect(diff.tables[0]!.action).toBe('drop');
    expect(diff.tables[0]!.table).toBe('legacy');
  });

  it('detects new columns', () => {
    const from = [makeTable({ name: 'users', columns: [makeColumn({ name: 'id' })] })];
    const to = [
      makeTable({
        name: 'users',
        columns: [makeColumn({ name: 'id' }), makeColumn({ name: 'email', type: 'String' })],
      }),
    ];
    const diff = diffSchemas(from, to);

    expect(diff.tables).toHaveLength(1);
    expect(diff.tables[0]!.action).toBe('modify');
    expect(diff.tables[0]!.columns).toHaveLength(1);
    expect(diff.tables[0]!.columns![0]!.action).toBe('add');
    expect(diff.tables[0]!.columns![0]!.column.name).toBe('email');
  });

  it('detects dropped columns', () => {
    const from = [
      makeTable({
        name: 'users',
        columns: [makeColumn({ name: 'id' }), makeColumn({ name: 'legacy_field' })],
      }),
    ];
    const to = [makeTable({ name: 'users', columns: [makeColumn({ name: 'id' })] })];
    const diff = diffSchemas(from, to);

    expect(diff.tables).toHaveLength(1);
    expect(diff.tables[0]!.action).toBe('modify');
    expect(diff.tables[0]!.columns).toHaveLength(1);
    expect(diff.tables[0]!.columns![0]!.action).toBe('drop');
    expect(diff.tables[0]!.columns![0]!.column.name).toBe('legacy_field');
  });

  it('detects modified columns — type change', () => {
    const from = [makeTable({ name: 'events', columns: [makeColumn({ name: 'count', type: 'UInt32' })] })];
    const to = [makeTable({ name: 'events', columns: [makeColumn({ name: 'count', type: 'UInt64' })] })];
    const diff = diffSchemas(from, to);

    expect(diff.tables).toHaveLength(1);
    expect(diff.tables[0]!.columns).toHaveLength(1);
    expect(diff.tables[0]!.columns![0]!.action).toBe('modify');
    expect(diff.tables[0]!.columns![0]!.column.type).toBe('UInt64');
    expect(diff.tables[0]!.columns![0]!.previous!.type).toBe('UInt32');
  });

  it('detects modified columns — default change', () => {
    const from = [
      makeTable({
        name: 'events',
        columns: [makeColumn({ name: 'status', defaultKind: 'DEFAULT', defaultExpression: "'active'" })],
      }),
    ];
    const to = [
      makeTable({
        name: 'events',
        columns: [makeColumn({ name: 'status', defaultKind: 'DEFAULT', defaultExpression: "'inactive'" })],
      }),
    ];
    const diff = diffSchemas(from, to);

    expect(diff.tables[0]!.columns).toHaveLength(1);
    expect(diff.tables[0]!.columns![0]!.action).toBe('modify');
  });

  it('detects modified columns — comment change', () => {
    const from = [makeTable({ name: 'events', columns: [makeColumn({ name: 'id', comment: 'old' })] })];
    const to = [makeTable({ name: 'events', columns: [makeColumn({ name: 'id', comment: 'new' })] })];
    const diff = diffSchemas(from, to);

    expect(diff.tables[0]!.columns).toHaveLength(1);
    expect(diff.tables[0]!.columns![0]!.action).toBe('modify');
  });

  it('handles multiple tables with mixed changes', () => {
    const from = [
      makeTable({ name: 'users', columns: [makeColumn({ name: 'id' })] }),
      makeTable({ name: 'sessions', columns: [makeColumn({ name: 'id' })] }),
    ];
    const to = [
      makeTable({
        name: 'users',
        columns: [makeColumn({ name: 'id' }), makeColumn({ name: 'email' })],
      }),
      makeTable({ name: 'events', columns: [makeColumn({ name: 'id' })] }),
    ];
    const diff = diffSchemas(from, to);

    // sessions dropped, users modified, events added
    expect(diff.tables).toHaveLength(3);
    const actions = diff.tables.map((t) => `${t.action}:${t.table}`).sort();
    expect(actions).toEqual(['add:events', 'drop:sessions', 'modify:users']);
  });

  it('ignores tables with no column changes', () => {
    const table = makeTable({ name: 'stable', columns: [makeColumn({ name: 'id' })] });
    const diff = diffSchemas([table], [table]);
    expect(diff.isEmpty).toBe(true);
  });
});
