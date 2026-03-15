import { describe, expect, it } from 'bun:test';
import { readFile, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import type { IntrospectedTable } from '../../codegen/introspect.js';
import { createSnapshot, loadSnapshot, saveSnapshot } from '../snapshot.js';

const sampleTable: IntrospectedTable = {
  name: 'users',
  engine: 'MergeTree',
  engineFull: 'MergeTree()',
  versionColumn: null,
  sortingKey: 'id',
  partitionKey: '',
  primaryKey: 'id',
  comment: '',
  columns: [
    {
      name: 'id',
      type: 'String',
      defaultKind: '',
      defaultExpression: '',
      comment: '',
      isInSortingKey: true,
      isInPrimaryKey: true,
      isInPartitionKey: false,
    },
  ],
};

describe('createSnapshot', () => {
  it('creates a snapshot with metadata', () => {
    const snapshot = createSnapshot([sampleTable], 'test_db');
    expect(snapshot.meta.version).toBe(1);
    expect(snapshot.meta.database).toBe('test_db');
    expect(snapshot.meta.createdAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(snapshot.tables).toHaveLength(1);
    expect(snapshot.tables[0]!.name).toBe('users');
  });
});

describe('saveSnapshot / loadSnapshot', () => {
  it('round-trips a snapshot through JSON', async () => {
    const snapshot = createSnapshot([sampleTable], 'test_db');
    const filePath = join(tmpdir(), `chtype-test-${Date.now()}.json`);

    try {
      await saveSnapshot(snapshot, filePath);
      const raw = await readFile(filePath, 'utf-8');
      expect(raw).toContain('"version": 1');
      expect(raw.endsWith('\n')).toBe(true);

      const loaded = await loadSnapshot(filePath);
      expect(loaded.meta.version).toBe(1);
      expect(loaded.meta.database).toBe('test_db');
      expect(loaded.tables).toHaveLength(1);
      expect(loaded.tables[0]!.name).toBe('users');
      expect(loaded.tables[0]!.columns[0]!.type).toBe('String');
    } finally {
      await unlink(filePath).catch(() => {});
    }
  });

  it('throws on non-existent file', async () => {
    await expect(loadSnapshot('/nonexistent/file.json')).rejects.toThrow();
  });

  it('throws on invalid JSON', async () => {
    const filePath = join(tmpdir(), `chtype-test-bad-${Date.now()}.json`);
    const { writeFile } = await import('node:fs/promises');
    try {
      await writeFile(filePath, '{"not": "a snapshot"}', 'utf-8');
      await expect(loadSnapshot(filePath)).rejects.toThrow('Invalid snapshot file');
    } finally {
      await unlink(filePath).catch(() => {});
    }
  });

  it('rejects snapshot with null table entries', async () => {
    const filePath = join(tmpdir(), `chtype-test-null-${Date.now()}.json`);
    const { writeFile } = await import('node:fs/promises');
    const bad = JSON.stringify({
      meta: { version: 1, createdAt: '2026-01-01T00:00:00Z', database: 'db' },
      tables: [null],
    });
    try {
      await writeFile(filePath, bad, 'utf-8');
      await expect(loadSnapshot(filePath)).rejects.toThrow('Invalid snapshot file');
    } finally {
      await unlink(filePath).catch(() => {});
    }
  });

  it('rejects snapshot with table entries missing columns', async () => {
    const filePath = join(tmpdir(), `chtype-test-nocols-${Date.now()}.json`);
    const { writeFile } = await import('node:fs/promises');
    const bad = JSON.stringify({
      meta: { version: 1, createdAt: '2026-01-01T00:00:00Z', database: 'db' },
      tables: [{ name: 'users' }],
    });
    try {
      await writeFile(filePath, bad, 'utf-8');
      await expect(loadSnapshot(filePath)).rejects.toThrow('Invalid snapshot file');
    } finally {
      await unlink(filePath).catch(() => {});
    }
  });
});
