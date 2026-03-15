/**
 * Schema snapshot — save and load IntrospectedTable arrays as JSON files.
 *
 * Snapshots are the mechanism for comparing a live database against a
 * previously captured schema state.
 */

import { readFile, writeFile } from 'node:fs/promises';
import type { IntrospectedTable } from '../codegen/introspect.js';

export interface SnapshotMeta {
  version: 1;
  createdAt: string;
  database: string;
}

export interface Snapshot {
  meta: SnapshotMeta;
  tables: IntrospectedTable[];
}

/**
 * Create a Snapshot object from introspected tables.
 */
export function createSnapshot(tables: IntrospectedTable[], database: string): Snapshot {
  return {
    meta: {
      version: 1,
      createdAt: new Date().toISOString(),
      database,
    },
    tables,
  };
}

/**
 * Save a snapshot to a JSON file.
 */
export async function saveSnapshot(snapshot: Snapshot, filePath: string): Promise<void> {
  const json = JSON.stringify(snapshot, null, 2);
  await writeFile(filePath, json + '\n', 'utf-8');
}

/**
 * Load a snapshot from a JSON file.
 *
 * Throws if the file does not exist or contains invalid JSON.
 */
export async function loadSnapshot(filePath: string): Promise<Snapshot> {
  const raw = await readFile(filePath, 'utf-8');
  const parsed: unknown = JSON.parse(raw);

  if (!isSnapshot(parsed)) {
    throw new Error(`Invalid snapshot file: ${filePath}`);
  }

  return parsed;
}

function isSnapshot(value: unknown): value is Snapshot {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  if (typeof obj['meta'] !== 'object' || obj['meta'] === null) return false;
  const meta = obj['meta'] as Record<string, unknown>;
  if (meta['version'] !== 1) return false;
  if (typeof meta['createdAt'] !== 'string') return false;
  if (typeof meta['database'] !== 'string') return false;
  if (!Array.isArray(obj['tables'])) return false;
  return true;
}
