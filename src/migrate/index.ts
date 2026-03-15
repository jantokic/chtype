export {
  diffSchemas,
  columnsEqual,
  type ColumnDiff,
  type TableDiff,
  type SchemaDiff,
  type DiffAction,
} from './differ.js';
export {
  generateSQL,
  generateCreateTable,
  generateDropTable,
  generateAlterTable,
  type GenerateOptions,
} from './generator.js';
export {
  createSnapshot,
  saveSnapshot,
  loadSnapshot,
  type Snapshot,
  type SnapshotMeta,
} from './snapshot.js';
