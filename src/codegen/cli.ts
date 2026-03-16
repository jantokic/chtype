#!/usr/bin/env node

/**
 * chtype CLI — Generate TypeScript types from your ClickHouse schema.
 *
 * Usage:
 *   npx chtype generate --host http://localhost:8123 --database my_db
 *   npx chtype generate --config chtype.config.ts
 *   npx chtype generate --config chtype.config.ts --watch
 */

import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createClient } from '@clickhouse/client';
import type { ClickHouseClient } from '@clickhouse/client';
import { defineCommand, runMain } from 'citty';
import { diffSchemas, formatDiff } from '../migrate/differ.js';
import { createSnapshot, loadSnapshot, saveSnapshot } from '../migrate/snapshot.js';
import { type ChtypeConfig, ChtypeConfigSchema, loadConfigFile } from './config.js';
import { generate } from './generator.js';
import { type IntrospectedTable, introspect, schemaHash } from './introspect.js';

function resolveConfig(args: {
  config?: string;
  host?: string;
  database?: string;
  username?: string;
  password?: string;
  output?: string;
  bigints?: boolean;
  include?: string;
  exclude?: string;
}): Promise<ChtypeConfig> | ChtypeConfig {
  if (args.config) {
    return loadConfigFile(resolve(args.config)).then((cfg) => {
      if (!cfg) process.exit(1);
      return cfg;
    });
  }
  return ChtypeConfigSchema.parse({
    connection: {
      host: args.host,
      database: args.database,
      username: args.username,
      password: args.password,
    },
    output: args.output,
    bigints: args.bigints,
    include: args.include ? args.include.split(',').map((s) => s.trim()) : [],
    exclude: args.exclude ? args.exclude.split(',').map((s) => s.trim()) : [],
  });
}

function snapshotPath(outputPath: string): string {
  return outputPath.replace(/\.[^./\\]+$/, '') + '.snapshot.json';
}

async function runGenerate(
  client: ClickHouseClient,
  config: ChtypeConfig,
): Promise<{ tables: IntrospectedTable[]; hash: string }> {
  const tables = await introspect(client, config.connection.database, {
    include: config.include,
    exclude: config.exclude,
  });
  const hash = schemaHash(tables);

  console.log(`Found ${tables.length} tables`);
  for (const table of tables) {
    console.log(`  ${table.name} (${table.engine}, ${table.columns.length} columns)`);
  }

  const output = generate(tables, {
    database: config.connection.database,
    bigints: config.bigints,
  });

  const outPath = resolve(config.output);
  writeFileSync(outPath, output, 'utf-8');
  console.log(`\nWrote ${tables.length} table types to ${outPath}`);

  const snap = createSnapshot(tables, config.connection.database);
  const snapPath = snapshotPath(outPath);
  await saveSnapshot(snap, snapPath);

  return { tables, hash };
}

const generateCommand = defineCommand({
  meta: {
    name: 'generate',
    description: 'Generate TypeScript types from a ClickHouse database',
  },
  args: {
    host: { type: 'string', description: 'ClickHouse HTTP URL', default: 'http://localhost:8123' },
    database: { type: 'string', description: 'Database name' },
    username: { type: 'string', description: 'ClickHouse username', default: 'default' },
    password: { type: 'string', description: 'ClickHouse password', default: '' },
    output: { type: 'string', alias: 'o', description: 'Output file path', default: './chtype.generated.ts' },
    bigints: { type: 'boolean', description: 'Map UInt64/Int64 to bigint instead of string', default: false },
    include: { type: 'string', description: 'Comma-separated table name patterns to include' },
    exclude: { type: 'string', description: 'Comma-separated table name patterns to exclude' },
    config: { type: 'string', alias: 'c', description: 'Path to config file (chtype.config.ts)' },
    watch: { type: 'boolean', description: 'Watch for schema changes and re-generate', default: false },
    interval: { type: 'string', description: 'Poll interval in seconds for --watch mode', default: '5' },
  },
  async run({ args }) {
    const config = await resolveConfig(args);

    if (!config.connection.database) {
      console.error('Error: --database is required');
      process.exit(1);
    }

    console.log(`Connecting to ${config.connection.host}...`);
    console.log(`Database: ${config.connection.database}`);

    const client = createClient({
      url: config.connection.host,
      database: config.connection.database,
      username: config.connection.username,
      password: config.connection.password,
    });

    try {
      const { hash } = await runGenerate(client, config);

      if (!args.watch) return;

      const pollMs = Math.max(1, Number(args.interval) || 5) * 1000;
      let lastHash = hash;
      console.log(`\nWatching for schema changes (polling every ${pollMs / 1000}s)...\n`);

      const poll = async (): Promise<void> => {
        try {
          const tables = await introspect(client, config.connection.database, {
            include: config.include,
            exclude: config.exclude,
          });
          const newHash = schemaHash(tables);
          if (newHash !== lastHash) {
            console.log(`Schema change detected, regenerating...`);
            const result = await runGenerate(client, config);
            lastHash = result.hash;
            console.log(`\nWatching for schema changes (polling every ${pollMs / 1000}s)...\n`);
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.error(`Watch poll error: ${msg}`);
        }
      };

      const timer = setInterval(poll, pollMs);

      const cleanup = async (): Promise<void> => {
        clearInterval(timer);
        await client.close();
        process.exit(0);
      };

      process.on('SIGINT', cleanup);
      process.on('SIGTERM', cleanup);

      // Keep process alive — the interval will keep running
      await new Promise(() => {});
    } finally {
      if (!args.watch) {
        await client.close();
      }
    }
  },
});

const diffCommand = defineCommand({
  meta: {
    name: 'diff',
    description: 'Compare current database schema against the last generated types',
  },
  args: {
    host: { type: 'string', description: 'ClickHouse HTTP URL', default: 'http://localhost:8123' },
    database: { type: 'string', description: 'Database name' },
    username: { type: 'string', description: 'ClickHouse username', default: 'default' },
    password: { type: 'string', description: 'ClickHouse password', default: '' },
    output: { type: 'string', alias: 'o', description: 'Output file path', default: './chtype.generated.ts' },
    include: { type: 'string', description: 'Comma-separated table name patterns to include' },
    exclude: { type: 'string', description: 'Comma-separated table name patterns to exclude' },
    config: { type: 'string', alias: 'c', description: 'Path to config file (chtype.config.ts)' },
    snapshot: { type: 'string', description: 'Path to snapshot file (overrides default)' },
  },
  async run({ args }) {
    const config = await resolveConfig(args);

    if (!config.connection.database) {
      console.error('Error: --database is required');
      process.exit(1);
    }

    const snapFile = args.snapshot
      ? resolve(args.snapshot)
      : snapshotPath(resolve(config.output));

    let previousTables;
    try {
      const snap = await loadSnapshot(snapFile);
      previousTables = snap.tables;
    } catch {
      console.error(`No snapshot found at ${snapFile}`);
      console.error('Run "chtype generate" first to create a snapshot.');
      process.exit(1);
    }

    console.log(`Connecting to ${config.connection.host}...`);
    console.log(`Database: ${config.connection.database}`);

    const client = createClient({
      url: config.connection.host,
      database: config.connection.database,
      username: config.connection.username,
      password: config.connection.password,
    });

    try {
      const currentTables = await introspect(client, config.connection.database, {
        include: config.include,
        exclude: config.exclude,
      });

      const diff = diffSchemas(previousTables, currentTables);
      console.log(`\n${formatDiff(diff)}`);

      if (!diff.isEmpty) {
        process.exit(1);
      }
    } finally {
      await client.close();
    }
  },
});

const main = defineCommand({
  meta: {
    name: 'chtype',
    version: '0.1.0',
    description: 'Type-safe ClickHouse toolkit for TypeScript',
  },
  subCommands: {
    generate: generateCommand,
    diff: diffCommand,
  },
});

runMain(main);
