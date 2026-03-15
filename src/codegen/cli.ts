#!/usr/bin/env node

/**
 * chtype CLI — Generate TypeScript types from your ClickHouse schema.
 *
 * Usage:
 *   npx chtype generate --host http://localhost:8123 --database my_db
 *   npx chtype generate --config chtype.config.ts
 */

import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createClient } from '@clickhouse/client';
import { defineCommand, runMain } from 'citty';
import { type ChtypeConfig, ChtypeConfigSchema, loadConfigFile } from './config.js';
import { generate } from './generator.js';
import { introspect } from './introspect.js';

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
  },
  async run({ args }) {
    let config: ChtypeConfig;

    if (args.config) {
      const fileConfig = await loadConfigFile(resolve(args.config));
      if (!fileConfig) {
        process.exit(1);
      }
      config = fileConfig;
    } else {
      config = ChtypeConfigSchema.parse({
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
      const tables = await introspect(client, config.connection.database, {
        include: config.include,
        exclude: config.exclude,
      });

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
  },
});

runMain(main);
