/**
 * Type-safe ClickHouse client.
 *
 * Wraps @clickhouse/client with schema-aware methods.
 */

import {
  type ClickHouseClient as BaseClient,
  type ClickHouseClientConfigOptions,
  createClient as createBaseClient,
} from '@clickhouse/client';
import type { CompiledQuery, DatabaseSchema, InsertType, TableName } from '../query/types.js';

export interface ChtypeClient<DB extends DatabaseSchema> {
  /** Execute a compiled query and return typed rows. Result type is inferred from the compiled query. */
  execute<T = Record<string, unknown>>(query: CompiledQuery<T>): Promise<T[]>;

  /** Execute a compiled query and return a typed async iterable for streaming large result sets. */
  stream<T = Record<string, unknown>>(query: CompiledQuery<T>): AsyncIterable<T[]>;

  /** Execute a raw SQL query with optional parameters. */
  query<T = Record<string, unknown>>(sql: string, params?: Record<string, unknown>): Promise<T[]>;

  /** Insert rows into a table. Type-checked against the table's Insert type. */
  insert<T extends TableName<DB>>(table: T, rows: InsertType<DB, T>[]): Promise<void>;

  /**
   * Execute a DDL/ALTER command (raw SQL, no parameterization).
   *
   * WARNING: Never interpolate user input into the SQL string passed here.
   * This method is for DDL statements only (CREATE TABLE, ALTER TABLE, etc.)
   * where ClickHouse does not support parameterized queries.
   */
  command(sql: string): Promise<void>;

  close(): Promise<void>;
  readonly raw: BaseClient;
}

/**
 * Create a type-safe ClickHouse client.
 *
 * @example
 * ```ts
 * import { createClient } from 'chtype/client';
 * import type { Database } from './chtype.generated';
 *
 * const ch = createClient<Database>({
 *   url: 'http://localhost:8123',
 *   database: 'my_db',
 * });
 * ```
 */
export function createClient<DB extends DatabaseSchema>(
  options: ClickHouseClientConfigOptions,
): ChtypeClient<DB> {
  const client = createBaseClient(options);

  return {
    async execute<T = Record<string, unknown>>(query: CompiledQuery<T>): Promise<T[]> {
      const result = await client.query({
        query: query.sql,
        query_params: query.params,
        format: 'JSONEachRow',
      });
      return result.json<T>();
    },

    async *stream<T = Record<string, unknown>>(query: CompiledQuery<T>): AsyncIterable<T[]> {
      const result = await client.query({
        query: query.sql,
        query_params: query.params,
        format: 'JSONEachRow',
      });
      const stream = result.stream<T>();
      for await (const rows of stream) {
        yield rows.map((row) => row.json<T>());
      }
    },

    async query<T = Record<string, unknown>>(
      sql: string,
      params?: Record<string, unknown>,
    ): Promise<T[]> {
      const result = await client.query({
        query: sql,
        query_params: params,
        format: 'JSONEachRow',
      });
      return result.json<T>();
    },

    async insert<T extends TableName<DB>>(table: T, rows: InsertType<DB, T>[]): Promise<void> {
      if (rows.length === 0) return;
      await client.insert({
        table: table as string,
        values: rows,
        format: 'JSONEachRow',
      });
    },

    async command(sql: string): Promise<void> {
      await client.command({ query: sql });
    },

    async close(): Promise<void> {
      await client.close();
    },

    get raw(): BaseClient {
      return client;
    },
  };
}
