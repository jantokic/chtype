/**
 * Configuration loading for chtype codegen.
 *
 * Supports both CLI flags and a chtype.config.ts file.
 */

import { z } from 'zod';

export const ChtypeConfigSchema = z.object({
  connection: z.object({
    host: z.string().url().default('http://localhost:8123'),
    database: z.string().min(1),
    username: z.string().default('default'),
    password: z.string().default(''),
  }),
  output: z.string().default('./chtype.generated.ts'),
  bigints: z.boolean().default(false),
  include: z.array(z.string()).default([]),
  exclude: z.array(z.string()).default([]),
  typeOverrides: z.record(z.string(), z.string()).default({}),
});

export type ChtypeConfig = z.infer<typeof ChtypeConfigSchema>;

/**
 * Helper for defining a typed config file.
 *
 * @example
 * ```ts
 * // chtype.config.ts
 * import { defineConfig } from '@chtype/codegen';
 *
 * export default defineConfig({
 *   connection: {
 *     host: 'http://localhost:8123',
 *     database: 'my_db',
 *   },
 *   output: './src/generated/schema.ts',
 * });
 * ```
 */
export function defineConfig(config: z.input<typeof ChtypeConfigSchema>): ChtypeConfig {
  return ChtypeConfigSchema.parse(config);
}

/**
 * Load config from a file path. Uses dynamic import to support .ts files
 * (requires tsx or similar loader in the environment).
 */
export async function loadConfigFile(configPath: string): Promise<ChtypeConfig | null> {
  try {
    const mod = await import(configPath);
    const raw = mod.default ?? mod;
    return ChtypeConfigSchema.parse(raw);
  } catch {
    return null;
  }
}
