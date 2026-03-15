# AGENTS.md — AI Agent Instructions for chtype

## Project Identity

**chtype** is the type-safe ClickHouse toolkit for TypeScript. It fills a gap no one else has: there is no mature, lightweight, ClickHouse-native ORM or type-safe query builder for TypeScript. The goal is to become the most-starred ClickHouse TypeScript library.

### Competitive Landscape

| Library | What it is | Stars | Gap chtype fills |
|---|---|---|---|
| `@clickhouse/client` | Official transport layer — raw SQL, no types on results | ~300 | We add schema codegen, query builder, and engine-aware types on top |
| `hypequery` | Full analytics framework with HTTP server, caching, Zod | ~40 | We're a composable library, not a framework — drop-in, zero overhead |
| Kysely + CH dialect | Generic SQL query builder, ClickHouse bolt-on | 13k (Kysely) | No FINAL, argMax, SETTINGS, PREWHERE, no codegen, no Row/Insert split |
| Prisma / Drizzle | Major ORMs — no ClickHouse support, both have 100+ upvote requests | huge | We are the answer to those requests |

**Our moat:** ClickHouse-native features (argMax, FINAL, SETTINGS, engine metadata, Row vs Insert types, version column tracking) that generic query builders cannot provide without deep ClickHouse knowledge.

---

## Architecture

```
src/
├── index.ts              # Re-exports all submodules
├── codegen/              # Schema introspection + TypeScript code generation
│   ├── cli.ts            # CLI entry (`npx chtype generate`)
│   ├── config.ts         # Config file loading (Zod-validated)
│   ├── introspect.ts     # Queries system.tables/columns
│   ├── generator.ts      # Emits .ts files with Row/Insert/Database interfaces
│   └── type-mapper.ts    # ClickHouse type → TypeScript type mapping
├── query/                # Type-safe query builder
│   ├── query-builder.ts  # Factory: createQueryBuilder<DB>()
│   ├── select-builder.ts # Fluent SELECT builder (WHERE, JOIN, GROUP BY, etc.)
│   ├── insert-builder.ts # INSERT builder
│   ├── expressions.ts    # Expression + fn.* helpers (count, argMax, etc.)
│   ├── param.ts          # {name:Type} param placeholders
│   └── types.ts          # Core type definitions (DatabaseSchema, ops, etc.)
└── client/               # Thin wrapper over @clickhouse/client
    └── client.ts         # ChtypeClient — execute, query, insert, command
```

Three subpath exports: `chtype/codegen`, `chtype/query`, `chtype/client`.

### Key Design Principles

1. **Parameterized by design** — WHERE clauses only accept `Param` or `Expression`, never raw strings. SQL injection is a compile-time error.
2. **Schema-driven types** — Generated `Database` interface powers autocomplete and compile-time validation across query builder and client.
3. **ClickHouse-native** — First-class support for things Postgres ORMs ignore: argMax, FINAL, SETTINGS, ReplacingMergeTree version columns, MATERIALIZED/ALIAS column exclusion from inserts.
4. **Zero runtime overhead** — Query builder compiles to plain SQL strings with `{name:Type}` param placeholders. No query parsing at runtime.
5. **Thin wrapper, not a framework** — The client wraps `@clickhouse/client` and exposes `.raw` for escape hatches. We don't hide the underlying client.

---

## Development Setup

```bash
bun install              # Install deps
bun test                 # Run tests (Bun test runner)
bun run typecheck        # tsc --noEmit
bun run check            # Tests + typecheck
bun run build            # Compile to dist/
```

- Runtime: Node.js >= 20 or Bun
- TypeScript 5.7+ with strict mode, `noUncheckedIndexedAccess: true`
- Tests live in `__tests__/` dirs next to source, named `*.test.ts`
- CI: GitHub Actions runs `bun test` + `bun run typecheck` on every push/PR

---

## Code Conventions

### TypeScript
- **Strict mode always.** Never use `any` — use `unknown` and narrow.
- **No enums.** Use union types (`type Foo = 'a' | 'b'`).
- **Explicit return types** on public API functions.
- **Interface over type** for object shapes.
- **`.js` extension** in all relative imports (ESM resolution).
- Module system: ESNext modules, ES2022 target.

### Naming
- Files: `kebab-case.ts`
- Classes: `PascalCase` (e.g., `SelectBuilder`, `Param`)
- Functions/methods: `camelCase`
- Types/interfaces: `PascalCase`
- Constants: `UPPER_SNAKE_CASE` for true constants, `camelCase` for config-like values

### Patterns
- Builder pattern with method chaining (return `this`)
- Immutable compile step — `compile()` produces a `CompiledQuery` and doesn't mutate
- Factory functions over constructors for public API (`createQueryBuilder`, `createClient`)
- Co-locate tests with source in `__tests__/` directories
- One module per file, named export (no default exports)

### Testing
- Use `bun:test` — `describe`, `test`, `expect`
- Tests compile queries and assert on the generated SQL string and params
- No mocking of ClickHouse — codegen/introspect tests mock at the client boundary
- Test file naming: `<module>.test.ts`

### What NOT to do
- Don't add runtime dependencies without strong justification (current deps: `@clickhouse/client`, `zod`, `citty`)
- Don't break subpath exports (`chtype/query`, `chtype/codegen`, `chtype/client`)
- Don't allow raw string values in WHERE clauses — this is a core security guarantee
- Don't generate code that requires runtime parsing — generated types are static
- Don't add framework-level features (HTTP servers, caching, auth) — that's hypequery's territory, not ours

---

## Type System Overview

The generated `Database` interface is the heart of the type system:

```typescript
interface Database {
  [tableName: string]: {
    row: Record<string, unknown>;      // SELECT result shape
    insert: Record<string, unknown>;   // INSERT input shape (DEFAULT cols optional, MATERIALIZED excluded)
    engine: string;                    // Engine literal type
    versionColumn: string | null;      // ReplacingMergeTree version column
  };
}
```

`createQueryBuilder<Database>()` threads this through `SelectBuilder`, `InsertBuilder`, and all methods to provide end-to-end type safety.

---

## Roadmap Priorities

These are the features that will differentiate chtype and drive adoption. When working on this project, prioritize in this order:

### High Impact (Star-worthy features)
1. **Subqueries** — `WHERE col IN (SELECT ...)` and `FROM (SELECT ...) AS sub`
2. **PREWHERE** — ClickHouse-specific optimization, no other TS library supports it
3. **DELETE / ALTER / UPDATE builders** — Complete DML coverage
4. **Migrations** — Schema diffing and migration generation (huge for adoption)
5. **Raw SQL tagged template** — `sql\`SELECT ${col} FROM ${table}\`` with type inference

### Medium Impact
6. **WITH (CTE) support** — Common table expressions
7. **UNION / INTERSECT / EXCEPT** — Set operations
8. **Materialized view codegen** — Include MVs in generated types
9. **SAMPLE clause** — ClickHouse-specific sampling
10. **Array/Map/Tuple function helpers** — `arrayMap`, `arrayFilter`, `mapKeys`, etc.

### Polish
11. **Better error messages** — Human-readable compile errors for type mismatches
12. **Watch mode for codegen** — Re-generate on schema change
13. **Dry-run / diff mode** — Show what codegen would change
14. **Playground / REPL** — Interactive query builder in terminal
15. **Documentation site** — Astro/Starlight docs with live examples

---

## PR and Commit Conventions

- Commit format: `type: short description` (e.g., `feat: add PREWHERE support`, `fix: handle Nullable(Array(...))`)
- Types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`
- Keep PRs focused — one feature or fix per PR
- All PRs must pass `bun test` and `bun run typecheck`
- Add tests for every new feature or bug fix

---

## Quick Reference

| Task | Command |
|---|---|
| Run tests | `bun test` |
| Type check | `bun run typecheck` |
| Build | `bun run build` |
| Run specific test | `bun test src/query/__tests__/select-builder.test.ts` |
| Generate types (dev) | `npx chtype generate --config chtype.config.ts` |
