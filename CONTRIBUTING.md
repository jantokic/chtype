# Contributing to chtype

Thanks for your interest in contributing! Here's how to get started.

## Setup

```bash
git clone https://github.com/jantokic/chtype.git
cd chtype
bun install
```

## Development

```bash
bun test           # run tests
bun run typecheck  # check types
bun run build      # compile
```

## Making Changes

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Add tests for any new functionality
4. Ensure `bun test` and `bun run typecheck` pass
5. Submit a pull request

## Conventions

- **Commits**: Use [conventional commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`
- **Imports**: All relative imports must use `.js` extensions (ESM)
- **Tests**: Use `bun:test` — not Jest, not Vitest
- **Security**: Never allow raw string/number values in WHERE clauses — only `Param` or `Expression`. This is a core invariant.
- **Types**: Strict TypeScript with `noUncheckedIndexedAccess` enabled

## Reporting Bugs

Open an issue with:
- What you expected to happen
- What actually happened
- Minimal reproduction (code snippet or repo)

## Feature Requests

Open an issue describing the use case. ClickHouse-native features are especially welcome — things that generic query builders can't do.
