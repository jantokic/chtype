# CLAUDE.md

Read [AGENTS.md](./AGENTS.md) for full project context, architecture, conventions, and roadmap.

## Claude-specific notes

- Run `bun test` after every code change to verify nothing breaks.
- Run `bun run typecheck` to catch type errors — this project uses strict TypeScript with `noUncheckedIndexedAccess`.
- Tests use `bun:test` — not Jest, not Vitest.
- All relative imports must use `.js` extensions (ESM).
- Never allow raw string/number values in WHERE clauses — only `Param` or `Expression`. This is a core security invariant.
- Prefer editing existing files over creating new ones. Keep the file count low.
- Don't add comments, docstrings, or type annotations to code you didn't change.
- **Never add Co-Authored-By lines to commits.** All commits are authored solely by the repo owner.
- Use conventional commits: `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`.
