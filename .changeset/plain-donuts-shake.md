---
'octaflow': patch
---

Housekeeping release — no engine changes.

- `homepage` now points at the docs' own domain, https://octaflow.octabits.io/, instead of the
  GitHub Pages URL it used to redirect from.
- Dev/peer tooling refreshed: `ai` 7.0.87, `pg-boss` 12.29.0, `zod` 4.5.4, `@ai-sdk/provider`
  4.0.9, `@types/node` 26.4.0, `@types/pg` 8.23.1, `@changesets/cli` 3.0.1,
  `simple-git-hooks` 2.14.0, `vitest` 4.1.11. Published peer ranges are unchanged, so nothing
  is required of consumers.
- Docs and examples: the diamond DAG's first step reads `fetchRecord`, and the site links have
  been rewritten for the new domain.
