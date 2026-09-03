---
"octaflow": patch
---

Fix a deadlock on single-connection databases: the first transactional dispatch
created the pg-boss queue (DDL) and read pg-boss's queue cache on a second
connection while the store transaction held the only one. On an embedded PGlite
that wedged the process on the first workflow start.

`Dispatcher` gains an optional `prepare()` — whatever must happen outside a
store transaction before enqueuing inside one. The engine calls it once, before
its first transaction opens, memoised and retried on failure. The pg-boss
dispatcher implements it: queue + dead-letter DDL, then a warm of pg-boss's
queue cache (which `createQueue` evicts rather than fills, and which `send`
otherwise refills on its own connection). A plain `enqueueStep` outside any
transaction still prepares lazily.
