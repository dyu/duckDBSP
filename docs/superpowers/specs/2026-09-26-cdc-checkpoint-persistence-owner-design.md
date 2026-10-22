# CDC checkpoint persistence owner

**Status:** Implemented 2026-10-21 (plan: docs/superpowers/plans/2026-10-21-checkpoint-store.md).

## Problem

`CDCManager` currently mixes two different kinds of checkpoint work. It decides whether a view is checkpointable, snapshots and serializes live circuits, applies restore data, and manages lazy-restore state. It also constructs SQL for `_dbsp_ckpt`, `_dbsp_ckpt_meta`, and `_dbsp_ckpt_version`, owns their transactions and row formats, checks table watermarks, fetches lazy blobs, and removes obsolete checkpoint rows. The storage operations are interleaved with manager locking, restore policy, and spill-sidecar handling in the 7,600-line manager header.

The coupling makes storage changes hard to review. It also makes a future reconciliation or propagation split riskier because those stages would have to carry DuckDB table details along with manager state.

## Goal

Create a focused `CheckpointStore` boundary for DuckDB circuit-checkpoint table I/O. Keep `CDCManager` responsible for deciding which state may be saved or restored, and keep its baseline and view lifecycle behavior unchanged.

The store is an I/O owner, not another runtime state owner. It should not retain a `CDCManager`, a view, a tracked table, or a `DatabaseInstance` between calls.

## Design

### 1. Add a focused checkpoint-store component

Add `include/dbsp_checkpoint_store.hpp`, following the repository's current header-based component pattern. It defines storage-transfer types and a `CheckpointStore` class that depends on DuckDB and standard-library values, but not on `CDCManager`, `TrackedTable`, `NativeMaterializedView`, view locks, or arrangement implementations.

The transfer types describe only persisted data:

- a view's node blobs, sink blob, and saved SQL fingerprint;
- saved and observed source watermarks;
- the checkpoint format version and per-save arrangement identity;
- names of views whose existing rows must be preserved;
- eager blobs or lazy blob-catalog entries.

The store returns an explicit result that distinguishes usable data, no usable checkpoint, and storage error. The manager maps those outcomes to today's replay/failure behavior. The store does not read or write `CDCManager::last_error_` itself.

### 2. Store all `_dbsp_ckpt*` table operations

Move the SQL construction and execution for these tables into `CheckpointStore`:

- checkpoint write transaction, DDL, prepared inserts, and rollback paths;
- format-version lookup and raw checkpoint-row reads;
- saved-watermark reads and live source-watermark queries;
- eager blob reads and lazy blob-catalog reads;
- per-view lazy blob fetches and cross-catalog pending-blob copies;
- best-effort deletion of a view's checkpoint rows during replacement.

The manager continues to choose view serialization, the views included in a save, pending-view preservation, and which catalog a request targets. It supplies a snapshot DTO to the store and consumes a raw checkpoint DTO when loading.

### 3. Keep checkpoint policy and lifecycle in `CDCManager`

`CDCManager` remains the owner of:

- `checkpointable()` decisions, circuit serialization, sink-result encoding, and its lock-protected snapshot;
- incremental-generation checks and promotion after a successful store commit;
- the watermark-cache decision for each tracked table;
- interpreting version and SQL-fingerprint mismatches;
- deciding which views are stale from observed watermarks and propagating that stale-source closure;
- cold creation, eager restore, lazy stashing/realization, rebuild fallback, and public counters;
- deferred-baseline seeds and the loaded checkpoint identity used by arrangement adoption.

For source watermarks, the manager provides the store with a small callback that preserves the current `TrackedTable` clean-cache/scan behavior. The store owns the transaction-scoped SQL query and rows in `_dbsp_ckpt_meta`; the callback owns lookup, scan-dirty signaling, and cache updates. The callback must be invoked under the same structural-lock protection as the current save path and must not acquire `struct_mutex_` recursively.

The manager may turn a successful raw watermark comparison into a stale-table decision. A changed or missing source table still declines checkpoint use only for views whose source closure reaches it. A storage or decode failure must retain today's replay/rebuild behavior.

### 4. Preserve transaction, policy, and sidecar invariants

- The checkpoint blobs, SQL fingerprints, save identity, source watermarks, and version row are written in one transaction. Cache generations and save identity are promoted only after that transaction commits.
- Same-catalog still-pending lazy views preserve their existing `_dbsp_ckpt` rows without decode/re-encode. Cross-catalog saves copy their persisted bytes; an unavailable copy skips that view for this save as today.
- Lazy load reads only the blob catalog and fingerprints initially. Per-view bytes are fetched only when the manager realizes that view.
- Bookkeeping access continues to use `InternalConnection` with `AllowedInTxn` and its current hook-suppression lifetime. Private-`DatabaseInstance` reads retain a scoped `InternalQueryGuard`.
- Tracked-table spill indexes and shared-arrangement sidecars remain post-commit best-effort work in `CDCManager`. View-arrangement sidecars continue to use the save identity written atomically with the checkpoint; a crash before their post-commit write may cause a safe backfill, never adoption of a mismatched file.
- Missing or mismatched checkpoint versions continue to mean “no usable checkpoint.” SQL fingerprint mismatch remains per-view. The refactor does not change public save/load results or whether persistence errors are fatal.

## Scope and non-goals

**In scope:** the `_dbsp_ckpt*` SQL schema and row format, checkpoint transaction and read helpers, lazy checkpoint-byte access, checkpoint-row deletion, manager-to-store transfer types, and focused checkpoint regression coverage.

**Out of scope for this slice:** baseline reconciliation ownership, view propagation/lifecycle ownership, serialization formats, spill and arrangement sidecar formats, `_dbsp_views` definition persistence, `__dbsp_mv_meta` and `__mv_*` mirroring, generic table/JSON import-export APIs, public SQL behavior, and the other oversized-file findings. `_dbsp_views` has additional create/drop/replace call sites tied to view lifecycle and will need its own boundary decision rather than being partially routed through this checkpoint store.

## Acceptance and verification

- Runtime checkpoint-table SQL and raw `_dbsp_ckpt*` reads/writes reside in `CheckpointStore`; `CDCManager` calls the store using transfer DTOs and contains no checkpoint-table query construction.
- The store has no access to manager maps, manager mutexes, circuit objects, or sidecar implementations.
- Existing behavior is retained for checkpoint format/version fallback, left/right join round trips, SQL-fingerprint mismatch, changed-source selective replay, eager/lazy restore, still-pending saves, lazy fetch failures, and view-arrangement save-ID adoption. Add a changed-source selective-replay regression if the existing suite does not already exercise the stale-source closure; the inspected checkpoint tests did not reveal one.
- A failed write does not promote the manager's saved generations or save identity. A successful write commits checkpoint rows and watermark/version metadata together; post-commit sidecar failure remains a safe cache miss/backfill.
- Run the focused `join_checkpoint`, `lazy_restore`, and `view_arr_sidecars` targets during implementation, then one full CTest run at the end of the overall implementation task. Keep the previously recorded first-slice full-suite result as prior evidence; do not rerun that suite just for this specification.
- Search the source after migration to ensure checkpoint SQL table access is centralized, then run `git diff --check`.

## Risks

- Moving watermark SQL without preserving the manager's clean-cache and dirty-scan handshake could add full-table scans or let a concurrent change leave a stale cached watermark. Keep the callback boundary narrow and its lock contract explicit.
- Moving row writes without keeping generation promotion after `COMMIT` could cause a later save to preserve rows that were never committed.
- Eagerly reading lazy blobs, re-encoding still-pending blobs, or changing save-ID timing could break the memory and arrangement-adoption guarantees. Keep byte ownership and save-ID transaction timing covered by the lazy-restore and sidecar tests.
- Treating a checkpoint read error as an empty-but-valid image could hide a corrupt restore. Preserve current invalid-checkpoint/replay behavior and keep manager-level interpretation separate from store-level SQL results.
