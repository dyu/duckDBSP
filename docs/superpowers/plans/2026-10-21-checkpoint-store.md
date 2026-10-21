# CheckpointStore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move all `_dbsp_ckpt*` table I/O out of `CDCManager` (`include/dbsp_cdc.hpp`) into a focused `CheckpointStore` header, with no behavior change.

**Architecture:** A new header `include/dbsp_checkpoint_store.hpp` owns checkpoint SQL, row formats and the write transaction. It depends on DuckDB, `dbsp_internal_connection.hpp`, `dbsp_qualified_name.hpp` and `dbsp_checkpoint.hpp` only. It never includes `dbsp_cdc.hpp`, retains no manager or instance state between calls, and never writes `last_error_`. `CDCManager` keeps every policy decision: what to save, preserve, mark stale, stash, realize, and when to promote generations. The save becomes an RAII `CheckpointWrite` transaction object, so the manager keeps holding `struct_mutex_` only around the watermark phase, exactly as today.

**Tech Stack:** C++17, DuckDB v2.0.0 C++ API, Catch2 (ctest from `test/build_test`).

**Spec:** `docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-owner-design.md` (approved as written, 2026-10-21). Safety net already landed: `test/integration/test_checkpoint_invariants.cpp` (df39699).

## Global Constraints

- Public save/load results, error messages surfaced through `last_error_`, and whether persistence errors are fatal: all unchanged.
- SQL text, table schemas (`_dbsp_ckpt(kind VARCHAR, name VARCHAR, node_id BIGINT, data BLOB)`, `_dbsp_ckpt_meta(table_name VARCHAR, row_count BIGINT, row_hash VARCHAR)`, `_dbsp_ckpt_version(version BIGINT)`) and the watermark query (`SELECT COUNT(*), CAST(bit_xor(hash(__dbsp_wm_row)) AS VARCHAR) FROM <t> __dbsp_wm_row`) stay byte-identical.
- Checkpoint blobs, SQL fingerprints, save id, source watermarks and version row are written in ONE transaction. Generations and save identity are promoted only after `commit()` returns true.
- Bookkeeping connections keep `InternalConnection(context, InternalReadPolicy::AllowedInTxn, <same site string as today>)`. Reads on the private `DatabaseInstance` (`mv_db_`) keep a scoped `InternalQueryGuard` + `duckdb::Connection`.
- The store must not call back into the manager while holding any store-internal lock (it has none), and watermark callbacks run on the caller's thread inside the caller's existing `struct_mutex_` shared hold. Never take `struct_mutex_` inside a callback.
- Per-view blob fetches keep the per-view `WHERE name = '<v>' AND kind IN ('node', 'sink')` form. It is measured faster than an IN-list (comment at `fetch_pending_blobs`).
- Out of scope: `_dbsp_views`, `__dbsp_mv_meta`, `__mv_*`, sidecar files, spill indexes, blob codecs.
- Builds: `cmake --build . -j 8` only. ctest only from `test/build_test`. One full ctest at the end (Task 4); focused targets per task.
- After any DuckDB result loop: read cells through one `Collection().GetRows()` taken before the loop (2.0.0 `GetValue` re-materializes per call).

## Review Focus

1. **Checkpoint in an ATTACHed catalog** (`dbsp_save('m')`-style catalog arg): reads and writes must hit `"m"."_dbsp_ckpt"`, never the default catalog. Test in Task 1 and Task 3.
2. **A checkpoint with zero checkpointable views:** read must report `NoCheckpoint` (today `!out.sinks.empty()` gates it), not `Usable` with nothing in it. Test in Task 1.
3. **A source table dropped after the save:** its live watermark query errors, so it must surface as `live = nullopt` and therefore stale. It must not throw or poison the whole read. Test in Task 1.
4. **A failed commit leaves the previous checkpoint intact and readable.** Test in Task 3.
5. **A malformed `_dbsp_ckpt` table** (wrong columns): the read must return `StorageError`, which the manager maps to replay. It must never be treated as an empty valid image. Test in Task 1.

Residual, review-only: a concurrent writer racing the watermark cache (`wm_begin_scan` must still run before the scan query; Task 3 keeps the call order).

---

### Task 1: Store header, types, and the checkpoint read path

**Files:**
- Create: `include/dbsp_checkpoint_store.hpp`
- Modify: `include/dbsp_qualified_name.hpp`. Move `qualify()` here from `include/dbsp_cdc.hpp:311` unchanged, and add `escape_sql_literal()`.
- Modify: `include/dbsp_cdc.hpp`:
  - delete `qualify()` at :311;
  - `CDCManager::escape_string` delegates to `escape_sql_literal`;
  - replace the `CkptData` struct (:1321-1358) with `using CkptData = CheckpointImage;`;
  - `checkpoint_valid()` (:1361-1508) becomes a thin mapper over `CheckpointStore::read`.
- Create: `test/integration/test_checkpoint_store.cpp`
- Modify: `test/CMakeLists.txt`. Add `add_dbsp_integration_test(checkpoint_store integration/test_checkpoint_store.cpp)` after the `checkpoint_invariants` line.

**Interfaces:**
- Produces, in `include/dbsp_qualified_name.hpp`, namespace `dbsp_native`:
  - `inline std::string qualify(const std::string &cat, const std::string &tbl)` (moved, body unchanged)
  - `inline std::string escape_sql_literal(const std::string &s)` (doubles `'`; body of today's `escape_string`)
- Produces, in `include/dbsp_checkpoint_store.hpp`, namespace `dbsp_native`:
  ```cpp
  struct SourceWatermark { int64_t count = 0; std::string hash; };
  inline bool operator==(const SourceWatermark &a, const SourceWatermark &b);
  struct ViewBlobs {                       // one view's persisted rows
    std::string name;
    std::string sql;                       // fingerprint row ('sql')
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> nodes;
    std::vector<uint8_t> sink;
  };
  struct TableWatermarkCheck {             // one _dbsp_ckpt_meta row
    std::string table;
    SourceWatermark saved;
    std::optional<SourceWatermark> live;   // nullopt: query failed (e.g. table gone)
  };
  struct CheckpointImage {                 // == today's CkptData fields, same names/types
    // nodes, sinks, sql_fingerprints, view_arr_seq (-1), view_arr_id,
    // watermarks, stale_tables, lazy_blobs, blob_catalog
  };
  enum class CheckpointReadStatus { Usable, NoCheckpoint, StorageError };
  struct CheckpointReadResult {
    CheckpointReadStatus status = CheckpointReadStatus::NoCheckpoint;
    CheckpointImage image;                 // nodes/sinks/fingerprints/save id/lazy fields only
    std::vector<TableWatermarkCheck> watermarks;
    std::string error;                     // set for StorageError
  };
  class CheckpointStore {
  public:
    static CheckpointReadResult read(duckdb::ClientContext &context,
                                     const std::string &catalog,
                                     bool lazy_blobs);
  };
  ```
- `read` status mapping, reproducing today's `false` returns exactly:
  - no `_dbsp_ckpt`, no `_dbsp_ckpt_version`, or version row count ≠ 1 or ≠ `kDbspCkptFormatVersion`: `NoCheckpoint`;
  - meta query error, blob/catalog query error, fingerprint query error, or any exception: `StorageError`;
  - no sinks after reading: `NoCheckpoint`;
  - otherwise `Usable`.

  The `_dbsp_ckpt_meta` read becomes `watermarks`, one entry per row, with `live` from the watermark query on the same connection. The existence checks keep the catalog-scoped `information_schema.tables` SQL.
- Manager mapping in `checkpoint_valid(context, out, catalog)`: call `read(context, catalog, mv_tables_enabled_.load())`. Return `false` unless `Usable`. On `Usable`, `out = image`, then for each check put `t` in `out.stale_tables` if `!live || !(*live == saved)`, else set `out.watermarks[t] = {saved.count, saved.hash}`.

- [ ] **Step 1: Write the failing tests** in `test/integration/test_checkpoint_store.cpp` (fixture style as in `test_checkpoint_invariants.cpp`: track `ta`, create `va` = `SELECT grp, SUM(v) AS s FROM ta GROUP BY grp`, then `dbsp_save()`):
  - `"checkpoint store: read returns the saved image"`: `status == Usable`; `image.sinks.count("va") == 1`; `image.sql_fingerprints.at("va") == "SELECT grp, SUM(v) AS s FROM ta GROUP BY grp"`; `!image.view_arr_id.empty()`; one watermark check whose `table` is `ta`'s canonical `catalog.schema.table` key, with `live.has_value() && *live == saved`.
  - `"checkpoint store: lazy read returns placeholders"`: `read(ctx, "", true)`; `image.lazy_blobs`; `image.sinks.at("va").empty()`; fingerprints still present.
  - `"checkpoint store: no tables means NoCheckpoint"`: fresh harness, no save, so `NoCheckpoint`.
  - `"checkpoint store: version mismatch means NoCheckpoint"`: after save, `UPDATE _dbsp_ckpt_version SET version = version + 1` gives `NoCheckpoint`.
  - `"checkpoint store: zero checkpointable views means NoCheckpoint"`: delete the `sink` rows from `_dbsp_ckpt`, then `NoCheckpoint` (Review Focus 2).
  - `"checkpoint store: an edited source reads as changed"`: after save, `INSERT INTO ta ...` and `dbsp_sync('ta')`, then `*live != saved` for `ta`.
  - `"checkpoint store: a dropped source reads as live = nullopt"`: after save, untrack and drop `ta` (drop the view first), then the check for `ta` has `!live.has_value()` and `status` is not `StorageError` (Review Focus 3).
  - `"checkpoint store: malformed _dbsp_ckpt is a StorageError"`: after save, `CREATE OR REPLACE TABLE _dbsp_ckpt (x INTEGER)`, then `StorageError` with non-empty `error` (Review Focus 5).
  - `"checkpoint store: attached catalog reads its own tables"`: `ATTACH ':memory:' AS m`, track and view as above, `dbsp_save` to catalog `m` (use the `dbsp_save` catalog argument the extension exposes; see `src/dbsp_extension.cpp` save_func `catalog` kwarg). `read(ctx, "m", false)` is `Usable` and `read(ctx, "", false)` is `NoCheckpoint` (Review Focus 1).

- [ ] **Step 2: Run them to verify they fail**
  Run: `cd test/build_test && cmake .. >/dev/null && cmake --build . -j 8 --target test_checkpoint_store`
  Expected: compile error, `dbsp_checkpoint_store.hpp` not found or `CheckpointStore` undeclared.

- [ ] **Step 3: Implement the header, the moves, and the thin `checkpoint_valid`** per the Interfaces block. Lift `read`'s body from today's `checkpoint_valid`, replacing early `return false` with the status mapping. `dbsp_cdc.hpp` includes `dbsp_checkpoint_store.hpp`.

- [ ] **Step 4: Run the new and existing checkpoint targets**
  Run: `cmake --build . -j 8 --target test_checkpoint_store test_checkpoint_invariants test_lazy_restore test_join_checkpoint test_view_arr_sidecars && for t in checkpoint_store checkpoint_invariants lazy_restore join_checkpoint view_arr_sidecars; do ./test_$t || break; done`
  Expected: every binary ends `All tests passed`.

- [ ] **Step 5: Commit**
  ```bash
  git add include/dbsp_checkpoint_store.hpp include/dbsp_qualified_name.hpp include/dbsp_cdc.hpp test/integration/test_checkpoint_store.cpp test/CMakeLists.txt
  git commit -m "refactor(ckpt): CheckpointStore owns the checkpoint read path"
  ```

### Task 2: Per-view blob fetch and checkpoint-row erase

**Files:**
- Modify: `include/dbsp_checkpoint_store.hpp`
- Modify: `include/dbsp_cdc.hpp`:
  - the cross-catalog pending copy inside `save_checkpoint` (:786-830);
  - `fetch_pending_blobs` (:1747-1778);
  - `erase_persisted_checkpoint_rows` (:3587-3601).
- Test: `test/integration/test_checkpoint_store.cpp`

**Interfaces:**
- Consumes: `ViewBlobs`, `escape_sql_literal`, `qualify` (Task 1).
- Produces:
  ```cpp
  // Node + sink rows of one view from <catalog>._dbsp_ckpt on the private
  // instance. True with whatever rows exist (possibly none); false on error.
  static bool read_view_blobs(duckdb::DatabaseInstance &db,
                              const std::string &catalog,
                              const std::string &view, ViewBlobs &out);
  // Best-effort: DELETE FROM _dbsp_ckpt (default catalog, unqualified, as
  // today) WHERE name = '<escaped view>'; swallows every error.
  static void erase_view_rows(duckdb::ClientContext &context,
                              const std::string &view);
  ```
- `read_view_blobs` opens `InternalQueryGuard` + `duckdb::Connection(db)`. `erase_view_rows` opens `InternalConnection(context, AllowedInTxn, "erase_persisted_checkpoint_rows")`.
- Manager call sites:
  - `fetch_pending_blobs` copies `out.nodes` into `pv.nodes` (map by node id) and `out.sink` into `pv.sink`, then sets `pv.lazy_from_table = false` only on success;
  - the save-time cross-catalog copy pushes `ViewBlobs` into its `view_blobs` (whose element type becomes `ViewBlobs`, replacing the local `ViewCkpt`);
  - `erase_persisted_checkpoint_rows` forwards to `erase_view_rows`.

  `erase_view_rows` escapes the name. Today it concatenates the raw name, but view names are validated identifiers, so this is identical for every reachable input; note it in the commit message.

- [ ] **Step 1: Write the failing tests**
  - `"checkpoint store: read_view_blobs matches the eager image"`: after save, `read_view_blobs(DatabaseInstance::GetDatabase(ctx), "", "va", b)` is true; `b.sink == read(ctx,"",false).image.sinks.at("va")`; same node ids and bytes as `image.nodes.at("va")`.
  - `"checkpoint store: read_view_blobs of an unknown view is empty, not an error"`: true, `b.nodes.empty() && b.sink.empty()`.
  - `"checkpoint store: erase_view_rows removes only that view"`: create a second view `vb`, save, `erase_view_rows(ctx, "va")`. `SELECT count(*) FROM _dbsp_ckpt WHERE name = 'va'` is 0 and the same for `vb` is > 0.

- [ ] **Step 2: Run to verify they fail**
  Run: `cmake --build . -j 8 --target test_checkpoint_store`
  Expected: compile error, `read_view_blobs` / `erase_view_rows` not members of `CheckpointStore`.

- [ ] **Step 3: Implement both statics and switch the three manager sites.**

- [ ] **Step 4: Run** the Task 1 Step 4 command.
  Expected: all `All tests passed`. `test_lazy_restore`'s "missing node blob" and "corrupt … blob" cases cover the fetch-failure path.

- [ ] **Step 5: Commit**
  ```bash
  git add include/dbsp_checkpoint_store.hpp include/dbsp_cdc.hpp test/integration/test_checkpoint_store.cpp
  git commit -m "refactor(ckpt): CheckpointStore owns per-view blob fetch and row erase"
  ```

### Task 3: The write transaction

**Files:**
- Modify: `include/dbsp_checkpoint_store.hpp`. Add `CheckpointWrite` and `WatermarkCache`, and move `g_fail_next_ckpt_commit` here from `dbsp_cdc.hpp` (same name and namespace; the test keeps compiling unchanged).
- Modify: `include/dbsp_cdc.hpp`. In `save_checkpoint` (:727-1320), replace the region from `const std::string ckpt_tbl = ...` through the COMMIT check with `CheckpointWrite` calls. Everything before (serialization, preserve decisions) and after (promotion, spill-index and sidecar post-commit work) stays in the manager.
- Test: `test/integration/test_checkpoint_store.cpp`

**Interfaces:**
- Consumes: `ViewBlobs`, `SourceWatermark` (Task 1).
- Produces:
  ```cpp
  // Manager-owned watermark cache, invoked by write_watermarks on the
  // caller's thread under the caller's struct_mutex_ shared hold.
  struct WatermarkCache {
    // true + out filled: clean cached watermark (no scan). false: the
    // callee has armed a scan (TrackedTable::wm_begin_scan) or the table is
    // untracked; the store then scans.
    std::function<bool(const std::string &table, SourceWatermark &out)> lookup;
    // A watermark the store just scanned (callee: wm_store if tracked).
    std::function<void(const std::string &table, const SourceWatermark &wm)> stored;
  };
  class CheckpointWrite {                  // one open transaction; RAII
  public:
    ~CheckpointWrite();                    // ROLLBACK if not committed
    bool write_save_id(int64_t seq, const std::string &id);
    bool write_view(const ViewBlobs &v);   // node rows, sink row, sql row
    bool write_watermarks(const std::vector<std::string> &tables,
                          const WatermarkCache &cache,
                          std::unordered_map<std::string, SourceWatermark> &saved);
    bool commit();                         // honors g_fail_next_ckpt_commit
    const std::string &error() const;      // exact texts listed below
  };
  // BEGIN; create/replace or preserve-delete _dbsp_ckpt; create/replace
  // _dbsp_ckpt_meta and _dbsp_ckpt_version; prepare inserts; write the
  // version row. nullptr + error on failure (already rolled back).
  static std::unique_ptr<CheckpointWrite>
  begin_write(duckdb::ClientContext &context, const std::string &catalog,
              const std::vector<std::string> &preserve, std::string &error);
  ```
- Error texts copied verbatim from today:
  - `"checkpoint prepare failed"`;
  - `"checkpoint watermark failed for " + t`;
  - `"checkpoint commit failed: injected by test"`;
  - `"checkpoint commit failed: " + <duckdb error>`;
  - otherwise the DuckDB error string.

  The manager assigns `last_error_ = w->error()` (or the `begin_write` error) and returns false at each failure point, exactly where today's code does.
- Manager lock scope: take `std::shared_lock<std::shared_mutex> wm_struct_lock(struct_mutex_)` around `write_watermarks` only, as today.
  - `lookup` does today's `tracked_tables_.find` plus `wm_clean() && !wm_hash().empty()` check, and on miss calls `wm_begin_scan()` before returning false.
  - `stored` calls `wm_store`.
  - The DBSP_TIMING breakdown counters move into the lambdas.
- Promotion (`last_ckpt_saved_count_`, `mark_saved`, `view_saved_generation_`, `view_saved_catalog_`, `view_arr_seq_/id_`) stays after `commit()` returns true. Nothing promotes on any false.

- [ ] **Step 1: Write the failing tests**
  - `"checkpoint store: an uncommitted write rolls back"`: after a save, `begin_write(ctx, "", {}, err)`, `write_view` for a fake `ViewBlobs{"zz", "SELECT 1", {}, {1,2,3}}`, then destroy without `commit`. `read(...).image.sinks.count("zz") == 0` and `sinks.count("va") == 1`.
  - `"checkpoint store: a failed commit leaves the previous checkpoint readable"`: set `g_fail_next_ckpt_commit = true`, begin, write, `commit()` returns false, and `error() == "checkpoint commit failed: injected by test"`. The prior image still reads `Usable` with `va` (Review Focus 4).
  - `"checkpoint store: preserve keeps listed views' rows"`: two views saved; `begin_write(ctx, "", {"va"}, err)`, `write_view` for `vb` only, then commit. Both `va` and `vb` read back, and `va`'s sink bytes are unchanged.
  - `"checkpoint store: write_watermarks uses the cache and reports scans"`: a cache whose `lookup` returns true for `ta` with `{7, "h"}` and false otherwise. After commit, the read's check for `ta` has `saved == {7, "h"}`, and `stored` was called for every other table name passed and never for `ta`.
  - `"checkpoint store: attached catalog write lands in that catalog"`: `begin_write(ctx, "m", {}, err)` → commit → `read(ctx, "m", false)` sees the view and `read(ctx, "", false)` does not (Review Focus 1).

- [ ] **Step 2: Run to verify they fail**
  Run: `cmake --build . -j 8 --target test_checkpoint_store`
  Expected: compile error, `begin_write` / `CheckpointWrite` undeclared.

- [ ] **Step 3: Implement `CheckpointWrite` and `begin_write`, and switch `save_checkpoint`** per the Interfaces block. Order inside the manager stays: begin, save id, views, watermarks (under lock), commit, promotion, post-commit sidecars.

- [ ] **Step 4: Run** the Task 1 Step 4 command.
  Expected: all `All tests passed`. `test_checkpoint_invariants` is the regression gate for selective replay, the watermark cache and failed-save promotion.

- [ ] **Step 5: Commit**
  ```bash
  git add include/dbsp_checkpoint_store.hpp include/dbsp_cdc.hpp test/integration/test_checkpoint_store.cpp
  git commit -m "refactor(ckpt): CheckpointStore owns the checkpoint write transaction"
  ```

### Task 4: Acceptance sweep, docs, full verification

**Files:**
- Modify: `docs/ARCHITECTURE.md`. Add `CheckpointStore` to the component diagram between `CDCManager` and the DuckDB storage it writes `_dbsp_ckpt*` to, with a one-paragraph component description.
- Modify: `include/dbsp_cdc.hpp` header comments that still describe checkpoint SQL living in the manager. Modify `include/dbsp_checkpoint.hpp` (its header names `CDCManager::save_checkpoint` as the writer; it should name `CheckpointStore`).
- Modify: `docs/TESTING.md`. Add a `checkpoint_store` paragraph next to `checkpoint_invariants`.
- Modify: `CHANGELOG.md`. New top entry, ending with the ctest count.
- Modify: `docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-owner-design.md`. Change **Status** to "Implemented 2026-10-21 (plan: docs/superpowers/plans/2026-10-21-checkpoint-store.md)".

- [ ] **Step 1: Centralization check**
  Run: `grep -n "_dbsp_ckpt" include/dbsp_cdc.hpp src/*.cpp`
  Expected: no SQL construction. Comments only; any remaining hit must be a comment line.

- [ ] **Step 2: Isolation check**
  Run: `grep -nE "CDCManager|struct_mutex_|TrackedTable|NativeMaterializedView|views_|last_error_" include/dbsp_checkpoint_store.hpp`
  Expected: no matches outside comments.

- [ ] **Step 3: Update the docs listed above.**

- [ ] **Step 4: Full verification**
  Run:
  - `df -h .` (needs ≥ 6GB);
  - `cd test/build_test && cmake --build . -j 8 && ctest --output-on-failure --timeout 900`;
  - `cd ../.. && bash ./build.sh`;
  - then each `test/python/*.py` via `uv run --isolated --with 'duckdb==2.0.0' --with pyarrow python <script> build/dbsp.duckdb_extension`.

  Expected: `100% tests passed out of 47`; all 30 Python scripts exit 0, `test_partial_ckpt.py` and `test_checkpoint_restore.py` included.

- [ ] **Step 5: `git diff --check`**, then commit
  ```bash
  git add docs/ARCHITECTURE.md docs/TESTING.md CHANGELOG.md include/dbsp_cdc.hpp include/dbsp_checkpoint.hpp docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-owner-design.md
  git commit -m "docs(ckpt): CheckpointStore in architecture, testing and changelog"
  ```
