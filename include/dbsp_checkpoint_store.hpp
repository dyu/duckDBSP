#pragma once

// CheckpointStore: the I/O boundary for DBSP's circuit-checkpoint tables
// (design: docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-
// owner-design.md).
//
//   _dbsp_ckpt          (kind, name, node_id, data)  node/sink/sql/saveid rows
//   _dbsp_ckpt_meta     (table_name, row_count, row_hash)  source watermarks
//   _dbsp_ckpt_version  (version)  kDbspCkptFormatVersion
//
// The store owns the SQL, the row formats and the transactions for these
// tables. It is an I/O owner, not a state owner: it holds no CDCManager,
// view, tracked table or DatabaseInstance between calls, and never decides
// policy. What to save, which views to preserve, which tables are stale and
// what to do on failure all stay with CDCManager, which maps the explicit
// results below onto its own replay/rebuild behavior.

#include "dbsp_checkpoint.hpp"
#include "dbsp_internal_connection.hpp"
#include "dbsp_qualified_name.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dbsp_native {

// A source table's content watermark: COUNT(*) and bit_xor(hash(row)) as
// VARCHAR, exactly as the watermark query returns them.
struct SourceWatermark {
  int64_t count = 0;
  std::string hash;
};

inline bool operator==(const SourceWatermark &a, const SourceWatermark &b) {
  return a.count == b.count && a.hash == b.hash;
}

// One view's persisted rows.
struct ViewBlobs {
  std::string name;
  std::string sql; // definition at save time (the 'sql' fingerprint row)
  std::vector<std::pair<uint64_t, std::vector<uint8_t>>> nodes;
  std::vector<uint8_t> sink;
};

// One _dbsp_ckpt_meta row beside the table's live watermark now.
struct TableWatermarkCheck {
  std::string table;
  SourceWatermark saved;
  std::optional<SourceWatermark> live; // nullopt: the query failed (e.g. dropped)
};

// What a load works from. The store fills the blob, fingerprint, save-id
// and lazy fields; `watermarks` and `stale_tables` are the manager's
// decision from TableWatermarkCheck.
struct CheckpointImage {
  std::unordered_map<std::string,
                     std::unordered_map<uint64_t, std::vector<uint8_t>>>
      nodes;
  std::unordered_map<std::string, std::vector<uint8_t>> sinks;
  // Per-view SQL fingerprint at save time (Finding 1) — compared against
  // the SQL about to be used to cold-create the view; a mismatch means
  // the view's definition changed since this checkpoint was written, so
  // the fast path is declined for that view alone.
  std::unordered_map<std::string, std::string> sql_fingerprints;
  // View-arrangement sidecar identity of this checkpoint (the
  // kind='saveid' row; design note at save_checkpoint's sidecar loop):
  // sidecar files stamped with exactly this (seq, id) pair were written
  // or re-stamped by the save that wrote these view blobs. An empty id
  // (pre-feature checkpoint) just declines view-sidecar adoption.
  int64_t view_arr_seq = -1;
  std::string view_arr_id;
  // Verified per-source watermarks (COUNT, bit_xor(hash) as VARCHAR) —
  // seeds for lazy (deferred) baselines on the load fast path (D3c).
  // Only tables whose live content MATCHED the save-time watermark
  // appear here; changed tables land in stale_tables instead.
  std::unordered_map<std::string, std::pair<int64_t, std::string>>
      watermarks;
  // Tables whose live content diverged from the save-time watermark
  // (crash lost an edit, out-of-band write). O(delta) recovery
  // increment A: a mismatch no longer discards the checkpoint — views
  // whose source closure touches a stale table cold-replay, everything
  // else restores from the checkpoint.
  std::unordered_set<std::string> stale_tables;
  // Phase 3 (reattach): when true, nodes/sinks hold EMPTY placeholder
  // blobs — the bytes stay in _dbsp_ckpt and are fetched per view at
  // realize time. Set only for disk-backed (mv-table) databases, where
  // operator state runs to GBs and eager blob reads dominated reattach.
  bool lazy_blobs = false;
  std::string blob_catalog;
};

enum class CheckpointReadStatus { Usable, NoCheckpoint, StorageError };

struct CheckpointReadResult {
  CheckpointReadStatus status = CheckpointReadStatus::NoCheckpoint;
  CheckpointImage image;
  std::vector<TableWatermarkCheck> watermarks;
  std::string error; // set for StorageError
};

// Test-only fault injection: when set, the next CheckpointWrite::commit()
// rolls back at the point it would COMMIT and reports a commit failure
// (then the flag clears). Lets tests pin that a failed save promotes
// nothing.
inline std::atomic<bool> g_fail_next_ckpt_commit{false};

// The live watermark of `table` on `con`, or nullopt when the query fails
// (the table vanished or cannot be read). The alias must not be shadowable
// by a same-named column: `hash(x)` resolves to the COLUMN x when one
// exists, silently hashing a single column instead of the row and blinding
// the watermark to every other column's updates.
inline std::optional<SourceWatermark>
query_source_watermark(duckdb::Connection &con, const std::string &table) {
  auto wm = con.Query("SELECT COUNT(*), CAST(bit_xor(hash("
                      "__dbsp_wm_row)) AS VARCHAR) FROM " +
                      quote_table_key(table) + " __dbsp_wm_row");
  if (wm->HasError() || wm->RowCount() != 1) {
    return std::nullopt;
  }
  SourceWatermark out;
  out.count = wm->Collection().GetValue(0, 0).GetValue<int64_t>();
  out.hash = wm->Collection().GetValue(1, 0).ToString();
  return out;
}

// The manager's per-table watermark cache, consulted by write_watermarks.
// Both functions run on the caller's thread, inside whatever lock the
// caller holds around write_watermarks; they must not take that lock again.
struct WatermarkCache {
  // True with `out` filled: the table's cached watermark is clean, no scan.
  // False: the callee has armed a scan (or the table is untracked) and the
  // store scans it now.
  std::function<bool(const std::string &table, SourceWatermark &out)> lookup;
  // A watermark the store just scanned.
  std::function<void(const std::string &table, const SourceWatermark &wm)>
      stored;
};

// One open checkpoint write transaction. Every row of a save — version,
// save id, view blobs, source watermarks — lands in it, so a reader sees
// all of a save or none of it. Destroying it uncommitted rolls back. After
// any failure the transaction is already rolled back, error() says why, and
// every further call returns false.
class CheckpointWrite {
public:
  ~CheckpointWrite() {
    if (!finished_) {
      try {
        (*con_owner_).Query("ROLLBACK");
      } catch (...) {
      }
    }
  }
  CheckpointWrite(const CheckpointWrite &) = delete;
  CheckpointWrite &operator=(const CheckpointWrite &) = delete;

  // The view-arrangement sidecar identity of this save (kind='saveid').
  bool write_save_id(int64_t seq, const std::string &id) {
    if (finished_) {
      return false;
    }
    auto r = ins_->Execute("saveid", "", seq, duckdb::Value::BLOB(id));
    return r->HasError() ? fail(r->GetError()) : true;
  }

  // A view's node rows, sink row and SQL fingerprint row.
  bool write_view(const ViewBlobs &v) {
    if (finished_) {
      return false;
    }
    for (const auto &[node_id, blob] : v.nodes) {
      auto r = ins_->Execute("node", v.name, static_cast<int64_t>(node_id),
                             duckdb::Value::BLOB(blob.data(), blob.size()));
      if (r->HasError()) {
        return fail(r->GetError());
      }
    }
    auto r = ins_->Execute("sink", v.name, static_cast<int64_t>(0),
                           duckdb::Value::BLOB(v.sink.data(), v.sink.size()));
    if (r->HasError()) {
      return fail(r->GetError());
    }
    // SQL fingerprint (Finding 1): the view's exact definition at save
    // time, so a load whose _dbsp_views SQL has since diverged can detect
    // the mismatch and decline the fast path for this view. Raw bytes: a
    // BLOB built from a string parses `\` escapes and rejects bytes above
    // 127, so a definition with a regex or non-ASCII literal failed every
    // save.
    auto rsql = ins_->Execute(
        "sql", v.name, static_cast<int64_t>(0),
        duckdb::Value::BLOB(
            reinterpret_cast<duckdb::const_data_ptr_t>(v.sql.data()),
            v.sql.size()));
    return rsql->HasError() ? fail(rsql->GetError()) : true;
  }

  // One _dbsp_ckpt_meta row per table: the cached watermark when `cache`
  // has a clean one, else a scan inside this transaction (reported back
  // through cache.stored). `saved` receives every watermark written.
  bool write_watermarks(
      const std::vector<std::string> &tables, const WatermarkCache &cache,
      std::unordered_map<std::string, SourceWatermark> &saved) {
    if (finished_) {
      return false;
    }
    for (const auto &t : tables) {
      SourceWatermark wm;
      if (!cache.lookup(t, wm)) {
        auto scanned = query_source_watermark(*con_owner_, t);
        if (!scanned) {
          return fail("checkpoint watermark failed for " + t);
        }
        wm = std::move(*scanned);
        cache.stored(t, wm);
      }
      auto r = insm_->Execute(t, duckdb::Value::BIGINT(wm.count), wm.hash);
      if (r->HasError()) {
        return fail(r->GetError());
      }
      saved[t] = std::move(wm);
    }
    return true;
  }

  // COMMIT. False when it failed — then nothing of this save was written.
  bool commit() {
    if (finished_) {
      return false;
    }
    if (g_fail_next_ckpt_commit.exchange(false)) {
      return fail("checkpoint commit failed: injected by test");
    }
    auto committed = (*con_owner_).Query("COMMIT");
    finished_ = true;
    if (!committed || committed->HasError()) {
      error_ = "checkpoint commit failed: " +
               (committed ? committed->GetError() : "null result");
      return false;
    }
    return true;
  }

  const std::string &error() const { return error_; }

private:
  friend class CheckpointStore;

  // WHITELISTED for the DDL AND the per-source watermark reads. The DDL
  // creates DBSP's OWN bookkeeping tables (_dbsp_ckpt*), never the user's,
  // so it never needs to see their uncommitted catalog. The watermarks
  // describe COMMITTED storage and are compared against committed storage
  // at restore, beside circuit state that is likewise committed-only
  // (deltas apply at commit), so a reader that cannot see an open
  // transaction's rows is reading exactly the right thing.
  explicit CheckpointWrite(duckdb::ClientContext &context)
      : con_owner_(context, InternalReadPolicy::AllowedInTxn,
                   "save_checkpoint (bookkeeping DDL + source watermarks)") {}

  bool fail(std::string error) {
    try {
      (*con_owner_).Query("ROLLBACK");
    } catch (...) {
    }
    finished_ = true;
    error_ = std::move(error);
    return false;
  }

  InternalConnection con_owner_;
  duckdb::unique_ptr<duckdb::PreparedStatement> ins_, insm_, insv_;
  bool finished_ = false;
  std::string error_;
};

class CheckpointStore {
public:
  // Open a checkpoint write in `catalog` ("" = default): BEGIN, then
  // _dbsp_ckpt is replaced — or, when `preserve` names views, kept with
  // only those views' rows (a still-pending lazy view's rows are the ONLY
  // copy of its bytes) — _dbsp_ckpt_meta and _dbsp_ckpt_version are
  // replaced, and the format version row is written. nullptr with `error`
  // set on failure, already rolled back.
  static std::unique_ptr<CheckpointWrite>
  begin_write(duckdb::ClientContext &context, const std::string &catalog,
              const std::vector<std::string> &preserve, std::string &error) {
    std::unique_ptr<CheckpointWrite> w(new CheckpointWrite(context));
    auto &con = *w->con_owner_;
    const std::string ckpt_tbl = qualify(catalog, "_dbsp_ckpt");
    const std::string ckpt_meta_tbl = qualify(catalog, "_dbsp_ckpt_meta");
    const std::string ckpt_ver_tbl = qualify(catalog, "_dbsp_ckpt_version");
    con.Query("BEGIN");
    if (preserve.empty()) {
      con.Query("CREATE OR REPLACE TABLE " + ckpt_tbl + " (kind VARCHAR, "
                "name VARCHAR, node_id BIGINT, data BLOB)");
    } else {
      // The table necessarily exists with this schema: the preserved views'
      // placeholders were stashed by a load that read it from this catalog.
      con.Query("CREATE TABLE IF NOT EXISTS " + ckpt_tbl + " (kind "
                "VARCHAR, name VARCHAR, node_id BIGINT, data BLOB)");
      std::string keep;
      for (const auto &n : preserve) {
        keep += (keep.empty() ? "'" : ", '") + escape_sql_literal(n) + "'";
      }
      auto del = con.Query("DELETE FROM " + ckpt_tbl +
                           " WHERE name NOT IN (" + keep + ")");
      if (del->HasError()) {
        w->fail(del->GetError());
        error = w->error();
        return nullptr;
      }
    }
    con.Query("CREATE OR REPLACE TABLE " + ckpt_meta_tbl + " (table_name "
              "VARCHAR, row_count BIGINT, row_hash VARCHAR)");
    // Format version (see dbsp_checkpoint.hpp kDbspCkptFormatVersion): a
    // pre-bump checkpoint lacks this table entirely, so a read treats a
    // missing/mismatched version as no checkpoint at all rather than
    // misparsing node blobs written under an older layout.
    con.Query("CREATE OR REPLACE TABLE " + ckpt_ver_tbl + " (version BIGINT)");
    w->ins_ = con.Prepare("INSERT INTO " + ckpt_tbl + " VALUES ($1, $2, $3, $4)");
    w->insm_ = con.Prepare("INSERT INTO " + ckpt_meta_tbl + " VALUES ($1, $2, $3)");
    w->insv_ = con.Prepare("INSERT INTO " + ckpt_ver_tbl + " VALUES ($1)");
    if (!w->ins_ || w->ins_->HasError() || !w->insm_ || w->insm_->HasError() ||
        !w->insv_ || w->insv_->HasError()) {
      w->fail("checkpoint prepare failed");
      error = w->error();
      return nullptr;
    }
    auto rv = w->insv_->Execute(static_cast<int64_t>(kDbspCkptFormatVersion));
    if (rv->HasError()) {
      w->fail(rv->GetError());
      error = w->error();
      return nullptr;
    }
    return w;
  }

  // Read the checkpoint in `catalog` ("" = the default catalog). With
  // `lazy_blobs`, node/sink entries are empty placeholders (names and ids
  // only) and the bytes stay in _dbsp_ckpt for read_view_blobs.
  //
  // WHITELISTED. Restore-time verification compares the SAVED watermark
  // against live COMMITTED storage, which is the state the saved circuit
  // corresponds to. Reached from load_from_duck_table, which can run inside
  // an open user transaction.
  static CheckpointReadResult read(duckdb::ClientContext &context,
                                   const std::string &catalog,
                                   bool lazy_blobs) {
    CheckpointReadResult out;
    auto storage_error = [&out](std::string error) {
      out.status = CheckpointReadStatus::StorageError;
      out.error = std::move(error);
      return out;
    };
    const std::string ckpt_tbl = qualify(catalog, "_dbsp_ckpt");
    const std::string ckpt_meta_tbl = qualify(catalog, "_dbsp_ckpt_meta");
    const std::string ckpt_ver_tbl = qualify(catalog, "_dbsp_ckpt_version");
    try {
      InternalConnection con_owner(
          context, InternalReadPolicy::AllowedInTxn,
          "checkpoint_valid (restore watermarks)");
      auto &con = *con_owner;
      // Scope the existence check to the right catalog when one is given.
      std::string exists_sql;
      if (catalog.empty()) {
        exists_sql = "SELECT 1 FROM information_schema.tables WHERE table_name = '_dbsp_ckpt'";
      } else {
        exists_sql = "SELECT 1 FROM information_schema.tables WHERE table_catalog = '" +
                     catalog + "' AND table_name = '_dbsp_ckpt'";
      }
      auto exists = con.Query(exists_sql);
      if (exists->HasError() || exists->RowCount() == 0) {
        return out; // NoCheckpoint
      }
      // Format-version gate: a checkpoint saved before the version table
      // existed (or under a different blob layout) must be treated as
      // absent, never fed to node restore_state — see
      // dbsp_checkpoint.hpp kDbspCkptFormatVersion.
      std::string ver_exists_sql;
      if (catalog.empty()) {
        ver_exists_sql = "SELECT 1 FROM information_schema.tables WHERE "
                         "table_name = '_dbsp_ckpt_version'";
      } else {
        ver_exists_sql = "SELECT 1 FROM information_schema.tables WHERE "
                         "table_catalog = '" +
                         catalog + "' AND table_name = '_dbsp_ckpt_version'";
      }
      auto ver_exists = con.Query(ver_exists_sql);
      if (ver_exists->HasError() || ver_exists->RowCount() == 0) {
        return out; // pre-bump checkpoint: no version table at all
      }
      auto ver = con.Query("SELECT version FROM " + ckpt_ver_tbl);
      if (ver->HasError() || ver->RowCount() != 1 ||
          ver->Collection().GetValue(0, 0).GetValue<int64_t>() !=
              kDbspCkptFormatVersion) {
        return out; // version mismatch: discard, rebuild by replay
      }
      auto meta = con.Query(
          "SELECT table_name, row_count, row_hash FROM " + ckpt_meta_tbl);
      if (meta->HasError()) {
        return storage_error(meta->GetError());
      }
      // DuckDB 2.0.0: Collection().GetValue(c, r) materializes every row on
      // each call, so per-row loops read through one GetRows() taken first.
      const auto meta_cells = meta->Collection().GetRows();
      for (duckdb::idx_t i = 0; i < meta->RowCount(); i++) {
        TableWatermarkCheck check;
        check.table = meta_cells.GetValue(0, i).ToString();
        check.saved.count = meta_cells.GetValue(1, i).GetValue<int64_t>();
        check.saved.hash = meta_cells.GetValue(2, i).ToString();
        check.live = query_source_watermark(con, check.table);
        out.watermarks.push_back(std::move(check));
      }
      // View-arrangement sidecar identity (kind='saveid'; design note at
      // save_checkpoint's sidecar loop). A missing row — pre-feature
      // checkpoint — leaves the id empty and adoption simply declines.
      auto sid = con.Query("SELECT node_id, data FROM " + ckpt_tbl +
                           " WHERE kind = 'saveid'");
      if (!sid->HasError() && sid->RowCount() == 1) {
        out.image.view_arr_seq =
            sid->Collection().GetValue(0, 0).GetValue<int64_t>();
        out.image.view_arr_id =
            duckdb::StringValue::Get(sid->Collection().GetValue(1, 0));
      }
      // Phase 3 (reattach): on a disk-backed database operator blobs run to
      // GBs — read only the CATALOG of blobs here (names/ids, no bytes) and
      // fetch per view at realize time. SQL fingerprints are tiny and
      // always eager.
      out.image.lazy_blobs = lazy_blobs;
      out.image.blob_catalog = catalog;
      if (lazy_blobs) {
        auto rows = con.Query("SELECT kind, name, node_id FROM " + ckpt_tbl +
                              " WHERE kind IN ('node', 'sink')");
        if (rows->HasError()) {
          return storage_error(rows->GetError());
        }
        const auto rows_cells = rows->Collection().GetRows();
        for (duckdb::idx_t i = 0; i < rows->RowCount(); i++) {
          const std::string kind = rows_cells.GetValue(0, i).ToString();
          const std::string name = rows_cells.GetValue(1, i).ToString();
          const auto node_id = static_cast<uint64_t>(
              rows_cells.GetValue(2, i).GetValue<int64_t>());
          if (kind == "node") {
            out.image.nodes[name][node_id] = {};
          } else {
            out.image.sinks[name] = {};
          }
        }
        auto fps = con.Query("SELECT name, data FROM " + ckpt_tbl +
                             " WHERE kind = 'sql'");
        if (fps->HasError()) {
          return storage_error(fps->GetError());
        }
        const auto fps_cells = fps->Collection().GetRows();
        for (duckdb::idx_t i = 0; i < fps->RowCount(); i++) {
          out.image.sql_fingerprints[fps_cells.GetValue(0, i).ToString()] =
              duckdb::StringValue::Get(fps_cells.GetValue(1, i));
        }
      } else {
        auto rows =
            con.Query("SELECT kind, name, node_id, data FROM " + ckpt_tbl);
        if (rows->HasError()) {
          return storage_error(rows->GetError());
        }
        const auto rows_cells = rows->Collection().GetRows();
        for (duckdb::idx_t i = 0; i < rows->RowCount(); i++) {
          const std::string kind = rows_cells.GetValue(0, i).ToString();
          const std::string name = rows_cells.GetValue(1, i).ToString();
          const auto node_id = static_cast<uint64_t>(
              rows_cells.GetValue(2, i).GetValue<int64_t>());
          const auto blob_str =
              duckdb::StringValue::Get(rows_cells.GetValue(3, i));
          std::vector<uint8_t> blob(blob_str.begin(), blob_str.end());
          if (kind == "node") {
            out.image.nodes[name][node_id] = std::move(blob);
          } else if (kind == "sink") {
            out.image.sinks[name] = std::move(blob);
          } else if (kind == "sql") {
            out.image.sql_fingerprints[name] =
                std::string(blob.begin(), blob.end());
          }
        }
      }
      out.status = out.image.sinks.empty() ? CheckpointReadStatus::NoCheckpoint
                                           : CheckpointReadStatus::Usable;
      return out;
    } catch (const std::exception &e) {
      return storage_error(e.what());
    } catch (...) {
      return storage_error("unknown error reading checkpoint");
    }
  }

  // One view's node + sink rows from <catalog>._dbsp_ckpt, read on the
  // private instance `db`. True with whatever rows exist (possibly none);
  // false on any error. Per-view `name =` probes beat one big IN-list query:
  // measured at the 60emp reattach, 141 filtered probes cost 1.3s total
  // while a 110-name IN scan cost 2.0s alone (the equality filter prunes
  // row groups before the blob column is touched; the IN filter does not).
  // Touches only `out`, on a fresh connection, so callers may run it from
  // worker threads.
  static bool read_view_blobs(duckdb::DatabaseInstance &db,
                              const std::string &catalog,
                              const std::string &view, ViewBlobs &out) {
    try {
      InternalQueryGuard fetch_guard;
      duckdb::Connection fcon(db);
      auto rows = fcon.Query("SELECT kind, node_id, data FROM " +
                             qualify(catalog, "_dbsp_ckpt") +
                             " WHERE name = '" + escape_sql_literal(view) +
                             "' AND kind IN ('node', 'sink')");
      if (rows->HasError()) {
        return false;
      }
      const auto rows_cells = rows->Collection().GetRows();
      for (duckdb::idx_t i = 0; i < rows->RowCount(); i++) {
        const auto blob_str =
            duckdb::StringValue::Get(rows_cells.GetValue(2, i));
        std::vector<uint8_t> blob(blob_str.begin(), blob_str.end());
        if (rows_cells.GetValue(0, i).ToString() == "node") {
          out.nodes.emplace_back(
              static_cast<uint64_t>(
                  rows_cells.GetValue(1, i).GetValue<int64_t>()),
              std::move(blob));
        } else {
          out.sink = std::move(blob);
        }
      }
      return true;
    } catch (...) {
      return false;
    }
  }

  // Best-effort: delete a view's rows from the default catalog's
  // _dbsp_ckpt. Ignores every error (including "table does not exist") —
  // persistence is best-effort here. WHITELISTED: DML over DBSP's OWN
  // bookkeeping table, never the user's, so it never needs to see their
  // uncommitted catalog.
  static void erase_view_rows(duckdb::ClientContext &context,
                              const std::string &view) {
    try {
      InternalConnection con_owner(context, InternalReadPolicy::AllowedInTxn,
                                   "erase_persisted_checkpoint_rows");
      auto &con = *con_owner;
      con.Query("DELETE FROM _dbsp_ckpt WHERE name = '" +
                escape_sql_literal(view) + "'");
    } catch (...) {
    }
  }
};

} // namespace dbsp_native
