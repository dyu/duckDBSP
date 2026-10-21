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

#include <cstdint>
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

class CheckpointStore {
public:
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
        check.live = live_watermark(con, check.table);
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

private:
  // The live watermark of `table` on `con`, or nullopt when the query
  // fails (the table vanished or cannot be read). The alias must not be
  // shadowable by a same-named column: `hash(x)` resolves to the COLUMN x
  // when one exists, silently hashing a single column instead of the row.
  static std::optional<SourceWatermark> live_watermark(duckdb::Connection &con,
                                                       const std::string &table) {
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
};

} // namespace dbsp_native
