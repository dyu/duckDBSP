// CheckpointStore: the _dbsp_ckpt* table I/O boundary (design:
// docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-owner-
// design.md). These cases drive the store directly, below CDCManager's
// policy, so a storage regression points here instead of surfacing as a
// wrong restore in the differential suites. The manager-level rules the
// store must not break are pinned in test_checkpoint_invariants.cpp.

#include "catch.hpp"
#include "../test_helpers.hpp"

#include <string>

using namespace dbsp_test;
using dbsp_native::CheckpointReadStatus;
using dbsp_native::CheckpointStore;

namespace {

const char *kSqlA = "SELECT grp, SUM(v) AS s FROM ta GROUP BY grp";

// Track `ta` (in `catalog` when given), create `va` over it, and save.
void setupSaved(DuckDBTestHarness &db, const std::string &catalog = "") {
  const std::string t = catalog.empty() ? "ta" : catalog + ".ta";
  db.exec("CREATE TABLE " + t + " (grp VARCHAR, v INTEGER)");
  db.exec("SELECT * FROM dbsp_track('" + t + "')");
  db.exec("INSERT INTO " + t + " VALUES ('x', 10), ('x', 20), ('y', 5)");
  db.exec("SELECT * FROM dbsp_sync()");
  const std::string sql = catalog.empty()
                              ? std::string(kSqlA)
                              : "SELECT grp, SUM(v) AS s FROM " + t +
                                    " GROUP BY grp";
  db.exec("SELECT * FROM dbsp_create_view('va', '" + sql + "')");
  REQUIRE(db.manager().get_view("va")->checkpointable());
  const std::string save =
      catalog.empty() ? "SELECT * FROM dbsp_save()"
                      : "SELECT * FROM dbsp_save(catalog := '" + catalog +
                            "')";
  REQUIRE_FALSE(db.query(save)->HasError());
}

duckdb::ClientContext &ctx(DuckDBTestHarness &db) {
  return *db.conn().context;
}

// The one watermark check whose table key ends in ".ta".
const dbsp_native::TableWatermarkCheck *
checkFor(const dbsp_native::CheckpointReadResult &r) {
  for (const auto &c : r.watermarks) {
    if (c.table.size() >= 3 &&
        c.table.compare(c.table.size() - 3, 3, ".ta") == 0) {
      return &c;
    }
  }
  return nullptr;
}

} // namespace

TEST_CASE("checkpoint store: read returns the saved image",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  const auto r = CheckpointStore::read(ctx(db), "", false);
  INFO("error: " << r.error);
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  REQUIRE(r.image.sinks.count("va") == 1);
  REQUIRE_FALSE(r.image.sinks.at("va").empty());
  REQUIRE(r.image.sql_fingerprints.at("va") == kSqlA);
  REQUIRE_FALSE(r.image.view_arr_id.empty());
  REQUIRE_FALSE(r.image.lazy_blobs);
  const auto *c = checkFor(r);
  REQUIRE(c != nullptr);
  REQUIRE(c->live.has_value());
  REQUIRE(*c->live == c->saved);
}

TEST_CASE("checkpoint store: lazy read returns placeholders",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  const auto r = CheckpointStore::read(ctx(db), "", true);
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  REQUIRE(r.image.lazy_blobs);
  REQUIRE(r.image.sinks.count("va") == 1);
  REQUIRE(r.image.sinks.at("va").empty());
  REQUIRE(r.image.sql_fingerprints.at("va") == kSqlA);
}

TEST_CASE("checkpoint store: no tables means NoCheckpoint",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::NoCheckpoint);
}

TEST_CASE("checkpoint store: version mismatch means NoCheckpoint",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("UPDATE _dbsp_ckpt_version SET version = version + 1");
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::NoCheckpoint);
}

TEST_CASE("checkpoint store: zero checkpointable views means NoCheckpoint",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("DELETE FROM _dbsp_ckpt WHERE kind = 'sink'");
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::NoCheckpoint);
}

TEST_CASE("checkpoint store: an edited source reads as changed",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("INSERT INTO ta VALUES ('y', 30)");
  db.exec("SELECT * FROM dbsp_sync('ta')");
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  const auto *c = checkFor(r);
  REQUIRE(c != nullptr);
  REQUIRE(c->live.has_value());
  REQUIRE_FALSE(*c->live == c->saved);
}

TEST_CASE("checkpoint store: a dropped source reads as live = nullopt",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  REQUIRE_FALSE(db.query("SELECT dbsp_drop('va')")->HasError());
  db.exec("DROP TABLE ta");
  const auto r = CheckpointStore::read(ctx(db), "", false);
  INFO("error: " << r.error);
  REQUIRE(r.status != CheckpointReadStatus::StorageError);
  const auto *c = checkFor(r);
  REQUIRE(c != nullptr);
  REQUIRE_FALSE(c->live.has_value());
}

TEST_CASE("checkpoint store: malformed _dbsp_ckpt is a StorageError",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("CREATE OR REPLACE TABLE _dbsp_ckpt (x INTEGER)");
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::StorageError);
  REQUIRE_FALSE(r.error.empty());
}

TEST_CASE("checkpoint store: attached catalog reads its own tables",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  db.exec("ATTACH ':memory:' AS m");
  setupSaved(db, "m");
  REQUIRE(CheckpointStore::read(ctx(db), "m", false).status ==
          CheckpointReadStatus::Usable);
  REQUIRE(CheckpointStore::read(ctx(db), "", false).status ==
          CheckpointReadStatus::NoCheckpoint);
}
