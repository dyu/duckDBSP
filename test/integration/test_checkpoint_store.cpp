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
  // The failed live query must not poison the rest of the read: va's rows
  // are still in _dbsp_ckpt (dbsp_drop does not erase them).
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  REQUIRE(r.image.sinks.count("va") == 1);
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
  // Nothing read before the failure leaks out with the error.
  REQUIRE(r.watermarks.empty());
  REQUIRE(r.image.sinks.empty());
  REQUIRE(r.image.nodes.empty());
  REQUIRE(r.image.sql_fingerprints.empty());
  REQUIRE(r.image.view_arr_id.empty());
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

TEST_CASE("checkpoint store: read_view_blobs matches the eager image",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  const auto image = CheckpointStore::read(ctx(db), "", false).image;
  dbsp_native::ViewBlobs b;
  REQUIRE(CheckpointStore::read_view_blobs(
      duckdb::DatabaseInstance::GetDatabase(ctx(db)), "", "va", b));
  REQUIRE(b.sink == image.sinks.at("va"));
  const auto &nodes = image.nodes.at("va");
  REQUIRE(b.nodes.size() == nodes.size());
  for (const auto &[id, bytes] : b.nodes) {
    REQUIRE(nodes.count(id) == 1);
    REQUIRE(bytes == nodes.at(id));
  }
}

TEST_CASE("checkpoint store: read_view_blobs of an unknown view is empty, "
          "not an error",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  dbsp_native::ViewBlobs b;
  REQUIRE(CheckpointStore::read_view_blobs(
      duckdb::DatabaseInstance::GetDatabase(ctx(db)), "", "nope", b));
  REQUIRE(b.nodes.empty());
  REQUIRE(b.sink.empty());
}

TEST_CASE("checkpoint store: erase_view_rows removes only that view",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("SELECT * FROM dbsp_create_view('vb', "
          "'SELECT grp, COUNT(*) AS n FROM ta GROUP BY grp')");
  REQUIRE_FALSE(db.query("SELECT * FROM dbsp_save()")->HasError());
  CheckpointStore::erase_view_rows(ctx(db), "va");
  auto count = [&db](const std::string &v) {
    return db.query("SELECT count(*) FROM _dbsp_ckpt WHERE name = '" + v + "'")
        ->Collection()
        .GetValue(0, 0)
        .GetValue<int64_t>();
  };
  REQUIRE(count("va") == 0);
  REQUIRE(count("vb") > 0);
}

namespace {

// A view's persisted rows as a ViewBlobs, from an eager read.
dbsp_native::ViewBlobs blobsOf(const dbsp_native::CheckpointImage &image,
                               const std::string &view) {
  dbsp_native::ViewBlobs b;
  b.name = view;
  b.sql = image.sql_fingerprints.at(view);
  for (const auto &[id, bytes] : image.nodes.at(view)) {
    b.nodes.emplace_back(id, bytes);
  }
  b.sink = image.sinks.at(view);
  return b;
}

dbsp_native::ViewBlobs fakeView(const std::string &name) {
  dbsp_native::ViewBlobs b;
  b.name = name;
  b.sql = "SELECT 1";
  b.sink = {1, 2, 3};
  return b;
}

} // namespace

TEST_CASE("checkpoint store: an uncommitted write rolls back",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  {
    std::string err;
    auto w = CheckpointStore::begin_write(ctx(db), "", {}, err);
    REQUIRE(w != nullptr);
    REQUIRE(w->write_view(fakeView("zz")));
  } // destroyed without commit
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  REQUIRE(r.image.sinks.count("zz") == 0);
  REQUIRE(r.image.sinks.count("va") == 1);
}

TEST_CASE("checkpoint store: a failed commit leaves the previous checkpoint "
          "readable",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  std::string err;
  auto w = CheckpointStore::begin_write(ctx(db), "", {}, err);
  REQUIRE(w != nullptr);
  REQUIRE(w->write_view(fakeView("zz")));
  dbsp_native::g_fail_next_ckpt_commit = true;
  REQUIRE_FALSE(w->commit());
  REQUIRE(w->error() == "checkpoint commit failed: injected by test");
  REQUIRE_FALSE(dbsp_native::g_fail_next_ckpt_commit.load());
  w.reset();
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.status == CheckpointReadStatus::Usable);
  REQUIRE(r.image.sinks.count("va") == 1);
  REQUIRE(r.image.sinks.count("zz") == 0);
}

TEST_CASE("checkpoint store: preserve keeps listed views' rows",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("SELECT * FROM dbsp_create_view('vb', "
          "'SELECT grp, COUNT(*) AS n FROM ta GROUP BY grp')");
  REQUIRE_FALSE(db.query("SELECT * FROM dbsp_save()")->HasError());
  const auto before = CheckpointStore::read(ctx(db), "", false).image;
  std::string err;
  auto w = CheckpointStore::begin_write(ctx(db), "", {"va"}, err);
  REQUIRE(w != nullptr);
  REQUIRE(w->write_view(blobsOf(before, "vb")));
  REQUIRE(w->commit());
  w.reset();
  const auto after = CheckpointStore::read(ctx(db), "", false).image;
  REQUIRE(after.sinks.count("va") == 1);
  REQUIRE(after.sinks.count("vb") == 1);
  REQUIRE(after.sinks.at("va") == before.sinks.at("va"));
  REQUIRE(after.sinks.at("vb") == before.sinks.at("vb"));
}

TEST_CASE("checkpoint store: write_watermarks uses the cache and reports "
          "scans",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  setupSaved(db);
  db.exec("CREATE TABLE tb (v INTEGER)");
  db.exec("INSERT INTO tb VALUES (1), (2)");
  const auto prior = CheckpointStore::read(ctx(db), "", false);
  const auto *ta_check = checkFor(prior);
  REQUIRE(ta_check != nullptr);
  const std::string ta = ta_check->table;
  const std::string tb = ta.substr(0, ta.size() - 2) + "tb"; // same schema

  std::vector<std::string> looked_up, stored;
  dbsp_native::WatermarkCache cache;
  cache.lookup = [&](const std::string &t, dbsp_native::SourceWatermark &out) {
    looked_up.push_back(t);
    if (t == ta) {
      out = {7, "h"};
      return true;
    }
    return false;
  };
  cache.stored = [&](const std::string &t, const dbsp_native::SourceWatermark &) {
    stored.push_back(t);
  };

  std::string err;
  auto w = CheckpointStore::begin_write(ctx(db), "", {}, err);
  REQUIRE(w != nullptr);
  REQUIRE(w->write_view(fakeView("zz")));
  std::unordered_map<std::string, dbsp_native::SourceWatermark> saved;
  REQUIRE(w->write_watermarks({ta, tb}, cache, saved));
  REQUIRE(w->commit());
  w.reset();

  REQUIRE(looked_up == std::vector<std::string>{ta, tb});
  REQUIRE(stored == std::vector<std::string>{tb});
  REQUIRE(saved.at(ta) == dbsp_native::SourceWatermark{7, "h"});
  REQUIRE(saved.at(tb).count == 2);
  const auto r = CheckpointStore::read(ctx(db), "", false);
  REQUIRE(r.watermarks.size() == 2);
  const auto *c = checkFor(r);
  REQUIRE(c != nullptr);
  REQUIRE(c->saved == dbsp_native::SourceWatermark{7, "h"});
}

TEST_CASE("checkpoint store: attached catalog write lands in that catalog",
          "[integration][checkpoint][checkpoint_store]") {
  DuckDBTestHarness db;
  db.exec("ATTACH ':memory:' AS m");
  std::string err;
  auto w = CheckpointStore::begin_write(ctx(db), "m", {}, err);
  INFO("error: " << err);
  REQUIRE(w != nullptr);
  REQUIRE(w->write_view(fakeView("zz")));
  REQUIRE(w->commit());
  w.reset();
  const auto m = CheckpointStore::read(ctx(db), "m", false);
  REQUIRE(m.status == CheckpointReadStatus::Usable);
  REQUIRE(m.image.sinks.count("zz") == 1);
  REQUIRE(CheckpointStore::read(ctx(db), "", false).status ==
          CheckpointReadStatus::NoCheckpoint);
}
