// Checkpoint invariants that a refactor of the checkpoint I/O must keep
// (design: docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-
// owner-design.md, "Risks"). Each case fails if one specific rule breaks:
//
//   1. Selective replay: a table changed since the save cold-replays ONLY
//      the views whose source closure reaches it (direct dependents and
//      views stacked on them); every other view restores from the
//      checkpoint. Was covered only by test/python/test_partial_ckpt.py,
//      which is not in ctest.
//   2. Watermark cache: a save that reuses cached source watermarks must
//      still record the table's current content, so "save, edit, save"
//      restores everything from the checkpoint, and an edit after a
//      cached save is still caught as stale.
//   3. Failed save promotes nothing: a save that fails at COMMIT must not
//      record its views as saved. Otherwise the next save "preserves" the
//      rows still in _dbsp_ckpt — written before the view last changed —
//      and the restore comes back silently wrong.
//
// "Restored from checkpoint" is observed through lazy restore (on here):
// a checkpoint-restored view is pending until first touched, a replayed
// view never is.

#include "catch.hpp"
#include "../test_helpers.hpp"

#include <string>

using namespace dbsp_test;

namespace {

// The view's rows equal plain SQL's, compared in a total order.
void requireViewMatches(DuckDBTestHarness &db, const std::string &view,
                        const std::string &sql) {
  auto actual =
      db.query("SELECT * FROM dbsp_query('" + view + "') ORDER BY ALL");
  auto expected = db.query("SELECT * FROM (" + sql + ") ORDER BY ALL");
  INFO("view " << view);
  REQUIRE_FALSE(actual->HasError());
  REQUIRE_FALSE(expected->HasError());
  REQUIRE(actual->ColumnCount() == expected->ColumnCount());
  REQUIRE(actual->RowCount() == expected->RowCount());
  const auto actual_cells = actual->Collection().GetRows();
  const auto expected_cells = expected->Collection().GetRows();
  for (size_t r = 0; r < expected->RowCount(); r++) {
    for (size_t c = 0; c < expected->ColumnCount(); c++) {
      INFO("row " << r << " col " << c);
      REQUIRE(actual_cells.GetValue(c, r).ToString() ==
              expected_cells.GetValue(c, r).ToString());
    }
  }
}

const char *kSqlA = "SELECT grp, SUM(v) AS s FROM ta GROUP BY grp";
const char *kSqlB = "SELECT grp, COUNT(*) AS n FROM tb GROUP BY grp";
const char *kSqlAOnA = "SELECT grp, s FROM va WHERE s > 15";
const char *kSqlAOnATruth =
    "SELECT grp, s FROM (SELECT grp, SUM(v) AS s FROM ta GROUP BY grp) "
    "WHERE s > 15";

// Two independent tracked tables; va/vb over each, va2 stacked on va.
void setupTwoTables(DuckDBTestHarness &db) {
  db.exec("SELECT * FROM dbsp_lazy_restore(true)");
  db.exec("CREATE TABLE ta (grp VARCHAR, v INTEGER)");
  db.exec("CREATE TABLE tb (grp VARCHAR, v INTEGER)");
  db.exec("SELECT * FROM dbsp_track('ta')");
  db.exec("SELECT * FROM dbsp_track('tb')");
  db.exec("INSERT INTO ta VALUES ('x', 10), ('x', 20), ('y', 5)");
  db.exec("INSERT INTO tb VALUES ('p', 1), ('q', 2), ('q', 3)");
  db.exec("SELECT * FROM dbsp_sync()");
  db.exec(std::string("SELECT * FROM dbsp_create_view('va', '") + kSqlA +
          "')");
  db.exec(std::string("SELECT * FROM dbsp_create_view('vb', '") + kSqlB +
          "')");
  db.exec(std::string("SELECT * FROM dbsp_create_view('va2', '") +
          kSqlAOnA + "')");
  REQUIRE(db.manager().get_view("va")->checkpointable());
  REQUIRE(db.manager().get_view("vb")->checkpointable());
  REQUIRE(db.manager().get_view("va2")->checkpointable());
}

std::string save(DuckDBTestHarness &db) {
  auto r = db.query("SELECT * FROM dbsp_save()");
  REQUIRE_FALSE(r->HasError());
  return r->Collection().GetValue(0, 0).ToString();
}

// Drop every view (dependents first) and load them back from storage.
std::string dropAndLoad(DuckDBTestHarness &db) {
  REQUIRE_FALSE(db.query("SELECT dbsp_drop('va2')")->HasError());
  REQUIRE_FALSE(db.query("SELECT dbsp_drop('va')")->HasError());
  REQUIRE_FALSE(db.query("SELECT dbsp_drop('vb')")->HasError());
  auto r = db.query("SELECT * FROM dbsp_load()");
  REQUIRE_FALSE(r->HasError());
  return r->Collection().GetValue(0, 0).ToString();
}

void requireAllCorrect(DuckDBTestHarness &db) {
  requireViewMatches(db, "va", kSqlA);
  requireViewMatches(db, "vb", kSqlB);
  requireViewMatches(db, "va2", kSqlAOnATruth);
}

} // namespace

TEST_CASE("checkpoint invariants: a table changed since the save replays "
          "only its own view closure",
          "[integration][checkpoint][checkpoint_invariants]") {
  DuckDBTestHarness db;
  setupTwoTables(db);
  save(db);
  // A second save with nothing changed reuses every cached watermark; the
  // edit below must still be caught against it.
  save(db);

  db.exec("INSERT INTO ta VALUES ('y', 30)");
  db.exec("SELECT * FROM dbsp_sync('ta')");

  const std::string msg = dropAndLoad(db);
  INFO("load: " << msg);
  REQUIRE(msg.find("1 from checkpoint") != std::string::npos);
  // ta is stale: va replays, and so does va2 through the view-on-view
  // closure. tb is untouched: vb restores from the checkpoint.
  REQUIRE_FALSE(db.manager().is_view_pending("va"));
  REQUIRE_FALSE(db.manager().is_view_pending("va2"));
  REQUIRE(db.manager().is_view_pending("vb"));
  requireAllCorrect(db);
}

TEST_CASE("checkpoint invariants: save, edit, save restores everything from "
          "the checkpoint",
          "[integration][checkpoint][checkpoint_invariants]") {
  DuckDBTestHarness db;
  setupTwoTables(db);
  save(db);

  // The edit dirties ta's cached watermark; the next save must re-scan it.
  // A stale cached watermark would make the load see ta as changed and
  // replay va/va2 instead of restoring them.
  db.exec("INSERT INTO ta VALUES ('y', 30)");
  db.exec("SELECT * FROM dbsp_sync('ta')");
  save(db);

  const std::string msg = dropAndLoad(db);
  INFO("load: " << msg);
  REQUIRE(msg.find("3 from checkpoint") != std::string::npos);
  REQUIRE(db.manager().is_view_pending("va"));
  REQUIRE(db.manager().is_view_pending("va2"));
  REQUIRE(db.manager().is_view_pending("vb"));
  requireAllCorrect(db);
}

TEST_CASE("checkpoint invariants: a save that fails at COMMIT records "
          "nothing as saved",
          "[integration][checkpoint][checkpoint_invariants]") {
  DuckDBTestHarness db;
  setupTwoTables(db);
  save(db); // committed: every view's current generation is saved

  // va (and va2 above it) move past the saved generation.
  db.exec("INSERT INTO ta VALUES ('y', 30)");
  db.exec("SELECT * FROM dbsp_sync('ta')");

  dbsp_native::g_fail_next_ckpt_commit = true;
  const std::string failed = save(db);
  INFO("failed save: " << failed);
  REQUIRE(failed.find("no circuit checkpoint") != std::string::npos);
  REQUIRE(failed.find("checkpoint commit failed") != std::string::npos);
  REQUIRE_FALSE(dbsp_native::g_fail_next_ckpt_commit.load());

  // _dbsp_ckpt still holds the first save's rows. This save must
  // re-serialize va/va2; preserving them would keep pre-edit bytes beside
  // post-edit watermarks, which the load would trust.
  const std::string ok = save(db);
  INFO("good save: " << ok);
  REQUIRE(ok.find("circuit checkpoint") != std::string::npos);
  REQUIRE(ok.find("no circuit checkpoint") == std::string::npos);

  const std::string msg = dropAndLoad(db);
  INFO("load: " << msg);
  REQUIRE(msg.find("3 from checkpoint") != std::string::npos);
  requireAllCorrect(db);
}
