// Engine-assumption canaries. The auto-CDC hooks stand on empirically probed
// DuckDB behaviour that no API contract guarantees. Each TEST_CASE here pins
// ONE assumption by name; on an engine upgrade these fail FIRST, with a
// readable message, instead of surfacing as mystery view corruption in the
// differential suites.
//
// The plan-shape canaries that used to live here (DML row references, the
// UPDATE rowid-last convention, projection-pushdown bindings,
// update_is_del_and_insert, INSERT child width) went with the optimizer plan
// tee they existed for: nothing reads or rewrites a DML plan any more, so an
// engine change to any of those shapes can no longer corrupt a delta.

#include "dbsp_internal_connection.hpp"
#include "../test_helpers.hpp"
#include "catch.hpp"

using namespace dbsp_test;
using namespace duckdb;

TEST_CASE("internal query guard comes from the focused boundary header",
          "[engine_assumptions][internal_connection][header]") {
  const auto depth_before = dbsp_native::internal_query_depth;
  {
    dbsp_native::InternalQueryGuard guard;
    REQUIRE(dbsp_native::internal_query_depth == depth_before + 1);
  }
  REQUIRE(dbsp_native::internal_query_depth == depth_before);
}

TEST_CASE("canary: autocommit hook ordering", "[engine_assumptions]") {
  // The fold split (autocommit folds at QueryBegin, explicit txn at
  // QueryEnd, QueryEnd skips autocommit) and the choice of
  // TransactionCommit as the apply point both depend on: an autocommit
  // statement having an ACTIVE transaction with IsAutoCommit() true at
  // BOTH QueryBegin and QueryEnd, and its TransactionCommit hook firing
  // mid-statement (before QueryEnd).
  struct ProbeState : public ClientContextState {
    bool begin_active = false, begin_auto = false;
    bool end_active = false, end_auto = false;
    int commit_before_end = -1; // 1 = commit hook saw no QueryEnd yet
    bool saw_end = false;
    void QueryBegin(ClientContext &context) override {
      begin_active = context.transaction.HasActiveTransaction();
      begin_auto = context.transaction.IsAutoCommit();
    }
    void QueryEnd(ClientContext &context,
                  optional_ptr<ErrorData> error) override {
      saw_end = true;
      end_active = context.transaction.HasActiveTransaction();
      end_auto = context.transaction.IsAutoCommit();
    }
    void TransactionCommit(MetaTransaction &, ClientContext &) override {
      commit_before_end = saw_end ? 0 : 1;
    }
  };
  DuckDBTestHarness db;
  db.exec("CREATE TABLE ca (id INTEGER, val INTEGER, name VARCHAR)");
  db.exec("INSERT INTO ca VALUES (1, 10, 'x'), (2, 20, 'y')");
  auto probe = db.conn().context->registered_state->GetOrCreate<ProbeState>(
      "dbsp_canary_probe");
  db.exec("INSERT INTO ca VALUES (500, 5, 'p')");
  // QueryBegin: transaction active, autocommit — this is why the fold and
  // the trigger sweep both run there
  REQUIRE(probe->begin_active);
  REQUIRE(probe->begin_auto);
  // TransactionCommit fires MID-statement, before QueryEnd — buffered
  // deltas apply there
  REQUIRE(probe->commit_before_end == 1);
  // QueryEnd: the transaction is GONE — nothing may fold or resolve
  // catalog entries here for autocommit statements
  REQUIRE_FALSE(probe->end_active);
}

TEST_CASE("internal connection keeps hooks suppressed through teardown",
          "[engine_assumptions][internal_connection]") {
  struct TeardownProbe : ClientContextState {
    bool *guarded = nullptr;
    ~TeardownProbe() override {
      if (guarded) {
        *guarded = dbsp_native::internal_query_depth > 0;
      }
    }
  };
  DuckDBTestHarness db;
  const int before = dbsp_native::internal_query_depth;
  bool guarded = false;
  {
    dbsp_native::InternalConnection owner(
        *db.conn().context, dbsp_native::InternalReadPolicy::Forbidden,
        "lifecycle canary");
    REQUIRE(dbsp_native::internal_query_depth == before + 1);
    auto probe = owner->context->registered_state->GetOrCreate<TeardownProbe>(
        "dbsp_internal_teardown_probe");
    probe->guarded = &guarded;
    REQUIRE_FALSE(owner->Query("SELECT 1")->HasError());
  }
  REQUIRE(guarded);
  REQUIRE(dbsp_native::internal_query_depth == before);
}

TEST_CASE("internal connection preserves explicit transaction policy",
          "[engine_assumptions][internal_connection]") {
  DuckDBTestHarness db;
  db.exec("BEGIN");
  const int before = dbsp_native::internal_query_depth;
  auto forbidden = [&] {
    dbsp_native::InternalConnection owner(
        *db.conn().context, dbsp_native::InternalReadPolicy::Forbidden,
        "policy canary");
  };
  if (dbsp_native::strict_internal_query()) {
    REQUIRE_THROWS_WITH(forbidden(), Catch::Contains("policy canary"));
  } else {
    REQUIRE_NOTHROW(forbidden());
  }
  REQUIRE(dbsp_native::internal_query_depth == before);
  {
    dbsp_native::InternalConnection owner(
        *db.conn().context, dbsp_native::InternalReadPolicy::AllowedInTxn,
        "committed bookkeeping is deliberately independent");
    REQUIRE_FALSE(owner->Query("SELECT 1")->HasError());
  }
  REQUIRE(dbsp_native::internal_query_depth == before);
  db.exec("ROLLBACK");
}

TEST_CASE("plan keep-alive owns the registered internal context",
          "[engine_assumptions][internal_connection][plan_keepalive]") {
  DuckDBTestHarness db;
  db.exec("CREATE TABLE retained_source (id INTEGER)");
  auto &context = *db.conn().context;
  auto &database = DatabaseInstance::GetDatabase(context);
  auto &registry = dbsp_native::get_instance_registry();
  const auto before = registry.internal_count(&database);
  const auto depth_before = dbsp_native::internal_query_depth;
  std::unique_ptr<dbsp_native::NativeMaterializedView> view;

  {
    dbsp_native::InternalQueryGuard query_guard;
    auto translated = dbsp_native::PlanTranslator::translate(
        context, "retained_view", "SELECT id FROM retained_source");
    REQUIRE(translated.view);
    view = std::move(translated.view);
  }

  REQUIRE(dbsp_native::internal_query_depth == depth_before);
  REQUIRE(registry.internal_count(&database) == before + 1);
  view.reset();
  REQUIRE(registry.internal_count(&database) == before);
}

TEST_CASE("retained connection scopes the guard and owns registry membership",
          "[engine_assumptions][internal_connection][retained]") {
  DuckDBTestHarness db;
  auto &context = *db.conn().context;
  auto &database = DatabaseInstance::GetDatabase(context);
  auto &registry = dbsp_native::get_instance_registry();
  const auto before = registry.internal_count(&database);
  const auto depth_before = dbsp_native::internal_query_depth;
  std::unique_ptr<dbsp_native::RetainedInternalConnection> owner;
  bool teardown_guarded = false;

  struct TeardownProbe : ClientContextState {
    bool *guarded = nullptr;
    ~TeardownProbe() override {
      if (guarded) {
        *guarded = dbsp_native::internal_query_depth > 0;
      }
    }
  };

  {
    dbsp_native::InternalQueryGuard query_guard;
    owner = std::make_unique<dbsp_native::RetainedInternalConnection>(
        dbsp_native::RetainedInternalConnection::open(
            context, dbsp_native::InternalReadPolicy::AllowedInTxn,
            "retained lifecycle canary"));
    auto probe = (*owner)->context->registered_state->GetOrCreate<TeardownProbe>(
        "dbsp_retained_internal_teardown_probe");
    probe->guarded = &teardown_guarded;
    REQUIRE_FALSE((*owner)->Query("SELECT 1")->HasError());
  }

  REQUIRE(dbsp_native::internal_query_depth == depth_before);
  REQUIRE(registry.internal_count(&database) == before + 1);
  auto *internal_context = (*owner)->context.get();
  REQUIRE(registry.is_internal(internal_context));
  owner.reset();
  REQUIRE(teardown_guarded);
  REQUIRE_FALSE(registry.is_internal(internal_context));
  REQUIRE(registry.internal_count(&database) == before);

  db.exec("BEGIN TRANSACTION");
  auto forbidden = [&] {
    auto rejected = dbsp_native::RetainedInternalConnection::open(
        context, dbsp_native::InternalReadPolicy::Forbidden,
        "retained policy canary");
  };
  if (dbsp_native::strict_internal_query()) {
    REQUIRE_THROWS_WITH(forbidden(), Catch::Contains("retained policy canary"));
  } else {
    REQUIRE_NOTHROW(forbidden());
  }
  REQUIRE(dbsp_native::internal_query_depth == depth_before);
  db.exec("ROLLBACK");
}

TEST_CASE("view-definition load preserves an open user transaction",
          "[engine_assumptions][internal_connection][persistence]") {
  DuckDBTestHarness db;
  db.exec("CREATE TABLE _dbsp_views (name VARCHAR, sql VARCHAR, "
          "sources VARCHAR, created_at BIGINT)");
  db.exec("CREATE TABLE __dbsp_mv_meta (view_name VARCHAR)");
  db.exec("BEGIN TRANSACTION");

  REQUIRE(db.manager().load_from_duck_table(*db.conn().context));
  REQUIRE(db.conn().context->transaction.HasActiveTransaction());
  db.exec("ROLLBACK");
}
