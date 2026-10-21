# Internal Connection Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Route every ordinary internal connection opened from a user `ClientContext` through a focused API that applies explicit transaction policy and preserves the distinct lifetimes of short-lived work and planner connections.

**Architecture:** Move `InternalQueryGuard` and `InternalConnection` out of `CDCManager`'s header into `dbsp_internal_connection.hpp`. Add a retained-connection owner for the planner that keeps registry membership through connection teardown, uses a temporary guard while opening and closing, and leaves query guarding to the caller's scope. Migrate persistence, sidecar, and transaction-manager probe opens without changing policy meanings or planner errors.

**Tech Stack:** C++17, DuckDB extension APIs, Catch2, CMake, CTest.

**Spec:** `docs/superpowers/specs/2026-09-26-internal-connection-boundary-design.md`

## Global Constraints

- The existing `InternalReadPolicy` meanings and the default-off `DBSP_STRICT_INTERNAL_QUERY` behavior stay unchanged.
- This subproject does not broaden `AllowedInTxn`, change which work may run inside a user transaction, or replace the planner's specific uncommitted-table diagnostic.
- Keep connections opened on the private `mv_db_` outside this context-backed API.
- Do not change public SQL APIs or the private MV database connection model.
- Use at most about half the available CPU cores for large builds; never raise repository resource guards.
- Check `df -h .` before building; abort below 6 GB free. Ask before any job expected to use sustained CPU for more than 5 minutes.
- Run targeted tests during implementation and one full CTest at the end. Run CTest from `test/build_test`, use at most four CTest workers on this 8-core machine, and build with at most four workers under the supplied 50%-CPU cap.

## Review Focus

- `Forbidden` retained connection opened during an explicit user transaction: strict mode throws the named policy error and restores `internal_query_depth`; test in `test_engine_assumptions`.
- `AllowedInTxn` persistence connection during an explicit transaction: definitions and MV metadata load successfully while the caller transaction remains active; test in `test_engine_assumptions`.
- Planner source table created only in the caller transaction: view creation keeps the existing “not yet committed” diagnostic and the caller can roll back; retain the regression in `test_trigger_source`.
- Retained planner connection outlives the query guard: registry membership lasts with the view, but `internal_query_depth` returns to its entry value as soon as translation scope ends; test in `test_engine_assumptions`.
- Transaction-manager-only start-timestamp probe and committed sidecar watermark inside transaction-sensitive create/restore paths: the probe does not scan user rows and sidecar adoption remains correct; retain the transaction-create and sidecar regressions in `test_trigger_source` and `test_view_arr_sidecars`.

---

## File Map

- Create `include/dbsp_internal_connection.hpp`: owns the recursion depth, guard, short-lived connection wrapper, and retained planner connection owner.
- Modify `include/dbsp_cdc.hpp`: include the focused boundary and replace remaining direct context-backed opens; remove the moved class definitions.
- Modify `include/dbsp_plan_ir.hpp`: store the retained owner in `PlanKeepAlive`, preserve connection-before-plan teardown order, and update the guard header reference.
- Modify `include/dbsp_plan_translator.hpp`: construct the planner connection through the retained owner and preserve bind-error handling.
- Modify `include/dbsp_trigger_capability.hpp`: update policy-contract comments to name the new connection header; leave policy behavior unchanged.
- Modify `test/integration/test_engine_assumptions.cpp`: pin retained connection policy, guard scope, and registry lifetime; add a persistence-load transaction canary.
- Reuse `test/unit/test_trigger_source.cpp`, `test/integration/test_persistence.cpp`, and `test/integration/test_view_arr_sidecars.cpp` for planner, persistence, probe, and sidecar regressions; add no duplicate cases where an existing case already asserts the contract.
- Modify `docs/TESTING.md`, `docs/ARCHITECTURE.md`, and `README.md`: update the boundary location and module map; keep the public feature list and usage examples unchanged.

## Execution Prerequisite

Complete this before Task 1 so its characterization tests can run. The current checkout has no `test/build_test` directory or test executables; the existing root `build` tree contains no test executables. The initial C++ test build is expected to exceed five minutes of sustained CPU, so the resource instructions require explicit user approval before these commands run.

```bash
df -h .
mkdir -p test/build_test
cmake -S test -B test/build_test -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER_LAUNCHER= -DCMAKE_CXX_COMPILER_LAUNCHER=
cmake --build test/build_test --target test_engine_assumptions test_trigger_source test_persistence test_view_arr_sidecars test_planner_frontend -j4
```

Expected: at least 6 GB free before configuration, then all five test targets build with four workers. If the initial build is not approved or the free-space threshold fails, pause at this prerequisite without modifying code.

The test CMake configuration must clear DuckDB's auto-detected compiler launchers: the host's ccache directory is outside the writable workspace and the sandbox rejects ccache cache writes. This keeps build outputs under `test/build_test` and invokes the configured compilers directly.

## Task 1: Extract the short-lived connection owner

**Files:**
- Create: `include/dbsp_internal_connection.hpp`
- Modify: `include/dbsp_cdc.hpp`
- Modify: `include/dbsp_plan_ir.hpp`
- Modify: `include/dbsp_trigger_capability.hpp`
- Test: `test/integration/test_engine_assumptions.cpp`

**Interfaces:**
- Consumes: `InternalReadPolicy` and `enforce_internal_read_policy(ClientContext&, InternalReadPolicy, const char*)` from `dbsp_trigger_capability.hpp`.
- Produces: `inline thread_local int internal_query_depth`, non-copyable `InternalQueryGuard`, and non-copyable `InternalConnection(ClientContext&, InternalReadPolicy, const char*)` with `operator*` and `operator->`.

- [x] **Step 1: Capture the existing lifecycle and policy behavior.**

Run the existing `[internal_connection]` canaries before moving definitions:

```bash
cd test/build_test
./test_engine_assumptions "[internal_connection]"
DBSP_STRICT_INTERNAL_QUERY=1 ./test_engine_assumptions "[internal_connection]"
```

Expected: both commands pass; the strict process exercises the `Forbidden` throw and confirms guard depth unwinds after construction failure. These are characterization tests for a behavior-preserving ownership move.

Before creating the new header, add a direct `dbsp_internal_connection.hpp` include and a guard-depth canary to `test_engine_assumptions.cpp`. Rebuild `test_engine_assumptions`; expected RED is a missing-header compile error. This pins that the new boundary is directly includable and its guard preserves depth.

- [x] **Step 2: Add the focused header with the existing scoped contract.**

Move `internal_query_depth`, `InternalQueryGuard`, and `InternalConnection` verbatim into `include/dbsp_internal_connection.hpp`. Include the direct DuckDB context, connection, database, and policy headers there. Preserve the member order `guard_` then `connection_`, and have both connection owners call the same `checked_internal_database` helper.

```cpp
inline thread_local int internal_query_depth = 0;

struct InternalQueryGuard {
  InternalQueryGuard() { ++internal_query_depth; }
  ~InternalQueryGuard() { --internal_query_depth; }
  InternalQueryGuard(const InternalQueryGuard &) = delete;
  InternalQueryGuard &operator=(const InternalQueryGuard &) = delete;
};

inline duckdb::DatabaseInstance &checked_internal_database(
    duckdb::ClientContext &context, InternalReadPolicy policy,
    const char *site) {
  enforce_internal_read_policy(context, policy, site);
  return duckdb::DatabaseInstance::GetDatabase(context);
}

class InternalConnection {
public:
  InternalConnection(duckdb::ClientContext &context,
                     InternalReadPolicy policy, const char *site)
      : connection_(checked_internal_database(context, policy, site)) {}

  duckdb::Connection &operator*() { return connection_; }
  duckdb::Connection *operator->() { return &connection_; }
  InternalConnection(const InternalConnection &) = delete;
  InternalConnection &operator=(const InternalConnection &) = delete;

private:
  InternalQueryGuard guard_;
  duckdb::Connection connection_;
};
```

- [x] **Step 3: Make the new header the only definition site.**

Include `dbsp_internal_connection.hpp` from `dbsp_cdc.hpp`, delete the old definitions there, and change the `dbsp_plan_ir.hpp` comment from “guard (`dbsp_cdc.hpp`)” to “guard (`dbsp_internal_connection.hpp`)”. Update the capability header comment to point at the new owner without moving `InternalReadPolicy` out of its current header.

- [x] **Step 4: Re-run the characterization canaries.**

```bash
cd test/build_test
./test_engine_assumptions "[internal_connection]"
DBSP_STRICT_INTERNAL_QUERY=1 ./test_engine_assumptions "[internal_connection]"
```

Expected: both pass with the same policy and teardown behavior as Step 1.

## Task 2: Give retained planner connections an explicit owner

**Files:**
- Modify: `include/dbsp_internal_connection.hpp`
- Modify: `include/dbsp_plan_ir.hpp`
- Modify: `include/dbsp_plan_translator.hpp`
- Test: `test/integration/test_engine_assumptions.cpp`

**Interfaces:**
- Consumes: `InternalConnection`, `InternalReadPolicy`, and `InstanceRegistry`.
- Produces: move-only `RetainedInternalConnection::open(ClientContext&, InternalReadPolicy, const char*)`, `operator*`, `operator->`, and `reset()`.
- `PlanKeepAlive::connection` becomes a `RetainedInternalConnection` value. Its explicit destructor calls `connection.reset()` before automatic destruction of `plan` and `rewritten_exprs`, preserving current teardown order.

- [x] **Step 1: Pin the current planner keep-alive behavior.**

Add a test that translates a simple query under a caller-held guard, keeps the returned view alive, and checks that registry membership outlives translation but ends when the view is destroyed:

```cpp
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
```

Run `./test_engine_assumptions "[plan_keepalive]"` before changing `PlanKeepAlive`; expected: PASS on the current implementation. This is a characterization test for the lifetime contract.

- [x] **Step 2: Add a retained-owner lifecycle canary.**

Add this case to `test_engine_assumptions.cpp` before implementing the new owner. Its initial compile failure should identify only the missing `RetainedInternalConnection` API; after this task it must pass.

```cpp
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
```

- [x] **Step 3: Implement the move-only retained owner.**

`open` must use the same `checked_internal_database(ClientContext&, InternalReadPolicy, const char*)` helper as `InternalConnection`, create the connection and register its context while a local `InternalQueryGuard` is active. The returned owner stores the connection and registered context pointer. `reset()` creates a temporary guard, destroys the connection first, then calls `unregister_internal`; custom move operations transfer the registration pointer with `std::exchange` so a moved-from object cannot unregister it early. Include `<utility>` for the move operations. This keeps teardown guarded without retaining a guard for the object's full lifetime.

```cpp
class RetainedInternalConnection {
public:
  RetainedInternalConnection() = default;
  static RetainedInternalConnection open(duckdb::ClientContext &context,
                                         InternalReadPolicy policy,
                                         const char *site);
  ~RetainedInternalConnection() { reset(); }
  RetainedInternalConnection(RetainedInternalConnection &&other) noexcept;
  RetainedInternalConnection &operator=(
      RetainedInternalConnection &&other) noexcept;
  RetainedInternalConnection(const RetainedInternalConnection &) = delete;
  RetainedInternalConnection &operator=(
      const RetainedInternalConnection &) = delete;
  duckdb::Connection &operator*() { return *connection_; }
  duckdb::Connection *operator->() { return connection_.get(); }
  void reset() noexcept;

private:
  RetainedInternalConnection(std::unique_ptr<duckdb::Connection> connection,
                             duckdb::ClientContext *registered_context) noexcept;
  std::unique_ptr<duckdb::Connection> connection_;
  duckdb::ClientContext *registered_context_ = nullptr;
};

inline RetainedInternalConnection RetainedInternalConnection::open(
    duckdb::ClientContext &context, InternalReadPolicy policy,
    const char *site) {
  InternalQueryGuard guard;
  auto &database = checked_internal_database(context, policy, site);
  auto connection = std::make_unique<duckdb::Connection>(database);
  auto *registered_context = connection->context.get();
  get_instance_registry().register_internal(registered_context, &database);
  return RetainedInternalConnection(std::move(connection), registered_context);
}

inline RetainedInternalConnection::RetainedInternalConnection(
    std::unique_ptr<duckdb::Connection> connection,
    duckdb::ClientContext *registered_context) noexcept
    : connection_(std::move(connection)), registered_context_(registered_context) {}

inline RetainedInternalConnection::RetainedInternalConnection(
    RetainedInternalConnection &&other) noexcept
    : connection_(std::move(other.connection_)),
      registered_context_(std::exchange(other.registered_context_, nullptr)) {}

inline RetainedInternalConnection &RetainedInternalConnection::operator=(
    RetainedInternalConnection &&other) noexcept {
  if (this != &other) {
    reset();
    connection_ = std::move(other.connection_);
    registered_context_ = std::exchange(other.registered_context_, nullptr);
  }
  return *this;
}

inline void RetainedInternalConnection::reset() noexcept {
  if (!connection_) {
    registered_context_ = nullptr;
    return;
  }
  InternalQueryGuard guard;
  auto *context = registered_context_;
  connection_.reset();
  if (context) {
    get_instance_registry().unregister_internal(context);
  }
  registered_context_ = nullptr;
}

```

- [x] **Step 4: Migrate `PlanKeepAlive` and planner construction.**

Replace `PlanKeepAlive::connection`'s `unique_ptr<duckdb::Connection>` with the retained owner. Keep `PlanKeepAlive::~PlanKeepAlive()` explicitly resetting it first. In `PlanTranslator::translate`, replace direct construction and `register_internal` with:

```cpp
keep_alive->connection = RetainedInternalConnection::open(
    context, InternalReadPolicy::AllowedInTxn,
    "PlanTranslator::translate (catalog binding only)");
```

Keep the caller's `InternalQueryGuard` around translation queries and preserve `explain_bind_error` unchanged.

- [x] **Step 5: Prove strict policy and planner diagnostics.**

Run:

```bash
cd test/build_test
./test_engine_assumptions "[retained]"
DBSP_STRICT_INTERNAL_QUERY=1 ./test_engine_assumptions "[retained]"
./test_engine_assumptions "[plan_keepalive]"
./test_trigger_source "[create_view]"
```

Expected: retained context registration remains until owner destruction, query-guard depth returns immediately after its scope, strict `Forbidden` construction fails with the named site and no depth leak, and the uncommitted-table diagnostic remains unchanged.

## Task 3: Route remaining context-backed opens through the owner

**Files:**
- Modify: `include/dbsp_cdc.hpp`
- Modify: `test/integration/test_engine_assumptions.cpp`
- Reuse: `test/unit/test_trigger_source.cpp`, `test/integration/test_view_arr_sidecars.cpp`

**Interfaces:**
- Consumes: `InternalConnection(ClientContext&, InternalReadPolicy, const char*)` and `RetainedInternalConnection::open(...)`.
- Produces: no new public interfaces. All policy reasons stay at their call sites.

- [x] **Step 1: Add the transaction-safe persistence-load canary.**

Add an integration case that creates empty persistence tables, begins a user transaction, invokes `load_from_duck_table`, and verifies the caller transaction remains active:

```cpp
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
```

Run the new case in both normal and strict processes before migrating the opens; it must pass in both as a characterization baseline.

- [x] **Step 2: Replace persistence and metadata opens.**

In `CDCManager::load_from_duck_table`, replace the scoped raw connection for `_dbsp_views` with `InternalConnection(context, AllowedInTxn, "load_from_duck_table")`. Replace the `__dbsp_mv_meta` raw connection with a second `InternalConnection` and a site string naming the metadata read. Keep query scopes and early returns unchanged.

- [x] **Step 3: Replace the arrangement-sidecar open without changing its exemption.**

In `register_arrangements`, replace the raw watermark query connection with an `InternalConnection` using `AllowedInTxn` and site `register_arrangements (sidecar watermark)`. Remove the redundant entry check only after the owner is at the query site; preserve the exact committed count/hash query and sidecar adoption checks.

- [x] **Step 4: Route the transaction-manager probe through the scoped owner.**

In `probe_new_start_timestamp`, use `InternalConnection` with `AllowedInTxn` and the explicit site string `probe_new_start_timestamp (transaction-manager counter only)`. Keep the `BeginTransaction`, timestamp read, rollback, catch-to-zero behavior, and existing comment that this path reads no table or catalog data.

- [x] **Step 5: Verify persistence, probe, and sidecar behavior.**

```bash
cd test/build_test
./test_engine_assumptions "[persistence]"
./test_trigger_source "[create_view]"
./test_persistence
./test_view_arr_sidecars
DBSP_STRICT_INTERNAL_QUERY=1 ./test_engine_assumptions "[internal_connection]"
```

Expected: all commands pass; user transaction state, view-load behavior, transaction-created views, and committed sidecar adoption remain unchanged.

## Task 4: Refresh the documented ownership map and audit construction sites

**Files:**
- Modify: `docs/TESTING.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `README.md`
- Review: `include/dbsp_cdc.hpp`, `include/dbsp_plan_translator.hpp`, `include/dbsp_trigger_source.hpp`, and `src/dbsp_extension.cpp`

- [x] **Step 1: Update the boundary descriptions.**

Name `dbsp_internal_connection.hpp` as the home for scoped and retained owners. Document that short-lived owners hold the recursion guard through teardown, retained planner owners keep registry membership but do not keep a thread-local guard alive, and the transaction-manager-only rationale is adjacent to the probe.

Recount the policy table in `docs/TESTING.md`; add the persistence definition and metadata reads, planner catalog-binding permission, and transaction-manager-only probe with their reasons. Update the `AllowedInTxn` rationale summary so it no longer claims those sites fit only the previous two categories.

- [x] **Step 2: Update module maps.**

Add `dbsp_internal_connection.hpp` to the architecture/module listings in `docs/ARCHITECTURE.md` and `README.md`; remove claims that `InternalConnection` lives in `dbsp_cdc.hpp`. Do not change public feature lists, SQL examples, or build instructions.

- [x] **Step 3: Audit every direct connection construction.**

```bash
rg -n 'InternalConnection|RetainedInternalConnection|duckdb::Connection|make_unique<duckdb::Connection>' include src test
```

Inspect every `duckdb::Connection` construction. Expected: context-backed construction is confined to `dbsp_internal_connection.hpp`; the remaining direct constructors are private `mv_db_` connections under their existing guards, or typed non-context exceptions documented at the site. Confirm no duplicate definitions remain in `dbsp_cdc.hpp`.

## Task 5: Run focused and final verification

**Files:** no additional source files; build artifacts belong under `test/build_test`.

- [x] **Step 1: Recheck disk before building remaining targets.**

Run `df -h .` and verify at least 6 GB are free before expanding from the focused targets to all registered targets.

```bash
df -h .
```

Expected: at least 6 GB free. If the threshold fails, stop without starting the full build.

- [x] **Step 2: Run the focused regression set.**

```bash
cd test/build_test
./test_engine_assumptions "[internal_connection]"
./test_trigger_source "[create_view]"
./test_persistence
./test_view_arr_sidecars
./test_planner_frontend
```

Expected: all selected tests pass with no failures.

- [x] **Step 3: Build the remaining CTest targets incrementally.**

```bash
cmake --build test/build_test -j4
```

Expected: all registered CTest executables finish building. Recheck free disk before this build; stop if available space falls below 6 GB.

- [x] **Step 4: Run the single full CTest pass at the end.**

```bash
cd test/build_test
DBSP_STRICT_INTERNAL_QUERY=1 ctest -j4
```

Expected: every registered CTest passes under strict internal-query policy. This is the task's only full-suite run; report the exact test and assertion totals from CTest.

## Plan Self-Review

- Spec coverage: scoped owner extraction (Task 1); retained planner lifetime and registry cleanup (Task 2); load, metadata, sidecar, and probe policy sites (Task 3); documentation and source audit (Task 4); focused and single full verification (Task 5).
- Placeholder scan: every task has concrete files, commands, and expected outcomes.
- Type consistency: `RetainedInternalConnection::open(ClientContext&, InternalReadPolicy, const char*)` returns a move-only owner that exposes DuckDB's connection with `operator*` and `operator->`; `PlanKeepAlive` stores that value and explicitly resets it before its plan and rewritten expressions are destroyed.
- Review Focus coverage: policy/unwind (Task 2), transaction-time persistence load (Task 3), planner diagnostic (Task 2), retained registry lifetime (Task 2), and transaction probe/sidecar correctness (Tasks 3 and 5).
