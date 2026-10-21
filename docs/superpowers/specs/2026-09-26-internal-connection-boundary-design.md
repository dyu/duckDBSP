# Internal connection boundary

**Status:** Approved for implementation on 2026-09-26. This is subproject 1 of the repository-wide maintainability refactor; implementation follows the approved inline plan on `main`.

## Problem

`InternalConnection` currently lives in `dbsp_cdc.hpp`, so its ownership and policy contract is coupled to the largest state manager. It correctly combines `InternalQueryGuard` with short-lived `duckdb::Connection` ownership and checks `InternalReadPolicy` before opening. Several context-backed opens still bypass that boundary: persistence loading and metadata reads in `CDCManager`, the early arrangement-sidecar watermark query, and the long-lived planner connection in `PlanTranslator`.

The planner connection has a different lifetime from ordinary internal reads. Its `duckdb::Connection` is retained by `PlanKeepAlive` so bound expressions remain valid. Keeping a thread-local `InternalQueryGuard` alive with it would be wrong: the guard must cover the planner queries and teardown on the operating thread, not the lifetime of a view that may be destroyed on another thread.

## Goal

Make connection construction from a user `ClientContext` go through one small, explicit boundary that applies the caller's transaction policy and preserves hook suppression. Model short-lived scoped connections and the retained planner connection as distinct lifetimes. Preserve transaction behavior, planner diagnostics, and internal-context teardown tracking.

## Design

1. Move `InternalQueryGuard` and `InternalConnection` from `dbsp_cdc.hpp` into a focused `dbsp_internal_connection.hpp`. Keep the boundary independent of `CDCManager` state.
2. Keep `InternalConnection` as the owner for short-lived work. It continues to check policy before construction and to declare guard ownership before connection ownership, so the connection and its client context are destroyed before the guard is released.
3. Add a clearly named retained-connection owner on the same boundary for planner use. It applies an explicit policy, creates the connection while hook suppression is active, and registers its client context with `InstanceRegistry`. Its teardown closes the connection under a temporary guard before unregistering it. The caller remains responsible for holding an `InternalQueryGuard` across planner queries. The owner must not keep a thread-local guard alive for the retained connection's lifetime.
4. Route the remaining user-context opens through one of those two APIs. Keep these existing policy contracts:

   | Site | Policy / reason |
   | --- | --- |
   | View-definition load and `__dbsp_mv_meta` load | `AllowedInTxn`: reads DBSP persistence definitions and metadata, not the caller's uncommitted source rows. |
   | Early arrangement-sidecar watermark | `AllowedInTxn`, matching the existing exemption: stamps a committed-storage fingerprint for sidecar adoption. |
   | Planner connection | Explicitly permitted while a transaction is open because it binds/plans but does not scan source rows; preserve the existing binder error that explains when an uncommitted table is invisible. |
   | `probe_new_start_timestamp` | Keep its documented transaction-manager-only scope; it reads no table data or catalog entries. Its construction must still state why ordinary committed-state policy does not apply. |
5. Keep connections opened on the private `mv_db_` outside this context-backed API. They do not derive from the caller's `ClientContext`; their existing `InternalQueryGuard` scopes remain responsible for hook suppression.

The existing `InternalReadPolicy` meanings and the default-off `DBSP_STRICT_INTERNAL_QUERY` behavior stay unchanged. This subproject does not broaden `AllowedInTxn`, change which work may run inside a user transaction, or replace the planner's specific uncommitted-table diagnostic.

## Scope and non-goals

**In scope:** connection and guard ownership, policy enforcement at context-backed construction sites, planner retained-connection registration, include dependencies, and focused regression coverage.

**Out of scope:** CDC state decomposition; materialized-view propagation or persistence redesign; changing transaction semantics; changing the private MV database connection model; changing public SQL APIs; refactoring other oversized files.

## Implementation sequence

1. Add the focused connection header and move the existing short-lived owner without changing its behavior.
2. Add the retained-connection creation path and migrate the planner, preserving `PlanKeepAlive` and `InstanceRegistry` lifetime behavior.
3. Migrate persistence-load, MV metadata, and arrangement-sidecar context opens with their explicit policies. Keep the transaction-manager probe as a documented narrow exception.
4. Add or adjust regression tests, then update `docs/TESTING.md` and relevant architecture documentation if the moved boundary changes the documented component map.

## Acceptance and verification

- A source search finds no unclassified construction of a `duckdb::Connection` from `DatabaseInstance::GetDatabase(context)`; every remaining site is routed through the boundary or is the documented transaction-manager-only probe.
- Existing lifecycle tests still prove hook suppression through connection teardown and restoration of thread-local depth.
- Strict-policy coverage proves `Forbidden` rejects an explicit user transaction and whitelisted sites retain their current behavior.
- Planner tests preserve retained-plan lifetime, internal-context registration, and the readable error for a table created only in the caller's open transaction.
- Persistence and arrangement-sidecar regressions remain green.
- Run focused integration targets during implementation and one full `ctest` at the end, with the repository's strict internal-query pass as documented.

## Risks

- Accidentally extending a thread-local guard to the lifetime of `PlanKeepAlive` can suppress transaction hooks on an unrelated future thread; the retained factory must not own that guard.
- Misclassifying the arrangement-sidecar query or planner policy could make strict mode fail on valid work or hide a transaction-visibility defect. Keep reasons adjacent to the call sites and pin the behavior in tests.
- Dropping planner context registration could make extension teardown treat its own connection as a user connection and retain database-instance state. Preserve the registry assertion in the regression coverage.
