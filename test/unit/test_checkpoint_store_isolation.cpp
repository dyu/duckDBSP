// Build-time guard for the CheckpointStore boundary (design:
// docs/superpowers/specs/2026-09-26-cdc-checkpoint-persistence-owner-
// design.md, section 1): the store depends on DuckDB and standard-library
// values only, never on the manager's state types. This file includes the
// store header alone and then declares its own TrackedTable,
// NativeMaterializedView and CDCManager; if the header ever pulls in the
// real ones again, this stops compiling.

#include "dbsp_checkpoint_store.hpp"

#include "catch.hpp"

namespace dbsp_native {
struct TrackedTable {};
struct NativeMaterializedView {};
class CDCManager {};
} // namespace dbsp_native

TEST_CASE("checkpoint store header stands alone",
          "[unit][checkpoint][checkpoint_store]") {
  dbsp_native::SourceWatermark a{1, "h"};
  REQUIRE(a == dbsp_native::SourceWatermark{1, "h"});
}
