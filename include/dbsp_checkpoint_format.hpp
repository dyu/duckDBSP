#pragma once

// The checkpoint blob format version, on its own so the checkpoint table
// I/O (dbsp_checkpoint_store.hpp) can name it without pulling in the blob
// codecs and, through them, every view and tracked-table type.

#include <cstdint>

namespace dbsp_native {

// Checkpoint blob format version. Bump whenever a node's serialize_state
// layout changes shape (e.g. new fields appended) so a blob written under
// an older layout is detected and discarded rather than fed to the new
// restore_state, which would misparse it. CheckpointStore writes this
// into a dedicated _dbsp_ckpt_version table, and its read reports no
// checkpoint at all (-> rebuild-by-replay) when that table is missing
// (pre-bump blob) or its value differs.
//
// v2 (Task 3, durability-ergonomics): PlanJoinNode::serialize_state gained
// left_pad_/right_pad_ for LEFT/RIGHT outer-join pad bookkeeping.
//
// v3 (durability-ergonomics fix-wave, Finding 1): the checkpoint now also
// carries a per-view SQL fingerprint (a `kind='sql'` row in _dbsp_ckpt,
// alongside each view's `node`/`sink` rows) recording the view's exact
// definition at save time. Without it, a checkpoint saved under one
// definition could be silently replayed onto a *different* one — e.g.
// dbsp_replace_view()/CREATE OR REPLACE swaps a view's SQL, then an
// unclean close (crash, autopersist off, a failed auto-save) skips the
// next save_checkpoint(), leaving stale node/sink blobs on disk next to a
// _dbsp_views row that already carries the new SQL. On load,
// checkpoint_valid compares the stored fingerprint against the SQL about
// to be used to cold-create the view; a mismatch declines the checkpoint
// fast path for *that view only* (rebuild-by-replay), leaving every other
// view's restore unaffected. The version bump means any v2 checkpoint
// (which has no fingerprint rows at all) is treated as wholesale absent,
// same one-time-rebuild cost as prior bumps.
// v4 (cold/restore attack, Task 2): PlanAggregateNode::serialize_state
// gained the per-group `values` multiset for plain (non-DISTINCT) MIN/MAX,
// so those views now report SERIALIZABLE instead of UNSUPPORTED. A v3
// checkpoint has no such field for any group and would misparse under the
// new restore_state layout, so it is treated as absent (one-time
// rebuild-by-replay, same cost as prior bumps).
// v5 (Task 1, restore-tail): PlanRecursiveNode::serialize_state gained
// accumulated_/anchor_total_/base_totals_ plus the nested step_view_
// circuit's own per-node blobs (embedded as a length-prefixed sub-blob
// keyed by inner node id), so a UNION ALL recursive view whose step circuit
// is itself wholly checkpointable now reports SERIALIZABLE instead of
// UNSUPPORTED. A v4 checkpoint carries no node blob at all for a recursive
// node (the node was UNSUPPORTED then, so save_checkpoint never wrote one)
// and would misparse under the new restore_state layout, so it is treated
// as absent (one-time rebuild-by-replay, same cost as prior bumps).
//
// Task 2 (restore-tail) did NOT bump this: EmbeddedViewNode gained
// circuit_state_kind()/serialize_circuit_node_state()/
// restore_circuit_node_state() (dbsp_duckdb_types.hpp), letting a
// NativeWindowView embedded view report SERIALIZABLE instead of
// UNSUPPORTED — but EmbeddedViewNode never wrote a node blob under any
// prior version (it was unconditionally UNSUPPORTED), so an existing v5
// checkpoint simply has no blob keyed by that node's id, not a
// differently-shaped one. restore_circuit_state's per-node loop already
// treats a missing key as a decline for that node (blobs.find(n.id()) ==
// end()), gracefully falling back to rebuild-by-replay for that view
// alone — no whole-checkpoint invalidation needed for a node kind with no
// prior blob to misparse.
constexpr int64_t kDbspCkptFormatVersion = 5;

} // namespace dbsp_native
