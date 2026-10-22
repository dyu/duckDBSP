#pragma once

// Circuit-state checkpointing (Phase D3b).
//
// A checkpoint captures everything a view needs to resume incremental
// maintenance without replaying its sources: per-node operator state
// (aggregate groups, sink materialization), shared join arrangements, and
// tracked-table baselines. Blobs are opaque byte strings that
// CDCManager::save_checkpoint encodes and CheckpointStore
// (dbsp_checkpoint_store.hpp) writes to the _dbsp_ckpt table; the store
// reads them back for load_from_duck_table's fast path.
//
// Any node kind without checkpoint support marks its whole view
// non-checkpointable; such views fall back to the normal rebuild-by-replay
// on load. Correctness never depends on a checkpoint being present or
// fresh: a stale checkpoint (source tables changed since save) is detected
// by row-count watermarks and discarded; a checkpoint whose view
// *definition* has changed since save (e.g. via dbsp_replace_view) is
// detected per-view by a SQL fingerprint (v3, see kDbspCkptFormatVersion)
// and discarded for that view only.

#include "dbsp_duckdb_types.hpp" // DuckDBRow / ColumnVec::kNullHash
#include "dbsp_spill_store.hpp" // serialize_row / deserialize_row
#include "dbsp_checkpoint_format.hpp" // kDbspCkptFormatVersion

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace dbsp_native {

// How a circuit node participates in checkpointing.
enum class CkptSupport {
  STATELESS,    // nothing to save; restore is a no-op
  SERIALIZABLE, // implements serialize_state / restore_state
  UNSUPPORTED,  // view must be rebuilt by replay
};

// Little-endian fixed-width writer/reader for node state blobs. Values go
// through serialize_row (DuckDB BinarySerializer), scalars through these.
class BlobWriter {
public:
  void u64(uint64_t v) { raw(&v, sizeof(v)); }
  void i64(int64_t v) { raw(&v, sizeof(v)); }
  void f64(double v) { raw(&v, sizeof(v)); }

  // Accepts std::vector<Value> or DuckDBRow's ColumnVec (index/size API).
  // Serializes straight from the container (no vector<Value> copy) into a
  // member scratch buffer: a checkpoint serializes every row of every
  // view, and the per-row copy + two heap allocations dominated
  // ckpt_serialize.
  template <typename Container> void row(const Container &values) {
    serialize_row(values, row_tmp_);
    u64(row_tmp_.size());
    raw(row_tmp_.data(), row_tmp_.size());
  }

  // Length-prefixed opaque bytes (packed join-index blobs, Phase 2d).
  void bytes(const uint8_t *p, size_t n) {
    u64(n);
    raw(p, n);
  }

  std::vector<uint8_t> take() { return std::move(bytes_); }

  // Callers with a known result size can pre-reserve to avoid the
  // insert-driven doubling reallocs (each one copies the whole blob).
  void reserve(size_t n) { bytes_.reserve(n); }

private:
  void raw(const void *p, size_t n) {
    const auto *b = static_cast<const uint8_t *>(p);
    bytes_.insert(bytes_.end(), b, b + n);
  }
  std::vector<uint8_t> bytes_;
  std::vector<uint8_t> row_tmp_;
};

// Row hash matching ColumnVec::hash()/fold_vector_hashes bit-for-bit, computed
// from already-decoded Values without paying Value::Hash()'s cost (a fresh
// 1-element Vector + VectorOperations::Hash dispatch per column — the "lazy
// per-Value hashing" measured at ~2/3 of chunk-ingestion time, which is why
// the delta-ingest paths pre-seed their row hashes too). Every
// checkpoint-restored row becomes a hash-map key
// (aggregate states_, join Index/RowWeights) or a DuckDBZSet entry, so a
// restore built entirely from `row()` pays that lazy cost once per row on
// first use — and a checkpointed view with many groups/sink rows spends most
// of "blob_decode" there, not in the byte-level codec (already shared with
// the spill path, see serialize_row/deserialize_row above).
//
// For the codec's typed fast-path columns (INTEGER/BIGINT/DOUBLE/VARCHAR/
// BOOLEAN), duckdb::Hash<T> is the exact primitive VectorOperations::Hash
// calls per element for a flat vector of that physical type (verified
// against duckdb/src/common/vector_operations/vector_hash.cpp — HashOp
// dispatches straight to duckdb::Hash<T>, and duckdb::Hash(string_t) is
// documented+asserted equal to duckdb::Hash(ptr, size) for every string,
// inlined or not), so calling it directly here reproduces Value::Hash()
// exactly without the temporary-Vector detour. Fallback-typed and NULL
// values keep behaving exactly as the lazy path already does (NULL ->
// ColumnVec::kNullHash, ColumnVec::hash()'s own convention rather than
// duckdb's; fallback -> v.Hash(), the one case that still pays full price —
// same tradeoff serialize_row's byte-level fallback already makes). The
// combiner is copy-identical to ColumnVec::hash()/fold_vector_hashes so a
// row restored this way hashes the same as an equal-content row a live
// chunk-based path would build; test_join_checkpoint.cpp's "hashed_row
// matches the lazy path" case is the correctness guard for that invariant.
inline size_t hash_row_fast(const std::vector<duckdb::Value> &vals) {
  size_t h = 0;
  for (const auto &v : vals) {
    size_t col_hash;
    if (v.IsNull()) {
      col_hash = ColumnVec::kNullHash;
    } else {
      switch (v.type().id()) {
      case duckdb::LogicalTypeId::INTEGER:
        col_hash = duckdb::Hash<int32_t>(duckdb::IntegerValue::Get(v));
        break;
      case duckdb::LogicalTypeId::BIGINT:
        col_hash = duckdb::Hash<int64_t>(duckdb::BigIntValue::Get(v));
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        col_hash = duckdb::Hash<double>(duckdb::DoubleValue::Get(v));
        break;
      case duckdb::LogicalTypeId::VARCHAR: {
        const auto &s = duckdb::StringValue::Get(v);
        col_hash = duckdb::Hash(s.data(), s.size());
        break;
      }
      case duckdb::LogicalTypeId::BOOLEAN:
        col_hash = duckdb::Hash<bool>(duckdb::BooleanValue::Get(v));
        break;
      default:
        col_hash = v.Hash(); // fallback types: pay the slow-but-correct path
        break;
      }
    }
    h ^= col_hash + 0x9e3779b9 + (h << 6) + (h >> 2);
  }
  return h;
}

class BlobReader {
public:
  BlobReader(const uint8_t *data, size_t len) : data_(data), len_(len) {}

  uint64_t u64() { return fixed<uint64_t>(); }
  int64_t i64() { return fixed<int64_t>(); }
  double f64() { return fixed<double>(); }

  std::vector<duckdb::Value> row() {
    const uint64_t n = u64();
    if (pos_ + n > len_) {
      throw std::runtime_error("checkpoint blob truncated (row)");
    }
    auto vals = deserialize_row(data_ + pos_, n);
    pos_ += n;
    return vals;
  }

  // Same bytes as row(), but returns a DuckDBRow with its hash cache
  // pre-seeded (hash_row_fast) instead of left for the first map/Z-set
  // operation to compute lazily. Use this at every checkpoint restore call
  // site that turns a decoded row into a hash-map key or Z-set entry.
  DuckDBRow hashed_row() {
    auto vals = row();
    DuckDBRow out;
    const size_t h = hash_row_fast(vals);
    out.columns.assign(std::move(vals));
    out.columns.set_hash(h);
    return out;
  }

  // Length-prefixed opaque bytes (see BlobWriter::bytes).
  std::string byte_string() {
    const uint64_t n = u64();
    if (pos_ + n > len_) {
      throw std::runtime_error("blob truncated");
    }
    std::string out(reinterpret_cast<const char *>(data_ + pos_), n);
    pos_ += n;
    return out;
  }

  bool done() const { return pos_ == len_; }

private:
  template <typename T> T fixed() {
    if (pos_ + sizeof(T) > len_) {
      throw std::runtime_error("checkpoint blob truncated (scalar)");
    }
    T v;
    std::memcpy(&v, data_ + pos_, sizeof(T));
    pos_ += sizeof(T);
    return v;
  }

  const uint8_t *data_;
  size_t len_;
  size_t pos_ = 0;
};

} // namespace dbsp_native
