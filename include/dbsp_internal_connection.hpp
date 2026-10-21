#pragma once

#include "dbsp_trigger_capability.hpp"
#include "dbsp_instance_registry.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <memory>
#include <utility>

namespace dbsp_native {

// Marks queries DBSP issues on internal helper connections. Transaction hooks
// must not recurse into CDCManager for these (deadlock: the issuing thread may
// already hold struct_mutex_, which is not recursive).
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

// Own the recursion guard before opening the connection; reverse member
// destruction keeps hooks suppressed through Connection/ClientContext teardown.
// Policy remains explicit: callers sharing a scan helper can have different
// transaction contracts, and the commit hook has already lost that context.
class InternalConnection {
public:
  InternalConnection(duckdb::ClientContext &context, InternalReadPolicy policy,
                     const char *site)
      : connection_(checked_internal_database(context, policy, site)) {}

  duckdb::Connection &operator*() { return connection_; }
  duckdb::Connection *operator->() { return &connection_; }
  InternalConnection(const InternalConnection &) = delete;
  InternalConnection &operator=(const InternalConnection &) = delete;

private:
  InternalQueryGuard guard_;
  duckdb::Connection connection_;
};

// A planner connection outlives the query guard held by its caller. Keep its
// context registered for the full connection lifetime, and guard only the
// creation and teardown operations on the thread performing them.
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
    : connection_(std::move(connection)), registered_context_(registered_context) {
}

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

} // namespace dbsp_native
