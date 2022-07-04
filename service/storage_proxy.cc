/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include "partition_range_compat.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "storage_proxy.hh"
#include "unimplemented.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"
#include "supervisor.hh"
#include "query_result_merger.hh"
#include <seastar/core/do_with.hh>
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include <seastar/core/future-util.hh>
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "db/hints/manager.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/none_of.hpp>
#include <boost/algorithm/cxx11/partition_copy.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/empty.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/combine.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm/partition.hpp>
#include <boost/intrusive/list.hpp>
#include "utils/latency.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "utils/joinpoint.hh"
#include <seastar/util/lazy.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/execution_stage.hh>
#include "db/timeout_clock.hh"
#include "multishard_mutation_query.hh"
#include "database.hh"
#include "db/consistency_level_validations.hh"
#include "cdc/log.hh"
#include "cdc/stats.hh"
#include "cdc/cdc_options.hh"
#include "utils/histogram_metrics_helper.hh"
#include "service/paxos/prepare_summary.hh"
#include "service/migration_manager.hh"
#include "service/paxos/proposal.hh"
#include "locator/token_metadata.hh"
#include "seastar/core/coroutine.hh"
#include "locator/abstract_replication_strategy.hh"
#include "service/paxos/cas_request.hh"
#include "mutation_partition_view.hh"
#include "service/paxos/paxos_state.hh"
#include "gms/feature_service.hh"
#include "db/virtual_table.hh"
#include "canonical_mutation.hh"
#include "schema_mutations.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"

namespace bi = boost::intrusive;

namespace service {

static logging::logger slogger("storage_proxy");
static logging::logger qlogger("query_result");
static logging::logger mlogger("mutation_data");

namespace storage_proxy_stats {
static const sstring COORDINATOR_STATS_CATEGORY("storage_proxy_coordinator");
static const sstring REPLICA_STATS_CATEGORY("storage_proxy_replica");
static const seastar::metrics::label op_type_label("op_type");
static const seastar::metrics::label scheduling_group_label("scheduling_group_name");

seastar::metrics::label_instance current_scheduling_group_label() {
    return scheduling_group_label(current_scheduling_group().name());
}

}

thread_local uint64_t paxos_response_handler::next_id = 0;

distributed<service::storage_proxy> _the_storage_proxy;

using namespace exceptions;
using fbu = utils::fb_utilities;

static inline
query::digest_algorithm digest_algorithm(service::storage_proxy& proxy) {
    return proxy.features().cluster_supports_digest_for_null_values()
            ? query::digest_algorithm::xxHash
            : query::digest_algorithm::legacy_xxHash_without_null_digest;
}

static inline
const dht::token& start_token(const dht::partition_range& r) {
    static const dht::token min_token = dht::minimum_token();
    return r.start() ? r.start()->value().token() : min_token;
}

static inline
const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static inline
sstring get_dc(gms::inet_address ep) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(ep);
}

static inline
sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    return get_dc(local_addr);
}

unsigned storage_proxy::cas_shard(const schema& s, dht::token token) {
    return dht::shard_of(s, token);
}

class mutation_holder {
protected:
    size_t _size = 0;
    schema_ptr _schema;
public:
    virtual ~mutation_holder() {}
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) = 0;
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) = 0;
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) = 0;
    virtual bool is_shared() = 0;
    size_t size() const {
        return _size;
    }
    const schema_ptr& schema() {
        return _schema;
    }
    // called only when all replicas replied
    virtual void release_mutation() = 0;
    // called when reply is received
    // alllows mutation holder to have its own accounting
    virtual void reply(gms::inet_address ep) {};
};

// different mutation for each destination (for read repairs)
class per_destination_mutation : public mutation_holder {
    std::unordered_map<gms::inet_address, lw_shared_ptr<const frozen_mutation>> _mutations;
    dht::token _token;
public:
    per_destination_mutation(const std::unordered_map<gms::inet_address, std::optional<mutation>>& mutations) {
        for (auto&& m : mutations) {
            lw_shared_ptr<const frozen_mutation> fm;
            if (m.second) {
                _schema = m.second.value().schema();
                _token = m.second.value().token();
                fm = make_lw_shared<const frozen_mutation>(freeze(m.second.value()));
                _size += fm->representation().size();
            }
            _mutations.emplace(m.first, std::move(fm));
        }
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) override {
        auto m = _mutations[ep];
        if (m) {
            return hm.store_hint(ep, _schema, std::move(m), tr_state);
        } else {
            return false;
        }
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        auto m = _mutations[utils::fb_utilities::get_broadcast_address()];
        if (m) {
            tracing::trace(tr_state, "Executing a mutation locally");
            return sp.mutate_locally(_schema, *m, std::move(tr_state), db::commitlog::force_sync::no, timeout);
        }
        return make_ready_future<>();
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        auto m = _mutations[ep];
        if (m) {
            tracing::trace(tr_state, "Sending a mutation to /{}", ep);
            return sp._messaging.send_mutation(netw::messaging_service::msg_addr{ep, 0}, timeout, *m,
                                    std::move(forward), utils::fb_utilities::get_broadcast_address(), this_shard_id(),
                                    response_id, tracing::make_trace_info(tr_state));
        }
        sp.got_response(response_id, ep, std::nullopt);
        return make_ready_future<>();
    }
    virtual bool is_shared() override {
        return false;
    }
    virtual void release_mutation() override {
        for (auto&& m : _mutations) {
            if (m.second) {
                m.second.release();
            }
        }
    }
    dht::token& token() {
        return _token;
    }
};

// same mutation for each destination
class shared_mutation : public mutation_holder {
protected:
    lw_shared_ptr<const frozen_mutation> _mutation;
public:
    explicit shared_mutation(frozen_mutation_and_schema&& fm_a_s)
            : _mutation(make_lw_shared<const frozen_mutation>(std::move(fm_a_s.fm))) {
        _size = _mutation->representation().size();
        _schema = std::move(fm_a_s.s);
    }
    explicit shared_mutation(const mutation& m) : shared_mutation(frozen_mutation_and_schema{freeze(m), m.schema()}) {
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) override {
            return hm.store_hint(ep, _schema, _mutation, tr_state);
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        tracing::trace(tr_state, "Executing a mutation locally");
        return sp.mutate_locally(_schema, *_mutation, std::move(tr_state), db::commitlog::force_sync::no, timeout);
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        tracing::trace(tr_state, "Sending a mutation to /{}", ep);
        return sp._messaging.send_mutation(netw::messaging_service::msg_addr{ep, 0}, timeout, *_mutation,
                std::move(forward), utils::fb_utilities::get_broadcast_address(), this_shard_id(),
                response_id, tracing::make_trace_info(tr_state));
    }
    virtual bool is_shared() override {
        return true;
    }
    virtual void release_mutation() override {
        _mutation.release();
    }
};

// shared mutation, but gets sent as a hint
class hint_mutation : public shared_mutation {
public:
    using shared_mutation::shared_mutation;
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) override {
        throw std::runtime_error("Attempted to store a hint for a hint");
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        // A hint will be sent to all relevant endpoints when the endpoint it was originally intended for
        // becomes unavailable - this might include the current node
        return sp.mutate_hint(_schema, *_mutation, std::move(tr_state), timeout);
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        tracing::trace(tr_state, "Sending a hint to /{}", ep);
        return sp._messaging.send_hint_mutation(netw::messaging_service::msg_addr{ep, 0}, timeout, *_mutation,
                std::move(forward), utils::fb_utilities::get_broadcast_address(), this_shard_id(),
                response_id, tracing::make_trace_info(tr_state));
    }
};

class cas_mutation : public mutation_holder {
    lw_shared_ptr<paxos::proposal> _proposal;
    shared_ptr<paxos_response_handler> _handler;
public:
    explicit cas_mutation(lw_shared_ptr<paxos::proposal> proposal, schema_ptr s, shared_ptr<paxos_response_handler> handler)
            : _proposal(std::move(proposal)), _handler(std::move(handler)) {
        _size = _proposal->update.representation().size();
        _schema = std::move(s);
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) override {
            return false; // CAS does not save hints yet
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        tracing::trace(tr_state, "Executing a learn locally");
        return paxos::paxos_state::learn(_schema, *_proposal, timeout, tr_state);
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) override {
        tracing::trace(tr_state, "Sending a learn to /{}", ep);
        return sp._messaging.send_paxos_learn(netw::messaging_service::msg_addr{ep, 0}, timeout,
                                *_proposal, std::move(forward), utils::fb_utilities::get_broadcast_address(),
                                this_shard_id(), response_id, tracing::make_trace_info(tr_state));
    }
    virtual bool is_shared() override {
        return true;
    }
    virtual void release_mutation() override {
        _proposal.release();
    }
    virtual void reply(gms::inet_address ep) override {
        // The handler will be set for "learn", but not for PAXOS repair
        // since repair may not include all replicas
        if (_handler) {
            if (_handler->learned(ep)) {
                // It's OK to start PRUNE while LEARN is still in progress: LEARN
                // doesn't read any data from system.paxos, and PRUNE tombstone
                // will cover LEARNed value even if it arrives out of order.
                _handler->prune(_proposal->ballot);
            }
        }
    };
};

class abstract_write_response_handler : public seastar::enable_shared_from_this<abstract_write_response_handler> {
protected:
    using error = storage_proxy::error;
    storage_proxy::response_id_type _id;
    promise<> _ready; // available when cl is achieved
    shared_ptr<storage_proxy> _proxy;
    tracing::trace_state_ptr _trace_state;
    db::consistency_level _cl;
    size_t _total_block_for = 0;
    db::write_type _type;
    std::unique_ptr<mutation_holder> _mutation_holder;
    inet_address_vector_replica_set _targets; // who we sent this mutation to
    // added dead_endpoints as a memeber here as well. This to be able to carry the info across
    // calls in helper methods in a convinient way. Since we hope this will be empty most of the time
    // it should not be a huge burden. (flw)
    inet_address_vector_topology_change _dead_endpoints;
    size_t _cl_acks = 0;
    bool _cl_achieved = false;
    bool _throttled = false;
    error _error = error::NONE;
    std::optional<sstring> _message;
    size_t _failed = 0; // only failures that may impact consistency
    size_t _all_failures = 0; // total amount of failures
    size_t _total_endpoints = 0;
    storage_proxy::write_stats& _stats;
    lw_shared_ptr<cdc::operation_result_tracker> _cdc_operation_result_tracker;
    timer<storage_proxy::clock_type> _expire_timer;
    service_permit _permit; // holds admission permit until operation completes

protected:
    virtual bool waited_for(gms::inet_address from) = 0;
    void signal(gms::inet_address from) {
        _mutation_holder->reply(from);
        if (waited_for(from)) {
            signal();
        }
    }

public:
    abstract_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state,
            storage_proxy::write_stats& stats, service_permit permit, size_t pending_endpoints = 0, inet_address_vector_topology_change dead_endpoints = {})
            : _id(p->get_next_response_id()), _proxy(std::move(p)), _trace_state(trace_state), _cl(cl), _type(type), _mutation_holder(std::move(mh)), _targets(std::move(targets)),
              _dead_endpoints(std::move(dead_endpoints)), _stats(stats), _expire_timer([this] { timeout_cb(); }), _permit(std::move(permit)) {
        // original comment from cassandra:
        // during bootstrap, include pending endpoints in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        _total_block_for = db::block_for(ks, _cl) + pending_endpoints;
        ++_stats.writes;
    }
    virtual ~abstract_write_response_handler() {
        --_stats.writes;
        if (_cl_achieved) {
            if (_throttled) {
                _ready.set_value();
            } else {
                _stats.background_writes--;
                _proxy->_global_stats.background_write_bytes -= _mutation_holder->size();
                _proxy->unthrottle();
            }
        } else {
            if (_error == error::TIMEOUT) {
                _ready.set_exception(mutation_write_timeout_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _total_block_for, _type));
            } else if (_error == error::FAILURE) {
                if (!_message) {
                    _ready.set_exception(mutation_write_failure_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _failed, _total_block_for, _type));
                } else {
                    _ready.set_exception(mutation_write_failure_exception(*_message, _cl, _cl_acks, _failed, _total_block_for, _type));
                }
            }
            if (_cdc_operation_result_tracker) {
                _cdc_operation_result_tracker->on_mutation_failed();
            }
        }
    }
    bool is_counter() const {
        return _type == db::write_type::COUNTER;
    }

    void set_cdc_operation_result_tracker(lw_shared_ptr<cdc::operation_result_tracker> tracker) {
        _cdc_operation_result_tracker = std::move(tracker);
    }

    // While delayed, a request is not throttled.
    void unthrottle() {
        _stats.background_writes++;
        _proxy->_global_stats.background_write_bytes += _mutation_holder->size();
        _throttled = false;
        _ready.set_value();
    }
    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= _total_block_for) {
             _cl_achieved = true;
            delay(get_trace_state(), [] (abstract_write_response_handler* self) {
                if (self->_proxy->need_throttle_writes()) {
                    self->_throttled = true;
                    self->_proxy->_throttled_writes.push_back(self->_id);
                    ++self->_stats.throttled_writes;
                } else {
                    self->unthrottle();
                }
            });
        }
    }

    bool failure(gms::inet_address from, size_t count, error err, std::optional<sstring> msg) {
        if (waited_for(from)) {
            _failed += count;
            if (_total_block_for + _failed > _total_endpoints) {
                _error = err;
                _message = std::move(msg);
                delay(get_trace_state(), [] (abstract_write_response_handler*) { });
                return true;
            }
        }
        return false;
    }

    virtual bool failure(gms::inet_address from, size_t count, error err) {
        return failure(std::move(from), count, std::move(err), {});
    }

    void on_timeout() {
        if (_cl_achieved) {
            slogger.trace("Write is not acknowledged by {} replicas after achieving CL", get_targets());
        }
        _error = error::TIMEOUT;
        // We don't delay request completion after a timeout, but its possible we are currently delaying.
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        auto it = boost::find(_targets, from);
        if (it != _targets.end()) {
            signal(from);
            using std::swap;
            swap(*it, _targets.back());
            _targets.pop_back();
        } else {
            slogger.warn("Receive outdated write ack from {}", from);
        }
        return _targets.size() == 0;
    }
    // return true if handler is no longer needed because
    // CL cannot be reached
    bool failure_response(gms::inet_address from, size_t count, error err, std::optional<sstring> msg) {
        if (boost::find(_targets, from) == _targets.end()) {
            // There is a little change we can get outdated reply
            // if the coordinator was restarted after sending a request and
            // getting reply back. The chance is low though since initial
            // request id is initialized to server starting time
            slogger.warn("Receive outdated write failure from {}", from);
            return false;
        }
        _all_failures += count;
        // we should not fail CL=ANY requests since they may succeed after
        // writing hints
        return _cl != db::consistency_level::ANY && failure(from, count, err, std::move(msg));
    }
    void check_for_early_completion() {
        if (_all_failures == _targets.size()) {
            // leftover targets are all reported error, so nothing to wait for any longer
            timeout_cb();
        }
    }
    void expire_at(storage_proxy::clock_type::time_point timeout) {
        _expire_timer.arm(timeout);
    }
    void on_released() {
        _expire_timer.cancel();
        if (_targets.size() == 0) {
            _mutation_holder->release_mutation();
        }
    }
    void timeout_cb() {
        if (_cl_achieved || _cl == db::consistency_level::ANY) {
            // we are here because either cl was achieved, but targets left in the handler are not
            // responding, so a hint should be written for them, or cl == any in which case
            // hints are counted towards consistency, so we need to write hints and count how much was written
            auto hints = _proxy->hint_to_dead_endpoints(_mutation_holder, get_targets(), _type, get_trace_state());
            signal(hints);
            if (_cl == db::consistency_level::ANY && hints) {
                slogger.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            }
            if (_cl_achieved) { // For CL=ANY this can still be false
                for (auto&& ep : get_targets()) {
                    ++stats().background_replica_writes_failed.get_ep_stat(ep);
                }
                stats().background_writes_failed += int(!_targets.empty());
            }
        }

        on_timeout();
        _proxy->remove_response_handler(_id);
    }
    db::view::update_backlog max_backlog() {
        return boost::accumulate(
                get_targets() | boost::adaptors::transformed([this] (gms::inet_address ep) {
                    return _proxy->get_backlog_of(ep);
                }),
                db::view::update_backlog::no_backlog(),
                [] (const db::view::update_backlog& lhs, const db::view::update_backlog& rhs) {
                    return std::max(lhs, rhs);
                });
    }
    std::chrono::microseconds calculate_delay(db::view::update_backlog backlog) {
        constexpr auto delay_limit_us = 1000000;
        auto adjust = [] (float x) { return x * x * x; };
        auto budget = std::max(storage_proxy::clock_type::duration(0),
            _expire_timer.get_timeout() - storage_proxy::clock_type::now());
        std::chrono::microseconds ret(uint32_t(adjust(backlog.relative_size()) * delay_limit_us));
        // "budget" has millisecond resolution and can potentially be long
        // in the future so converting it to microseconds may overflow.
        // So to compare buget and ret we need to convert both to the lower
        // resolution.
        if (std::chrono::duration_cast<storage_proxy::clock_type::duration>(ret) < budget) {
            return ret;
        } else {
            // budget is small (< ret) so can be converted to microseconds
            return budget;
        }
    }
    // Calculates how much to delay completing the request. The delay adds to the request's inherent latency.
    template<typename Func>
    void delay(tracing::trace_state_ptr trace, Func&& on_resume) {
        auto backlog = max_backlog();
        auto delay = calculate_delay(backlog);
        stats().last_mv_flow_control_delay = delay;
        if (delay.count() == 0) {
            tracing::trace(trace, "Delay decision due to throttling: do not delay, resuming now");
            on_resume(this);
        } else {
            ++stats().throttled_base_writes;
            tracing::trace(trace, "Delaying user write due to view update backlog {}/{} by {}us",
                          backlog.current, backlog.max, delay.count());
            // Waited on indirectly.
            (void)sleep_abortable<seastar::steady_clock_type>(delay).finally([self = shared_from_this(), on_resume = std::forward<Func>(on_resume)] {
                --self->stats().throttled_base_writes;
                on_resume(self.get());
            }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
        }
    }
    future<> wait() {
        return _ready.get_future();
    }
    const inet_address_vector_replica_set& get_targets() const {
        return _targets;
    }
    const inet_address_vector_topology_change& get_dead_endpoints() const {
        return _dead_endpoints;
    }
    bool store_hint(db::hints::manager& hm, gms::inet_address ep, tracing::trace_state_ptr tr_state) {
        return _mutation_holder->store_hint(hm, ep, tr_state);
    }
    future<> apply_locally(storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state) {
        return _mutation_holder->apply_locally(*_proxy, timeout, std::move(tr_state));
    }
    future<> apply_remotely(gms::inet_address ep, inet_address_vector_replica_set&& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) {
        return _mutation_holder->apply_remotely(*_proxy, ep, std::move(forward), response_id, timeout, std::move(tr_state));
    }
    const schema_ptr& get_schema() const {
        return _mutation_holder->schema();
    }
    const size_t get_mutation_size() const {
        return _mutation_holder->size();
    }
    storage_proxy::response_id_type id() const {
      return _id;
    }
    bool read_repair_write() {
        return !_mutation_holder->is_shared();
    }
    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state;
    }
    storage_proxy::write_stats& stats() {
        return _stats;
    }
    friend storage_proxy;
};

class datacenter_write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        return fbu::is_me(from) || db::is_local(from);
    }

public:
    datacenter_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats, service_permit permit) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), stats, std::move(permit), db::count_local_endpoints(pending_endpoints), std::move(dead_endpoints)) {
        _total_endpoints = db::count_local_endpoints(_targets);
    }
};

class write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        return true;
    }
public:
    write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats, service_permit permit) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), stats, std::move(permit), pending_endpoints.size(), std::move(dead_endpoints)) {
        _total_endpoints = _targets.size();
    }
};

class view_update_write_response_handler : public write_response_handler, public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
public:
    view_update_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats, service_permit permit):
                write_response_handler(p, ks, cl, db::write_type::VIEW, std::move(mh),
                        std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit)) {
        register_in_intrusive_list(*p);
    }
    ~view_update_write_response_handler();
private:
    void register_in_intrusive_list(storage_proxy& p);
};

class storage_proxy::view_update_handlers_list : public bi::list<view_update_write_response_handler, bi::base_hook<view_update_write_response_handler>, bi::constant_time_size<false>> {
    // _live_iterators holds all iterators that point into the bi:list in the base class of this object.
    // If we remove a view_update_write_response_handler from the list, and an iterator happens to point
    // into it, we advance the iterator so it doesn't point at a removed object. See #4912.
    std::vector<iterator*> _live_iterators;
public:
    view_update_handlers_list() {
        _live_iterators.reserve(10); // We only expect 1.
    }
    void register_live_iterator(iterator* itp) noexcept { // We don't tolerate failure, so abort instead
        _live_iterators.push_back(itp);
    }
    void unregister_live_iterator(iterator* itp) {
        _live_iterators.erase(boost::remove(_live_iterators, itp), _live_iterators.end());
    }
    void update_live_iterators(view_update_write_response_handler* vuwrh) {
        // vuwrh is being removed from the b::list, so if any live iterator points at it,
        // move it to the next object (this requires that the list is traversed in the forward
        // direction).
        for (auto& itp : _live_iterators) {
            if (&**itp == vuwrh) {
                ++*itp;
            }
        }
    }
    class iterator_guard {
        view_update_handlers_list& _vuhl;
        iterator* _itp;
    public:
        iterator_guard(view_update_handlers_list& vuhl, iterator& it) : _vuhl(vuhl), _itp(&it) {
            _vuhl.register_live_iterator(_itp);
        }
        ~iterator_guard() {
            _vuhl.unregister_live_iterator(_itp);
        }
    };
};

void view_update_write_response_handler::register_in_intrusive_list(storage_proxy& p) {
    p.get_view_update_handlers_list().push_back(*this);
}


view_update_write_response_handler::~view_update_write_response_handler() {
    _proxy->_view_update_handlers_list->update_live_iterators(this);
}

class datacenter_sync_write_response_handler : public abstract_write_response_handler {
    struct dc_info {
        size_t acks;
        size_t total_block_for;
        size_t total_endpoints;
        size_t failures;
    };
    std::unordered_map<sstring, dc_info> _dc_responses;
    bool waited_for(gms::inet_address from) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        sstring data_center = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(data_center);

        if (dc_resp->second.acks < dc_resp->second.total_block_for) {
            ++dc_resp->second.acks;
            return true;
        }
        return false;
    }
public:
    datacenter_sync_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets, const inet_address_vector_topology_change& pending_endpoints,
            inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats, service_permit permit) :
        abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh), targets, std::move(tr_state), stats, std::move(permit), 0, dead_endpoints) {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        for (auto& target : targets) {
            auto dc = snitch_ptr->get_datacenter(target);

            if (!_dc_responses.contains(dc)) {
                auto pending_for_dc = boost::range::count_if(pending_endpoints, [&snitch_ptr, &dc] (const gms::inet_address& ep){
                    return snitch_ptr->get_datacenter(ep) == dc;
                });
                size_t total_endpoints_for_dc = boost::range::count_if(targets, [&snitch_ptr, &dc] (const gms::inet_address& ep){
                    return snitch_ptr->get_datacenter(ep) == dc;
                });
                _dc_responses.emplace(dc, dc_info{0, db::local_quorum_for(ks, dc) + pending_for_dc, total_endpoints_for_dc, 0});
                _total_block_for += pending_for_dc;
            }
        }
    }
    bool failure(gms::inet_address from, size_t count, error err) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        const sstring& dc = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(dc);

        dc_resp->second.failures += count;
        _failed += count;
        if (dc_resp->second.total_block_for + dc_resp->second.failures > dc_resp->second.total_endpoints) {
            _error = err;
            return true;
        }
        return false;
    }
};

static future<> sleep_approx_50ms() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<> dist(0, 100);
    return seastar::sleep(std::chrono::milliseconds(dist(re)));
}

/**
 * Begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies.
 *
 * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
 * nodes have seen the most recent commit. Otherwise, return null.
 */
future<paxos_response_handler::ballot_and_data>
paxos_response_handler::begin_and_repair_paxos(client_state& cs, unsigned& contentions, bool is_write) {
    if (!_proxy->features().cluster_supports_lwt()) {
        co_return coroutine::make_exception(std::runtime_error("The cluster does not support Paxos. Upgrade all the nodes to the version with LWT support."));
    }

    api::timestamp_type min_timestamp_micros_to_use = 0;
    auto _ = shared_from_this(); // hold the handler until co-routine ends

    while(true) {
        if (storage_proxy::clock_type::now() > _cas_timeout) {
            co_return coroutine::make_exception(
                mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl_for_paxos, 0, _required_participants, db::write_type::CAS)
            );
        }

        // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is
        // globally unique), but if we've got a prepare rejected already we also want to make sure
        // we pick a timestamp that has a chance to be promised, i.e. one that is greater that the
        // most recently known in progress (#5667). Lastly, we don't want to use a timestamp that is
        // older than the last one assigned by ClientState or operations may appear out-of-order
        // (#7801).
        api::timestamp_type ballot_micros = cs.get_timestamp_for_paxos(min_timestamp_micros_to_use);
        // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled
        // concurrently by the same coordinator. But we still need ballots to be unique for each
        // proposal so we have to use getRandomTimeUUIDFromMicros.
        utils::UUID ballot = utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{ballot_micros});

        paxos::paxos_state::logger.debug("CAS[{}] Preparing {}", _id, ballot);
        tracing::trace(tr_state, "Preparing {}", ballot);

        paxos::prepare_summary summary = co_await prepare_ballot(ballot);

        if (!summary.promised) {
            paxos::paxos_state::logger.debug("CAS[{}] Some replicas have already promised a higher ballot than ours; aborting", _id);
            tracing::trace(tr_state, "Some replicas have already promised a higher ballot than ours; aborting");
            contentions++;
            co_await sleep_approx_50ms();
            continue;
        }

        min_timestamp_micros_to_use = utils::UUID_gen::micros_timestamp(summary.most_recent_promised_ballot) + 1;

        std::optional<paxos::proposal> in_progress = std::move(summary.most_recent_proposal);

        // If we have an in-progress accepted ballot greater than the most recent commit
        // we know, then it's an in-progress round that needs to be completed, so do it.
        if (in_progress &&
            (!summary.most_recent_commit || (summary.most_recent_commit && in_progress->ballot.timestamp() > summary.most_recent_commit->ballot.timestamp()))) {
            paxos::paxos_state::logger.debug("CAS[{}] Finishing incomplete paxos round {}", _id, *in_progress);
            tracing::trace(tr_state, "Finishing incomplete paxos round {}", *in_progress);
            if (is_write) {
                ++_proxy->get_stats().cas_write_unfinished_commit;
            } else {
                ++_proxy->get_stats().cas_read_unfinished_commit;
            }

            auto refreshed_in_progress = make_lw_shared<paxos::proposal>(ballot, std::move(in_progress->update));

            bool is_accepted = co_await accept_proposal(refreshed_in_progress, false);

            if (is_accepted) {
                try {
                    co_await learn_decision(std::move(refreshed_in_progress), false);
                    continue;
                } catch (mutation_write_timeout_exception& e) {
                    e.type = db::write_type::CAS;
                    // we're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                    co_return coroutine::make_exception(std::move(e));
                }
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] Some replicas have already promised a higher ballot than ours; aborting", _id);
                tracing::trace(tr_state, "Some replicas have already promised a higher ballot than ours; aborting");
                // sleep a random amount to give the other proposer a chance to finish
                contentions++;
                co_await sleep_approx_50ms();
                continue;
            }
            assert(true); // no fall through
        }

        // To be able to propose our value on a new round, we need a quorum of replica to have learn
        // the previous one. Why is explained at:
        // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
        // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may
        // just be a timing issue, but may also mean we lost messages), we pro-actively "repair"
        // those nodes, and retry.
        auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(ballot);

        inet_address_vector_replica_set missing_mrc = summary.replicas_missing_most_recent_commit(_schema, now_in_sec);
        if (missing_mrc.size() > 0) {
            paxos::paxos_state::logger.debug("CAS[{}] Repairing replicas that missed the most recent commit", _id);
            tracing::trace(tr_state, "Repairing replicas that missed the most recent commit");
            std::array<std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, dht::token, inet_address_vector_replica_set>, 1>
                m{std::make_tuple(make_lw_shared<paxos::proposal>(std::move(*summary.most_recent_commit)), _schema, _key.token(), std::move(missing_mrc))};
            // create_write_response_handler is overloaded for paxos::proposal and will
            // create cas_mutation holder, which consequently will ensure paxos::learn is
            // used.
            auto f = _proxy->mutate_internal(std::move(m), db::consistency_level::ANY, false, tr_state, _permit, _timeout);

            // TODO: provided commits did not invalidate the prepare we just did above (which they
            // didn't), we could just wait for all the missing most recent commits to
            // acknowledge this decision and then move on with proposing our value.
            try {
                co_await std::move(f);
            } catch(...) {
                paxos::paxos_state::logger.debug("CAS[{}] Failure during commit repair {}", _id, std::current_exception());
                continue;
            }
        }
        co_return ballot_and_data{ballot, std::move(summary.data)};
    }
}

template<class T> struct dependent_false : std::false_type {};

// This function implement prepare stage of Paxos protocol and collects metadata needed to repair
// previously unfinished round (if there was one).
future<paxos::prepare_summary> paxos_response_handler::prepare_ballot(utils::UUID ballot) {
    struct {
        size_t errors = 0;
        // Whether the value of the requested key received from participating replicas match.
        bool digests_match = true;
        // Digest corresponding to the value of the requested key received from participating replicas.
        std::optional<query::result_digest> digest;
        // the promise can be set before all replies are received at which point
        // the optional will be disengaged so further replies are ignored
        std::optional<promise<paxos::prepare_summary>> p = promise<paxos::prepare_summary>();
        void set_value(paxos::prepare_summary&& s) {
            p->set_value(std::move(s));
            p.reset();
        }
        void set_exception(std::exception_ptr&& e) {
            p->set_exception(std::move(e));
            p.reset();
        }
    } request_tracker;

    auto f = request_tracker.p->get_future();

    // We may continue collecting prepare responses in the background after the reply is ready
    (void)do_with(paxos::prepare_summary(_live_endpoints.size()), std::move(request_tracker), shared_from_this(),
            [this, ballot] (paxos::prepare_summary& summary, auto& request_tracker, shared_ptr<paxos_response_handler>& prh) mutable -> future<> {
        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: sending ballot {} to {}", _id, ballot, _live_endpoints);
        auto handle_one_msg = [this, &summary, ballot, &request_tracker] (gms::inet_address peer) mutable -> future<> {
            paxos::prepare_response response;
            try {
                // To generate less network traffic, only the closest replica (first one in the list of participants)
                // sends query result content while other replicas send digests needed to check consistency.
                bool only_digest = peer != _live_endpoints[0];
                auto da = digest_algorithm(get_local_storage_proxy());
                if (fbu::is_me(peer)) {
                    tracing::trace(tr_state, "prepare_ballot: prepare {} locally", ballot);
                    response = co_await paxos::paxos_state::prepare(tr_state, _schema, *_cmd, _key.key(), ballot, only_digest, da, _timeout);
                } else {
                    tracing::trace(tr_state, "prepare_ballot: sending prepare {} to {}", ballot, peer);
                    response = co_await _proxy->_messaging.send_paxos_prepare(peer, _timeout, *_cmd, _key.key(), ballot, only_digest, da,
                            tracing::make_trace_info(tr_state));
                }
            } catch (...) {
                if (request_tracker.p) {
                    auto ex = std::current_exception();
                    if (is_timeout_exception(ex)) {
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: timeout while sending ballot {} to {}", _id,
                                    ballot, peer);
                        auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(),
                                    _cl_for_paxos, summary.committed_ballots_by_replica.size(),  _required_participants,
                                    db::write_type::CAS));
                        request_tracker.set_exception(std::move(e));
                    } else {
                        request_tracker.errors++;
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: fail to send ballot {} to {}: {}", _id,
                                ballot, peer, ex);
                        if (_required_participants + request_tracker.errors > _live_endpoints.size()) {
                            auto e = std::make_exception_ptr(mutation_write_failure_exception(_schema->ks_name(),
                                        _schema->cf_name(), _cl_for_paxos, summary.committed_ballots_by_replica.size(),
                                        request_tracker.errors, _required_participants, db::write_type::CAS));
                            request_tracker.set_exception(std::move(e));
                        }
                    }
                }
                co_return;
            }

            if (!request_tracker.p) {
                co_return;
            }

            auto on_prepare_response = [&] (auto&& response) {
                using T = std::decay_t<decltype(response)>;
                if constexpr (std::is_same_v<T, utils::UUID>) {
                    tracing::trace(tr_state, "prepare_ballot: got more up to date ballot {} from /{}", response, peer);
                    paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got more up to date ballot {} from {}", _id, response, peer);
                    // We got an UUID that prevented our proposal from succeeding
                    summary.update_most_recent_promised_ballot(response);
                    summary.promised = false;
                    request_tracker.set_value(std::move(summary));
                    return;
                } else if constexpr (std::is_same_v<T, paxos::promise>) {
                    utils::UUID mrc_ballot = utils::UUID_gen::min_time_UUID();

                    paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got a response {} from {}", _id, response, peer);
                    tracing::trace(tr_state, "prepare_ballot: got a response {} from /{}", response, peer);

                    // Find the newest learned value among all replicas that answered.
                    // It will be used to "repair" replicas that did not learn this value yet.
                    if (response.most_recent_commit) {
                        mrc_ballot = response.most_recent_commit->ballot;

                        if (!summary.most_recent_commit ||
                            summary.most_recent_commit->ballot.timestamp() < mrc_ballot.timestamp()) {
                            summary.most_recent_commit = std::move(response.most_recent_commit);
                        }
                    }

                    // cannot throw since the memory was reserved ahead
                    summary.committed_ballots_by_replica.emplace(peer, mrc_ballot);

                    if (response.accepted_proposal) {
                        summary.update_most_recent_promised_ballot(response.accepted_proposal->ballot);

                        // If some response has an accepted proposal, then we should replay the proposal with the highest ballot.
                        // So find the highest accepted proposal here.
                        if (!summary.most_recent_proposal || response.accepted_proposal > summary.most_recent_proposal) {
                            summary.most_recent_proposal = std::move(response.accepted_proposal);
                        }
                    }

                    // Check if the query result attached to the promise matches query results received from other participants.
                    if (request_tracker.digests_match) {
                        if (response.data_or_digest) {
                            foreign_ptr<lw_shared_ptr<query::result>> data;
                            if (std::holds_alternative<foreign_ptr<lw_shared_ptr<query::result>>>(*response.data_or_digest)) {
                                data = std::move(std::get<foreign_ptr<lw_shared_ptr<query::result>>>(*response.data_or_digest));
                            }
                            auto& digest = data ? data->digest() : std::get<query::result_digest>(*response.data_or_digest);
                            if (request_tracker.digest) {
                                if (*request_tracker.digest != digest) {
                                    request_tracker.digests_match = false;
                                }
                            } else {
                                request_tracker.digest = digest;
                            }
                            if (request_tracker.digests_match && !summary.data && data) {
                                summary.data = std::move(data);
                            }
                        } else {
                            request_tracker.digests_match = false;
                        }
                        if (!request_tracker.digests_match) {
                            request_tracker.digest.reset();
                            summary.data.reset();
                        }
                    }

                    if (summary.committed_ballots_by_replica.size() == _required_participants) { // got all replies
                        tracing::trace(tr_state, "prepare_ballot: got enough replies to proceed");
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got enough replies to proceed", _id);
                        request_tracker.set_value(std::move(summary));
                    }
                } else {
                    static_assert(dependent_false<T>::value, "unexpected type!");
                }
            };
            std::visit(on_prepare_response, std::move(response));
        };
        co_return co_await parallel_for_each(_live_endpoints, handle_one_msg);
    });

    return f;
}

// This function implements accept stage of the Paxos protocol.
future<bool> paxos_response_handler::accept_proposal(lw_shared_ptr<paxos::proposal> proposal, bool timeout_if_partially_accepted) {
    struct {
        // the promise can be set before all replies are received at which point
        // the optional will be disengaged so further replies are ignored
        std::optional<promise<bool>> p = promise<bool>();
        size_t accepts = 0;
        size_t rejects = 0;
        size_t errors = 0;

        size_t all_replies() const {
            return accepts + rejects + errors;
        }
        size_t non_accept_replies() const {
            return rejects + errors;
        }
        size_t non_error_replies() const {
            return accepts + rejects;
        }
        void set_value(bool v) {
            p->set_value(v);
            p.reset();
        }
        void set_exception(std::exception_ptr&& e) {
            p->set_exception(std::move(e));
            p.reset();
        }
    } request_tracker;

    auto f = request_tracker.p->get_future();

    // We may continue collecting propose responses in the background after the reply is ready
    (void)do_with(std::move(request_tracker), shared_from_this(), [this, timeout_if_partially_accepted, proposal = std::move(proposal)]
                           (auto& request_tracker, shared_ptr<paxos_response_handler>& prh) -> future<> {
        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: sending commit {} to {}", _id, *proposal, _live_endpoints);
        auto handle_one_msg = [this, &request_tracker, timeout_if_partially_accepted, proposal = std::move(proposal)] (gms::inet_address peer) mutable -> future<> {
            bool is_timeout = false;
            std::optional<bool> accepted;

            try {
                if (fbu::is_me(peer)) {
                    tracing::trace(tr_state, "accept_proposal: accept {} locally", *proposal);
                    accepted = co_await paxos::paxos_state::accept(tr_state, _schema, proposal->update.decorated_key(*_schema).token(), *proposal, _timeout);
                } else {
                    tracing::trace(tr_state, "accept_proposal: send accept {} to {}", *proposal, peer);
                    accepted = co_await _proxy->_messaging.send_paxos_accept(peer, _timeout, *proposal, tracing::make_trace_info(tr_state));
                }
            } catch(...) {
                if (request_tracker.p) {
                    auto ex = std::current_exception();
                    if (is_timeout_exception(ex)) {
                        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: timeout while sending proposal {} to {}",
                                _id, *proposal, peer);
                        is_timeout = true;
                    } else {
                        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: failure while sending proposal {} to {}: {}", _id,
                                *proposal, peer, ex);
                        request_tracker.errors++;
                    }
                }
            }

            if (!request_tracker.p) {
                // Ignore the response since a completion was already signaled.
                co_return;
            }

            if (accepted) {
                tracing::trace(tr_state, "accept_proposal: got \"{}\" from /{}", *accepted ? "accepted" : "rejected", peer);
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got \"{}\" from {}", _id,
                        accepted ? "accepted" : "rejected", peer);

                *accepted ? request_tracker.accepts++ : request_tracker.rejects++;
            }

            /**
            * The code has two modes of operation, controlled by the timeout_if_partially_accepted parameter.
            *
            * In timeout_if_partially_accepted is false, we will return a failure as soon as a majority of nodes reject
            * the proposal. This is used when replaying a proposal from an earlier leader.
            *
            * Otherwise, we wait for either all replicas to respond or until we achieve
            * the desired quorum. We continue to wait for all replicas even after we know we cannot succeed
            * because we need to know if no node at all has accepted our proposal or if at least one has.
            * In the former case, a proposer is guaranteed no-one will replay its value; in the
            * latter we don't, so we must timeout in case another leader replays it before we
            * can; see CASSANDRA-6013.
            */
            if (request_tracker.accepts == _required_participants) {
                tracing::trace(tr_state, "accept_proposal: got enough accepts to proceed");
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got enough accepts to proceed", _id);
                request_tracker.set_value(true);
            } else if (is_timeout) {
                auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(),
                            _cl_for_paxos, request_tracker.non_error_replies(), _required_participants, db::write_type::CAS));
                request_tracker.set_exception(std::move(e));
            } else if (_required_participants + request_tracker.errors > _live_endpoints.size()) {
                // We got one too many errors. The quorum is no longer reachable. We can fail here
                // timeout_if_partially_accepted or not because failing is always safe - a client cannot
                // assume that the value was not committed.
                auto e = std::make_exception_ptr(mutation_write_failure_exception(_schema->ks_name(),
                            _schema->cf_name(), _cl_for_paxos, request_tracker.non_error_replies(),
                            request_tracker.errors, _required_participants, db::write_type::CAS));
                request_tracker.set_exception(std::move(e));
            } else if (_required_participants + request_tracker.non_accept_replies()  > _live_endpoints.size() && !timeout_if_partially_accepted) {
                // In case there is no need to reply with a timeout if at least one node is accepted
                // we can fail the request as soon is we know a quorum is unreachable.
                tracing::trace(tr_state, "accept_proposal: got enough rejects to proceed");
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got enough rejects to proceed", _id);
                request_tracker.set_value(false);
            } else if (request_tracker.all_replies() == _live_endpoints.size()) { // wait for all replies
                if (request_tracker.accepts == 0 && request_tracker.errors == 0) {
                    tracing::trace(tr_state, "accept_proposal: proposal is fully rejected");
                    paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: proposal is fully rejected", _id);
                    // Return false if fully refused. Consider errors as accepts here since it
                    // is not possible to know for sure.
                    request_tracker.set_value(false);
                } else {
                    // We got some rejects, but not all, and there were errors. So we can't know for
                    // sure that the proposal is fully rejected, and it is obviously not
                    // accepted, either.
                    paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: proposal is partially rejected", _id);
                    tracing::trace(tr_state, "accept_proposal: proposal is partially rejected");
                    _proxy->get_stats().cas_write_timeout_due_to_uncertainty++;
                    // TODO: we report write timeout exception to be compatible with Cassandra,
                    // which uses write_timeout_exception to signal any "unknown" state.
                    // To be changed in scope of work on https://issues.apache.org/jira/browse/CASSANDRA-15350
                    auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(),
                                _schema->cf_name(), _cl_for_paxos, request_tracker.accepts, _required_participants,
                                db::write_type::CAS));
                    request_tracker.set_exception(std::move(e));
                }
            } // wait for more replies
        };
        co_return co_await parallel_for_each(_live_endpoints, handle_one_msg);
    }); // do_with

    return f;
}

// debug output in mutate_internal needs this
std::ostream& operator<<(std::ostream& os, const paxos_response_handler& h) {
    os << "paxos_response_handler{" << h.id() << "}";
    return os;
}

// This function implements learning stage of Paxos protocol
future<> paxos_response_handler::learn_decision(lw_shared_ptr<paxos::proposal> decision, bool allow_hints) {
    tracing::trace(tr_state, "learn_decision: committing {} with cl={}", *decision, _cl_for_learn);
    paxos::paxos_state::logger.trace("CAS[{}] learn_decision: committing {} with cl={}", _id, *decision, _cl_for_learn);
    // FIXME: allow_hints is ignored. Consider if we should follow it and remove if not.
    // Right now we do not store hints for when committing decisions.

    // `mutate_internal` behaves differently when its template parameter is a range of mutations and when it's
    // a range of (decision, schema, token)-tuples. Both code paths diverge on `create_write_response_handler`.
    // We use the first path for CDC mutations (if present) and the latter for "paxos mutations".
    // Attempts to send both kinds of mutations in one shot caused an infinite loop.
    future<> f_cdc = make_ready_future<>();
    if (_schema->cdc_options().enabled()) {
        auto update_mut = decision->update.unfreeze(_schema);
        const auto base_tbl_id = update_mut.column_family_id();
        std::vector<mutation> update_mut_vec{std::move(update_mut)};

        auto cdc = _proxy->get_cdc_service();
        if (cdc && cdc->needs_cdc_augmentation(update_mut_vec)) {
            f_cdc = cdc->augment_mutation_call(_timeout, std::move(update_mut_vec), tr_state, _cl_for_learn)
                    .then([this, base_tbl_id, cdc = cdc->shared_from_this()] (std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>&& t) {
                auto mutations = std::move(std::get<0>(t));
                auto tracker = std::move(std::get<1>(t));
                // Pick only the CDC ("augmenting") mutations
                std::erase_if(mutations, [base_tbl_id = std::move(base_tbl_id)] (const mutation& v) {
                    return v.schema()->id() == base_tbl_id;
                });
                if (mutations.empty()) {
                    return make_ready_future<>();
                }
                return _proxy->mutate_internal(std::move(mutations), _cl_for_learn, false, tr_state, _permit, _timeout, std::move(tracker));
            });
        }
    }

    // Path for the "base" mutations
    std::array<std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>, 1> m{std::make_tuple(std::move(decision), _schema, shared_from_this(), _key.token())};
    future<> f_lwt = _proxy->mutate_internal(std::move(m), _cl_for_learn, false, tr_state, _permit, _timeout);

    return when_all_succeed(std::move(f_cdc), std::move(f_lwt)).discard_result();
}

void paxos_response_handler::prune(utils::UUID ballot) {
    if ( _proxy->get_stats().cas_now_pruning >= pruning_limit) {
        _proxy->get_stats().cas_coordinator_dropped_prune++;
        return;
    }
     _proxy->get_stats().cas_now_pruning++;
    _proxy->get_stats().cas_prune++;
    // running in the background, but the amount of the bg job is limited by pruning_limit
    // it is waited by holding shared pointer to storage_proxy which guaranties
    // that storage_proxy::stop() will wait for this to complete
    (void)parallel_for_each(_live_endpoints, [this, ballot] (gms::inet_address peer) mutable {
        if (fbu::is_me(peer)) {
            tracing::trace(tr_state, "prune: prune {} locally", ballot);
            return paxos::paxos_state::prune(_schema, _key.key(), ballot, _timeout, tr_state);
        } else {
            tracing::trace(tr_state, "prune: send prune of {} to {}", ballot, peer);
            return _proxy->_messaging.send_paxos_prune(peer, _timeout, _schema->version(), _key.key(), ballot, tracing::make_trace_info(tr_state));
        }
    }).then_wrapped([this, h = shared_from_this()] (future<> f) {
        h->_proxy->get_stats().cas_now_pruning--;
        try {
            f.get();
        } catch (rpc::closed_error&) {
            // ignore errors due to closed connection
            tracing::trace(tr_state, "prune failed: connection closed");
        } catch (const mutation_write_timeout_exception& ex) {
            tracing::trace(tr_state, "prune failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
            paxos::paxos_state::logger.debug("CAS[{}] prune: failed {}", h->_id, std::current_exception());
        } catch (...) {
            tracing::trace(tr_state, "prune failed: {}", std::current_exception());
            paxos::paxos_state::logger.error("CAS[{}] prune: failed {}", h->_id, std::current_exception());
        }
    });
}

bool paxos_response_handler::learned(gms::inet_address ep) {
    if (_learned < _required_participants) {
        if (boost::range::find(_live_endpoints, ep) != _live_endpoints.end()) {
            _learned++;
            return _learned == _required_participants;
        }
    }
    return false;
}

static inet_address_vector_replica_set
replica_ids_to_endpoints(const locator::token_metadata& tm, const std::vector<utils::UUID>& replica_ids) {
    inet_address_vector_replica_set endpoints;
    endpoints.reserve(replica_ids.size());

    for (const auto& replica_id : replica_ids) {
        if (auto endpoint_opt = tm.get_endpoint_for_host_id(replica_id)) {
            endpoints.push_back(*endpoint_opt);
        }
    }

    return endpoints;
}

static std::vector<utils::UUID>
endpoints_to_replica_ids(const locator::token_metadata& tm, const inet_address_vector_replica_set& endpoints) {
    std::vector<utils::UUID> replica_ids;
    replica_ids.reserve(endpoints.size());

    for (const auto& endpoint : endpoints) {
        if (auto replica_id_opt = tm.get_host_id_if_known(endpoint)) {
            replica_ids.push_back(*replica_id_opt);
        }
    }

    return replica_ids;
}

query::max_result_size storage_proxy::get_max_result_size(const query::partition_slice& slice) const {
    // Unpaged and reverse queries.
    // FIXME: some reversed queries are no longer unlimited.
    // To filter these out we need to know if it's a single partition query.
    // Take the partition key range as a parameter? This would require changing many call sites
    // where the partition key range is not immediately available.
    // For now we ignore the issue, return the 'wrong' limit for some reversed queries, and wait until all reversed queries are fixed.
    if (!slice.options.contains<query::partition_slice::option::allow_short_read>() || slice.options.contains<query::partition_slice::option::reversed>()) {
        return _db.local().get_unlimited_query_max_result_size();
    } else {
        return query::max_result_size(query::result_memory_limiter::maximum_result_size);
    }
}

bool storage_proxy::need_throttle_writes() const {
    return get_global_stats().background_write_bytes > _background_write_throttle_threahsold || get_global_stats().queued_write_bytes > 6*1024*1024;
}

void storage_proxy::unthrottle() {
   while(!need_throttle_writes() && !_throttled_writes.empty()) {
       auto id = _throttled_writes.front();
       _throttled_writes.pop_front();
       auto it = _response_handlers.find(id);
       if (it != _response_handlers.end()) {
           it->second->unthrottle();
       }
   }
}

storage_proxy::response_id_type storage_proxy::register_response_handler(shared_ptr<abstract_write_response_handler>&& h) {
    auto id = h->id();
    auto e = _response_handlers.emplace(id, std::move(h));
    assert(e.second);
    return id;
}

void storage_proxy::remove_response_handler(storage_proxy::response_id_type id) {
    auto entry = _response_handlers.find(id);
    assert(entry != _response_handlers.end());
    remove_response_handler_entry(std::move(entry));
}

void storage_proxy::remove_response_handler_entry(response_handlers_map::iterator entry) {
    entry->second->on_released();
    _response_handlers.erase(std::move(entry));
}

void storage_proxy::got_response(storage_proxy::response_id_type id, gms::inet_address from, std::optional<db::view::update_backlog> backlog) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got a response from /{}", from);
        if (it->second->response(from)) {
            remove_response_handler_entry(std::move(it)); // last one, remove entry. Will cancel expiration timer too.
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::got_failure_response(storage_proxy::response_id_type id, gms::inet_address from, size_t count, std::optional<db::view::update_backlog> backlog, error err, std::optional<sstring> msg) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got {} failures from /{}", count, from);
        if (it->second->failure_response(from, count, err, std::move(msg))) {
            remove_response_handler_entry(std::move(it));
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::maybe_update_view_backlog_of(gms::inet_address replica, std::optional<db::view::update_backlog> backlog) {
    if (backlog) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        _view_update_backlogs[replica] = {std::move(*backlog), now};
    }
}

db::view::update_backlog storage_proxy::get_view_update_backlog() const {
    return _max_view_update_backlog.add_fetch(this_shard_id(), get_db().local().get_view_update_backlog());
}

db::view::update_backlog storage_proxy::get_backlog_of(gms::inet_address ep) const {
    auto it = _view_update_backlogs.find(ep);
    if (it == _view_update_backlogs.end()) {
        return db::view::update_backlog::no_backlog();
    }
    return it->second.backlog;
}

future<> storage_proxy::response_wait(storage_proxy::response_id_type id, clock_type::time_point timeout) {
    auto& handler = _response_handlers.find(id)->second;
    handler->expire_at(timeout);
    return handler->wait();
}

::shared_ptr<abstract_write_response_handler>& storage_proxy::get_write_response_handler(storage_proxy::response_id_type id) {
        return _response_handlers.find(id)->second;
}

storage_proxy::response_id_type storage_proxy::create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m,
                             inet_address_vector_replica_set targets, const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
                             storage_proxy::write_stats& stats, service_permit permit)
{
    shared_ptr<abstract_write_response_handler> h;
    auto& rs = ks.get_replication_strategy();

    set_replicas(tr_state, targets);

    if (db::is_datacenter_local(cl)) {
        h = ::make_shared<datacenter_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit));
    } else if (cl == db::consistency_level::EACH_QUORUM && rs.get_type() == locator::replication_strategy_type::network_topology){
        h = ::make_shared<datacenter_sync_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit));
    } else if (type == db::write_type::VIEW) {
        h = ::make_shared<view_update_write_response_handler>(shared_from_this(), ks, cl, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit));
    } else {
        h = ::make_shared<write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit));
    }
    return register_response_handler(std::move(h));
}

seastar::metrics::label storage_proxy_stats::split_stats::datacenter_label("datacenter");

storage_proxy_stats::split_stats::split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type, bool auto_register_metrics)
        : _short_description_prefix(short_description_prefix)
        , _long_description_prefix(long_description_prefix)
        , _category(category)
        , _op_type(op_type)
        , _auto_register_metrics(auto_register_metrics) { }

storage_proxy_stats::write_stats::write_stats()
: writes_attempts(COORDINATOR_STATS_CATEGORY, "total_write_attempts", "total number of write requests", "mutation_data")
, writes_errors(COORDINATOR_STATS_CATEGORY, "write_errors", "number of write requests that failed", "mutation_data")
, background_replica_writes_failed(COORDINATOR_STATS_CATEGORY, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data")
, read_repair_write_attempts(COORDINATOR_STATS_CATEGORY, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data") { }

storage_proxy_stats::write_stats::write_stats(const sstring& category, bool auto_register_stats)
        : writes_attempts(category, "total_write_attempts", "total number of write requests", "mutation_data", auto_register_stats)
        , writes_errors(category, "write_errors", "number of write requests that failed", "mutation_data", auto_register_stats)
        , background_replica_writes_failed(category, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data", auto_register_stats)
        , read_repair_write_attempts(category, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data", auto_register_stats) { }

void storage_proxy_stats::write_stats::register_split_metrics_local() {
    writes_attempts.register_metrics_local();
    writes_errors.register_metrics_local();
    background_replica_writes_failed.register_metrics_local();
    read_repair_write_attempts.register_metrics_local();
}

void storage_proxy_stats::write_stats::register_stats() {
    namespace sm = seastar::metrics;
    _metrics.add_group(COORDINATOR_STATS_CATEGORY, {
            sm::make_histogram("write_latency", sm::description("The general write latency histogram"),
                    {storage_proxy_stats::current_scheduling_group_label()},
                    [this]{return to_metrics_histogram(estimated_write);}),

            sm::make_queue_length("foreground_writes", [this] { return writes - background_writes; },
                           sm::description("number of currently pending foreground write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_queue_length("background_writes", background_writes,
                           sm::description("number of currently pending background write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_queue_length("current_throttled_base_writes", throttled_base_writes,
                           sm::description("number of currently throttled base replica write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_gauge("last_mv_flow_control_delay", [this] { return std::chrono::duration<float>(last_mv_flow_control_delay).count(); },
                                          sm::description("delay (in seconds) added for MV flow control in the last request"),
                                          {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("throttled_writes", throttled_writes,
                                      sm::description("number of throttled write requests"),
                                      {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("write_timeouts", write_timeouts._count,
                           sm::description("number of write request failed due to a timeout"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("write_unavailable", write_unavailables._count,
                           sm::description("number write requests failed due to an \"unavailable\" error"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("background_writes_failed", background_writes_failed,
                           sm::description("number of write requests that failed after CL was reached"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("writes_coordinator_outside_replica_set", writes_coordinator_outside_replica_set,
                    sm::description("number of CQL write requests which arrived to a non-replica and had to be forwarded to a replica"),
                    {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("reads_coordinator_outside_replica_set", reads_coordinator_outside_replica_set,
                    sm::description("number of CQL read requests which arrived to a non-replica and had to be forwarded to a replica"),
                    {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_total_operations("writes_failed_due_to_too_many_in_flight_hints", writes_failed_due_to_too_many_in_flight_hints,
                    sm::description("number of CQL write requests which failed because the hinted handoff mechanism is overloaded "
                    "and cannot store any more in-flight hints"),
                    {storage_proxy_stats::current_scheduling_group_label()}),
        });
}

storage_proxy_stats::stats::stats()
        : write_stats()
        , data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of data read requests", "data")
        , data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of data read requests that completed", "data")
        , data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of data read requests that failed", "data")
        , digest_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of digest read requests", "digest")
        , digest_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of digest read requests that completed", "digest")
        , digest_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of digest read requests that failed", "digest")
        , mutation_data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of mutation data read requests", "mutation_data")
        , mutation_data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of mutation data read requests that completed", "mutation_data")
        , mutation_data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of mutation data read requests that failed", "mutation_data") { }

void storage_proxy_stats::stats::register_split_metrics_local() {
    write_stats::register_split_metrics_local();

    data_read_attempts.register_metrics_local();
    data_read_completed.register_metrics_local();
    data_read_errors.register_metrics_local();
    digest_read_attempts.register_metrics_local();
    digest_read_completed.register_metrics_local();
    mutation_data_read_attempts.register_metrics_local();
    mutation_data_read_completed.register_metrics_local();
    mutation_data_read_errors.register_metrics_local();
}

void storage_proxy_stats::stats::register_stats() {
    namespace sm = seastar::metrics;
    write_stats::register_stats();
    _metrics.add_group(COORDINATOR_STATS_CATEGORY, {
        sm::make_histogram("read_latency", sm::description("The general read latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{ return to_metrics_histogram(estimated_read);}),

        sm::make_queue_length("foreground_reads", foreground_reads,
                sm::description("number of currently pending foreground read requests"),
                {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_queue_length("background_reads", [this] { return reads - foreground_reads; },
                       sm::description("number of currently pending background read requests"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("read_retries", read_retries,
                       sm::description("number of read retry attempts"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("canceled_read_repairs", global_read_repairs_canceled_due_to_concurrent_write,
                       sm::description("number of global read repairs canceled due to a concurrent write"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("foreground_read_repairs", read_repair_repaired_blocking,
                      sm::description("number of foreground read repairs"),
                      {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("background_read_repairs", read_repair_repaired_background,
                       sm::description("number of background read repairs"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("read_timeouts", read_timeouts._count,
                       sm::description("number of read request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("read_unavailable", read_unavailables._count,
                       sm::description("number read requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("range_timeouts", range_slice_timeouts._count,
                       sm::description("number of range read operations failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("range_unavailable", range_slice_unavailables._count,
                       sm::description("number of range read operations failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("speculative_digest_reads", speculative_digest_reads,
                       sm::description("number of speculative digest read requests that were sent"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("speculative_data_reads", speculative_data_reads,
                       sm::description("number of speculative data read requests that were sent"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_histogram("cas_read_latency", sm::description("Transactional read latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{ return to_metrics_histogram(estimated_cas_read);}),

        sm::make_histogram("cas_write_latency", sm::description("Transactional write latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{return to_metrics_histogram(estimated_cas_write);}),

        sm::make_total_operations("cas_write_timeouts", cas_write_timeouts._count,
                       sm::description("number of transactional write request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_write_unavailable", cas_write_unavailables._count,
                       sm::description("number of transactional write requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_read_timeouts", cas_read_timeouts._count,
                       sm::description("number of transactional read request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_read_unavailable", cas_read_unavailables._count,
                       sm::description("number of transactional read requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}),


        sm::make_total_operations("cas_read_unfinished_commit", cas_read_unfinished_commit,
                       sm::description("number of transaction commit attempts that occurred on read"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_write_unfinished_commit", cas_write_unfinished_commit,
                       sm::description("number of transaction commit attempts that occurred on write"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_write_condition_not_met", cas_write_condition_not_met,
                       sm::description("number of transaction preconditions that did not match current values"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_write_timeout_due_to_uncertainty", cas_write_timeout_due_to_uncertainty,
                       sm::description("how many times write timeout was reported because of uncertainty in the result"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_failed_read_round_optimization", cas_failed_read_round_optimization,
                       sm::description("CAS read rounds issued only if previous value is missing on some replica"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_histogram("cas_read_contention", sm::description("how many contended reads were encountered"),
                       {storage_proxy_stats::current_scheduling_group_label()},
                       [this]{ return cas_read_contention.get_histogram(1, 8);}),

        sm::make_histogram("cas_write_contention", sm::description("how many contended writes were encountered"),
                       {storage_proxy_stats::current_scheduling_group_label()},
                       [this]{ return cas_write_contention.get_histogram(1, 8);}),

        sm::make_total_operations("cas_prune", cas_prune,
                       sm::description("how many times paxos prune was done after successful cas operation"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_dropped_prune", cas_coordinator_dropped_prune,
                       sm::description("how many times a coordinator did not perfom prune after cas"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_total_operations", cas_total_operations,
                       sm::description("number of total paxos operations executed (reads and writes)"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_gauge("cas_foreground", cas_foreground,
                        sm::description("how many paxos operations that did not yet produce a result are running"),
                        {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_gauge("cas_background", [this] { return cas_total_running - cas_foreground; },
                        sm::description("how many paxos operations are still running after a result was alredy returned"),
                        {storage_proxy_stats::current_scheduling_group_label()}),
    });

    _metrics.add_group(REPLICA_STATS_CATEGORY, {
        sm::make_total_operations("received_counter_updates", received_counter_updates,
                       sm::description("number of counter updates received by this node acting as an update leader"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("received_mutations", received_mutations,
                       sm::description("number of mutations received by a replica Node"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("forwarded_mutations", forwarded_mutations,
                       sm::description("number of mutations forwarded to other replica Nodes"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("forwarding_errors", forwarding_errors,
                       sm::description("number of errors during forwarding mutations to other replica Nodes"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("reads", replica_data_reads,
                       sm::description("number of remote data read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("data")}),

        sm::make_total_operations("reads", replica_mutation_data_reads,
                       sm::description("number of remote mutation data read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("mutation_data")}),

        sm::make_total_operations("reads", replica_digest_reads,
                       sm::description("number of remote digest read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("digest")}),

        sm::make_total_operations("cross_shard_ops", replica_cross_shard_ops,
                       sm::description("number of operations that crossed a shard boundary"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_dropped_prune", cas_replica_dropped_prune,
                       sm::description("how many times a coordinator did not perfom prune after cas"),
                       {storage_proxy_stats::current_scheduling_group_label()}),
    });
}

inline uint64_t& storage_proxy_stats::split_stats::get_ep_stat(gms::inet_address ep) noexcept {
    if (fbu::is_me(ep)) {
        return _local.val;
    }

    try {
        sstring dc = get_dc(ep);
        if (_auto_register_metrics) {
            register_metrics_for(ep);
        }
        return _dc_stats[dc].val;
    } catch (...) {
        static thread_local uint64_t dummy_stat;
        slogger.error("Failed to obtain stats ({}), fall-back to dummy", std::current_exception());
        return dummy_stat;
    }
}

void storage_proxy_stats::split_stats::register_metrics_local() {
    namespace sm = seastar::metrics;

    _metrics.add_group(_category, {
        sm::make_derive(_short_description_prefix + sstring("_local_node"), [this] { return _local.val; },
                       sm::description(_long_description_prefix + "on a local Node"), {storage_proxy_stats::current_scheduling_group_label(), op_type_label(_op_type)})
    });
}

void storage_proxy_stats::split_stats::register_metrics_for(gms::inet_address ep) {
    namespace sm = seastar::metrics;

    sstring dc = get_dc(ep);
    // if this is the first time we see an endpoint from this DC - add a
    // corresponding collectd metric
    if (auto [ignored, added] = _dc_stats.try_emplace(dc); added) {
        _metrics.add_group(_category, {
            sm::make_derive(_short_description_prefix + sstring("_remote_node"), [this, dc] { return _dc_stats[dc].val; },
                            sm::description(seastar::format("{} when communicating with external Nodes in DC {}", _long_description_prefix, dc)), {storage_proxy_stats::current_scheduling_group_label(), datacenter_label(dc), op_type_label(_op_type)})
        });
    }
}

void storage_proxy_stats::global_write_stats::register_stats() {
    namespace sm = seastar::metrics;
    _metrics.add_group(COORDINATOR_STATS_CATEGORY, {
            sm::make_current_bytes("queued_write_bytes", queued_write_bytes,
                           sm::description("number of bytes in pending write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_current_bytes("background_write_bytes", background_write_bytes,
                           sm::description("number of bytes in pending background write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),
        });
}

void storage_proxy_stats::global_stats::register_stats() {
    global_write_stats::register_stats();
}

// A helper structure for differentiating hints from mutations in overload resolution
struct hint_wrapper {
    mutation mut;
};

inline std::ostream& operator<<(std::ostream& os, const hint_wrapper& h) {
    return os << "hint_wrapper{" << h.mut << "}";
}

using namespace std::literals::chrono_literals;

storage_proxy::~storage_proxy() {}
storage_proxy::storage_proxy(distributed<database>& db, gms::gossiper& gossiper, storage_proxy::config cfg, db::view::node_update_backlog& max_view_update_backlog,
        scheduling_group_key stats_key, gms::feature_service& feat, const locator::shared_token_metadata& stm, locator::effective_replication_map_factory& erm_factory, netw::messaging_service& ms)
    : _db(db)
    , _gossiper(gossiper)
    , _shared_token_metadata(stm)
    , _erm_factory(erm_factory)
    , _read_smp_service_group(cfg.read_smp_service_group)
    , _write_smp_service_group(cfg.write_smp_service_group)
    , _hints_write_smp_service_group(cfg.hints_write_smp_service_group)
    , _write_ack_smp_service_group(cfg.write_ack_smp_service_group)
    , _next_response_id(std::chrono::system_clock::now().time_since_epoch()/1ms)
    , _hints_resource_manager(cfg.available_memory / 10, _db.local().get_config().max_hinted_handoff_concurrency)
    , _hints_manager(_db.local().get_config().hints_directory(), cfg.hinted_handoff_enabled, _db.local().get_config().max_hint_window_in_ms(), _hints_resource_manager, _db)
    , _hints_directory_initializer(std::move(cfg.hints_directory_initializer))
    , _hints_for_views_manager(_db.local().get_config().view_hints_directory(), {}, _db.local().get_config().max_hint_window_in_ms(), _hints_resource_manager, _db)
    , _stats_key(stats_key)
    , _features(feat)
    , _messaging(ms)
    , _background_write_throttle_threahsold(cfg.available_memory / 10)
    , _mutate_stage{"storage_proxy_mutate", &storage_proxy::do_mutate}
    , _connection_dropped([this] (gms::inet_address addr) { connection_dropped(std::move(addr)); })
    , _condrop_registration(_messaging.when_connection_drops(_connection_dropped))
    , _max_view_update_backlog(max_view_update_backlog)
    , _view_update_handlers_list(std::make_unique<view_update_handlers_list>()) {
    namespace sm = seastar::metrics;
    _metrics.add_group(storage_proxy_stats::COORDINATOR_STATS_CATEGORY, {
        sm::make_queue_length("current_throttled_writes", [this] { return _throttled_writes.size(); },
                       sm::description("number of currently throttled write requests")),
    });

    slogger.trace("hinted DCs: {}", cfg.hinted_handoff_enabled.to_configuration_string());
    _hints_manager.register_metrics("hints_manager");
    _hints_for_views_manager.register_metrics("hints_for_views_manager");
}

storage_proxy::unique_response_handler::unique_response_handler(storage_proxy& p_, response_id_type id_) : id(id_), p(p_) {}
storage_proxy::unique_response_handler::unique_response_handler(unique_response_handler&& x) noexcept : id(x.id), p(x.p) { x.id = 0; };

storage_proxy::unique_response_handler&
storage_proxy::unique_response_handler::operator=(unique_response_handler&& x) noexcept {
    // this->p must equal x.p
    id = std::exchange(x.id, 0);
    return *this;
}

storage_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
storage_proxy::response_id_type storage_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

void storage_proxy::connection_dropped(gms::inet_address addr) {
    slogger.debug("Drop hit rate info for {} because of disconnect", addr);
    for (auto&& cf : _db.local().get_non_system_column_families()) {
        cf->drop_hit_rate(addr);
    }
}

future<>
storage_proxy::mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout, smp_service_group smp_grp) {
    auto shard = _db.local().shard_of(m);
    get_stats().replica_cross_shard_ops += shard != this_shard_id();
    return _db.invoke_on(shard, {smp_grp, timeout},
            [s = global_schema_ptr(m.schema()),
             m = freeze(m),
             gtr = tracing::global_trace_state_ptr(std::move(tr_state)),
             timeout,
             sync] (database& db) mutable -> future<> {
        return db.apply(s, m, gtr.get(), sync, timeout);
    });
}

future<>
storage_proxy::mutate_locally(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout,
        smp_service_group smp_grp) {
    auto shard = _db.local().shard_of(m);
    get_stats().replica_cross_shard_ops += shard != this_shard_id();
    return _db.invoke_on(shard, {smp_grp, timeout},
            [&m, gs = global_schema_ptr(s), gtr = tracing::global_trace_state_ptr(std::move(tr_state)), timeout, sync] (database& db) mutable -> future<> {
        return db.apply(gs, m, gtr.get(), sync, timeout);
    });
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutations, tracing::trace_state_ptr tr_state, clock_type::time_point timeout, smp_service_group smp_grp) {
    return do_with(std::move(mutations), [this, timeout, tr_state = std::move(tr_state), smp_grp] (std::vector<mutation>& pmut) mutable {
        return parallel_for_each(pmut.begin(), pmut.end(), [this, tr_state = std::move(tr_state), timeout, smp_grp] (const mutation& m) mutable {
            return mutate_locally(m, tr_state, db::commitlog::force_sync::no, timeout, smp_grp);
        });
    });
}

future<> 
storage_proxy::mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout) {
        return mutate_locally(std::move(mutation), tr_state, timeout, _write_smp_service_group);
}
future<>
storage_proxy::mutate_hint(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, clock_type::time_point timeout) {
    auto shard = _db.local().shard_of(m);
    get_stats().replica_cross_shard_ops += shard != this_shard_id();
    return _db.invoke_on(shard, {_hints_write_smp_service_group, timeout}, [&m, gs = global_schema_ptr(s), tr_state = std::move(tr_state), timeout] (database& db) mutable -> future<> {
        return db.apply_hint(gs, m, std::move(tr_state), timeout);
    });
}

future<>
storage_proxy::mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                         tracing::trace_state_ptr trace_state, service_permit permit) {
    get_stats().received_counter_updates += mutations.size();
    return do_with(std::move(mutations), [this, cl, timeout, trace_state = std::move(trace_state), permit = std::move(permit)] (std::vector<frozen_mutation_and_schema>& update_ms) mutable {
        return parallel_for_each(update_ms, [this, cl, timeout, trace_state, permit] (frozen_mutation_and_schema& fm_a_s) {
            return mutate_counter_on_leader_and_replicate(fm_a_s.s, std::move(fm_a_s.fm), cl, timeout, trace_state, permit);
        });
    });
}

future<>
storage_proxy::mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation fm, db::consistency_level cl, clock_type::time_point timeout,
                                                      tracing::trace_state_ptr trace_state, service_permit permit) {
    auto shard = _db.local().shard_of(fm);
    bool local = shard == this_shard_id();
    get_stats().replica_cross_shard_ops += !local;
    return _db.invoke_on(shard, {_write_smp_service_group, timeout}, [gs = global_schema_ptr(s), fm = std::move(fm), cl, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state)), permit = std::move(permit), local] (database& db) {
        auto trace_state = gt.get();
        auto p = local ? std::move(permit) : /* FIXME: either obtain a real permit on this shard or hold original one across shard */ empty_service_permit();
        return db.apply_counter_update(gs, fm, timeout, trace_state).then([cl, timeout, trace_state, p = std::move(p)] (mutation m) mutable {
            return service::get_local_storage_proxy().replicate_counter_from_leader(std::move(m), cl, std::move(trace_state), timeout, std::move(p));
        });
    });
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler_helper(schema_ptr s, const dht::token& token, std::unique_ptr<mutation_holder> mh,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    auto keyspace_name = s->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);
    auto erm = ks.get_effective_replication_map();
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints_without_node_being_replaced(token);
    inet_address_vector_topology_change pending_endpoints = erm->get_token_metadata_ptr()->pending_endpoints_for(token, keyspace_name);

    slogger.trace("creating write handler for token: {} natural: {} pending: {}", token, natural_endpoints, pending_endpoints);
    tracing::trace(tr_state, "Creating write handler for token: {} natural: {} pending: {}", token, natural_endpoints ,pending_endpoints);

    // Check if this node, which is serving as a coordinator for
    // the mutation, is also a replica for the partition being
    // changed. Mutations sent by drivers unaware of token
    // distribution create a lot of network noise and thus should be
    // accounted in the metrics.
    if (std::find(natural_endpoints.begin(), natural_endpoints.end(),
                  utils::fb_utilities::get_broadcast_address()) == natural_endpoints.end()) {

        get_stats().writes_coordinator_outside_replica_set++;
    }

    // filter out natural_endpoints from pending_endpoints if the latter is not yet updated during node join
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints] (gms::inet_address& p) {
        return boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    auto all = boost::range::join(natural_endpoints, pending_endpoints);

    if (cannot_hint(all, type)) {
        get_stats().writes_failed_due_to_too_many_in_flight_hints++;
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        throw overloaded_exception(_hints_manager.size_of_hints_in_progress());
    }

    // filter live endpoints from dead ones
    inet_address_vector_replica_set live_endpoints;
    inet_address_vector_topology_change dead_endpoints;
    live_endpoints.reserve(all.size());
    dead_endpoints.reserve(all.size());
    std::partition_copy(all.begin(), all.end(), std::back_inserter(live_endpoints),
            std::back_inserter(dead_endpoints), std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &_gossiper));

    slogger.trace("creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);
    tracing::trace(tr_state, "Creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);

    db::assure_sufficient_live_nodes(cl, ks, live_endpoints, pending_endpoints);

    return create_write_response_handler(ks, cl, type, std::move(mh), std::move(live_endpoints), pending_endpoints,
            std::move(dead_endpoints), std::move(tr_state), get_stats(), std::move(permit));
}

/**
 * Helper for create_write_response_handler, shared across mutate/mutate_atomically.
 * Both methods do roughly the same thing, with the latter intermixing batch log ops
 * in the logic.
 * Since ordering is (maybe?) significant, we need to carry some info across from here
 * to the hint method below (dead nodes).
 */
storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const mutation& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    return create_write_response_handler_helper(m.schema(), m.token(), std::make_unique<shared_mutation>(m), cl, type, tr_state,
            std::move(permit));
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const hint_wrapper& h, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    return create_write_response_handler_helper(h.mut.schema(), h.mut.token(), std::make_unique<hint_mutation>(h.mut), cl, type, tr_state,
            std::move(permit));
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const std::unordered_map<gms::inet_address, std::optional<mutation>>& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    inet_address_vector_replica_set endpoints;
    endpoints.reserve(m.size());
    boost::copy(m | boost::adaptors::map_keys, std::inserter(endpoints, endpoints.begin()));
    auto mh = std::make_unique<per_destination_mutation>(m);

    slogger.trace("creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);
    tracing::trace(tr_state, "Creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);

    auto keyspace_name = mh->schema()->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);

    return create_write_response_handler(ks, cl, type, std::move(mh), std::move(endpoints), inet_address_vector_topology_change(), inet_address_vector_topology_change(), std::move(tr_state), get_stats(), std::move(permit));
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>& meta,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    auto& [commit, s, h, t] = meta;

    return create_write_response_handler_helper(s, t, std::make_unique<cas_mutation>(std::move(commit), s, std::move(h)), cl,
            db::write_type::CAS, tr_state, std::move(permit));
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, dht::token, inet_address_vector_replica_set>& meta,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    auto& [commit, s, token, endpoints] = meta;

    slogger.trace("creating write handler for paxos repair token: {} endpoint: {}", token, endpoints);
    tracing::trace(tr_state, "Creating write handler for paxos repair token: {} endpoint: {}", token, endpoints);

    auto keyspace_name = s->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);

    return create_write_response_handler(ks, cl, db::write_type::CAS, std::make_unique<cas_mutation>(std::move(commit), s, nullptr), std::move(endpoints),
                    inet_address_vector_topology_change(), inet_address_vector_topology_change(), std::move(tr_state), get_stats(), std::move(permit));
}

void storage_proxy::register_cdc_operation_result_tracker(const storage_proxy::unique_response_handler_vector& ids, lw_shared_ptr<cdc::operation_result_tracker> tracker) {
    if (!tracker) {
        return;
    }

    for (auto& id : ids) {
        auto& h = get_write_response_handler(id.id);
        if (h->get_schema()->cdc_options().enabled()) {
            h->set_cdc_operation_result_tracker(tracker);
        }
    }
}

void
storage_proxy::hint_to_dead_endpoints(response_id_type id, db::consistency_level cl) {
    auto& h = *get_write_response_handler(id);

    size_t hints = hint_to_dead_endpoints(h._mutation_holder, h.get_dead_endpoints(), h._type, h.get_trace_state());

    if (cl == db::consistency_level::ANY) {
        // for cl==ANY hints are counted towards consistency
        h.signal(hints);
    }
}

template<typename Range, typename CreateWriteHandler>
future<storage_proxy::unique_response_handler_vector> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler create_handler) {
    // apply is used to convert exceptions to exceptional future
    return futurize_invoke([this] (Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler create_handler) {
        unique_response_handler_vector ids;
        ids.reserve(std::distance(std::begin(mutations), std::end(mutations)));
        for (auto& m : mutations) {
            ids.emplace_back(*this, create_handler(m, cl, type, permit));
        }
        return make_ready_future<unique_response_handler_vector>(std::move(ids));
    }, std::forward<Range>(mutations), cl, type, std::move(permit), std::move(create_handler));
}

template<typename Range>
future<storage_proxy::unique_response_handler_vector> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit) {
    return mutate_prepare<>(std::forward<Range>(mutations), cl, type, std::move(permit), [this, tr_state = std::move(tr_state)] (const typename std::decay_t<Range>::value_type& m, db::consistency_level cl, db::write_type type, service_permit permit) mutable {
        return create_write_response_handler(m, cl, type, tr_state, std::move(permit));
    });
}

future<> storage_proxy::mutate_begin(unique_response_handler_vector ids, db::consistency_level cl,
                                     tracing::trace_state_ptr trace_state, std::optional<clock_type::time_point> timeout_opt) {
    return parallel_for_each(ids, [this, cl, timeout_opt] (unique_response_handler& protected_response) {
        auto response_id = protected_response.id;
        // This function, mutate_begin(), is called after a preemption point
        // so it's possible that other code besides our caller just ran. In
        // particular, Scylla may have noticed that a remote node went down,
        // called storage_proxy::on_down(), and removed some of the ongoing
        // handlers, including this id. If this happens, we need to ignore
        // this id - not try to look it up or start a send.
        if (!_response_handlers.contains(response_id)) {
            protected_response.release(); // Don't try to remove this id again
            // Requests that time-out normally below after response_wait()
            // result in an exception (see ~abstract_write_response_handler())
            // However, here we no longer have the handler or its information
            // to put in the exception. The exception is not needed for
            // correctness (e.g., hints are written by timeout_cb(), not
            // because of an exception here).
            slogger.debug("unstarted write cancelled for id {}", response_id);
            return make_ready_future<>();
        }
        // it is better to send first and hint afterwards to reduce latency
        // but request may complete before hint_to_dead_endpoints() is called and
        // response_id handler will be removed, so we will have to do hint with separate
        // frozen_mutation copy, or manage handler live time differently.
        hint_to_dead_endpoints(response_id, cl);

        auto timeout = timeout_opt.value_or(clock_type::now() + std::chrono::milliseconds(_db.local().get_config().write_request_timeout_in_ms()));
        // call before send_to_live_endpoints() for the same reason as above
        auto f = response_wait(response_id, timeout);
        send_to_live_endpoints(protected_response.release(), timeout); // response is now running and it will either complete or timeout
        return f;
    });
}

// this function should be called with a future that holds result of mutation attempt (usually
// future returned by mutate_begin()). The future should be ready when function is called.
future<> storage_proxy::mutate_end(future<> mutate_result, utils::latency_counter lc, write_stats& stats, tracing::trace_state_ptr trace_state) {
    assert(mutate_result.available());
    stats.write.mark(lc.stop().latency());
    if (lc.is_start()) {
        stats.estimated_write.add(lc.latency());
    }
    try {
        mutate_result.get();
        tracing::trace(trace_state, "Mutation successfully completed");
        return make_ready_future<>();
    } catch (no_such_keyspace& ex) {
        tracing::trace(trace_state, "Mutation failed: write to non existing keyspace: {}", ex.what());
        slogger.trace("Write to non existing keyspace: {}", ex.what());
        return make_exception_future<>(std::current_exception());
    } catch(mutation_write_timeout_exception& ex) {
        // timeout
        tracing::trace(trace_state, "Mutation failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
        slogger.debug("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
        stats.write_timeouts.mark();
        return make_exception_future<>(std::current_exception());
    } catch (exceptions::unavailable_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: unavailable");
        stats.write_unavailables.mark();
        slogger.trace("Unavailable");
        return make_exception_future<>(std::current_exception());
    }  catch(overloaded_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: overloaded");
        stats.write_unavailables.mark();
        slogger.trace("Overloaded");
        return make_exception_future<>(std::current_exception());
    } catch (...) {
        tracing::trace(trace_state, "Mutation failed: unknown reason");
        throw;
    }
}

gms::inet_address storage_proxy::find_leader_for_counter_update(const mutation& m, db::consistency_level cl) {
    auto& ks = _db.local().find_keyspace(m.schema()->ks_name());
    auto live_endpoints = get_live_endpoints(ks, m.token());

    if (live_endpoints.empty()) {
        throw exceptions::unavailable_exception(cl, block_for(ks, cl), 0);
    }

    const auto my_address = utils::fb_utilities::get_broadcast_address();
    // Early return if coordinator can become the leader (so one extra internode message can be
    // avoided). With token-aware drivers this is the expected case, so we are doing it ASAP.
    if (boost::algorithm::any_of_equal(live_endpoints, my_address)) {
        return my_address;
    }

    const auto local_endpoints = boost::copy_range<inet_address_vector_replica_set>(live_endpoints | boost::adaptors::filtered([&] (auto&& ep) {
        return db::is_local(ep);
    }));

    if (local_endpoints.empty()) {
        // FIXME: O(n log n) to get maximum
        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        snitch->sort_by_proximity(my_address, live_endpoints);
        return live_endpoints[0];
    } else {
        static thread_local std::default_random_engine re{std::random_device{}()};
        std::uniform_int_distribution<> dist(0, local_endpoints.size() - 1);
        return local_endpoints[dist(re)];
    }
}

template<typename Range>
future<> storage_proxy::mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, service_permit permit, clock_type::time_point timeout) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate_counters cl={}", cl);
    mlogger.trace("counter mutations={}", mutations);


    // Choose a leader for each mutation
    std::unordered_map<gms::inet_address, std::vector<frozen_mutation_and_schema>> leaders;
    for (auto& m : mutations) {
        auto leader = find_leader_for_counter_update(m, cl);
        leaders[leader].emplace_back(frozen_mutation_and_schema { freeze(m), m.schema() });
        // FIXME: check if CL can be reached
    }

    // Forward mutations to the leaders chosen for them
    auto my_address = utils::fb_utilities::get_broadcast_address();
    return parallel_for_each(leaders, [this, cl, timeout, tr_state = std::move(tr_state), permit = std::move(permit), my_address] (auto& endpoint_and_mutations) {
        auto endpoint = endpoint_and_mutations.first;

        // The leader receives a vector of mutations and processes them together,
        // so if there is a timeout we don't really know which one is to "blame"
        // and what to put in ks and cf fields of write timeout exception.
        // Let's just use the schema of the first mutation in a vector.
        auto handle_error = [this, sp = this->shared_from_this(), s = endpoint_and_mutations.second[0].s, cl, permit] (std::exception_ptr exp) {
            auto& ks = _db.local().find_keyspace(s->ks_name());
            try {
                std::rethrow_exception(std::move(exp));
            } catch (rpc::timeout_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            } catch (timed_out_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            } catch (rpc::closed_error&) {
                return make_exception_future<>(mutation_write_failure_exception(s->ks_name(), s->cf_name(), cl, 0, 1, db::block_for(ks, cl), db::write_type::COUNTER));
            }
        };

        auto f = make_ready_future<>();
        if (endpoint == my_address) {
            f = this->mutate_counters_on_leader(std::move(endpoint_and_mutations.second), cl, timeout, tr_state, permit);
        } else {
            auto& mutations = endpoint_and_mutations.second;
            auto fms = boost::copy_range<std::vector<frozen_mutation>>(mutations | boost::adaptors::transformed([] (auto& m) {
                return std::move(m.fm);
            }));

            // Coordinator is preferred as the leader - if it's not selected we can assume
            // that the query was non-token-aware and bump relevant metric.
            get_stats().writes_coordinator_outside_replica_set += fms.size();

            auto msg_addr = netw::messaging_service::msg_addr{ endpoint_and_mutations.first, 0 };
            tracing::trace(tr_state, "Enqueuing counter update to {}", msg_addr);
            f = _messaging.send_counter_mutation(msg_addr, timeout, std::move(fms), cl, tracing::make_trace_info(tr_state));
        }
        return f.handle_exception(std::move(handle_error));
    });
}

storage_proxy::paxos_participants
storage_proxy::get_paxos_participants(const sstring& ks_name, const dht::token &token, db::consistency_level cl_for_paxos) {
    keyspace& ks = _db.local().find_keyspace(ks_name);
    auto erm = ks.get_effective_replication_map();
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints_without_node_being_replaced(token);
    inet_address_vector_topology_change pending_endpoints = erm->get_token_metadata_ptr()->pending_endpoints_for(token, ks_name);

    if (cl_for_paxos == db::consistency_level::LOCAL_SERIAL) {
        auto itend = boost::range::remove_if(natural_endpoints, std::not_fn(std::cref(db::is_local)));
        natural_endpoints.erase(itend, natural_endpoints.end());
        itend = boost::range::remove_if(pending_endpoints, std::not_fn(std::cref(db::is_local)));
        pending_endpoints.erase(itend, pending_endpoints.end());
    }

    // filter out natural_endpoints from pending_endpoints if the latter is not yet updated during node join
    // should never happen, but better to be safe
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints] (gms::inet_address& p) {
        return boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    const size_t participants = pending_endpoints.size() + natural_endpoints.size();
    const size_t quorum_size = natural_endpoints.size() / 2 + 1;
    const size_t required_participants = quorum_size + pending_endpoints.size();

    inet_address_vector_replica_set live_endpoints;
    live_endpoints.reserve(participants);

    boost::copy(boost::range::join(natural_endpoints, pending_endpoints) |
            boost::adaptors::filtered(std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &_gossiper)), std::back_inserter(live_endpoints));

    if (live_endpoints.size() < required_participants) {
        throw exceptions::unavailable_exception(cl_for_paxos, required_participants, live_endpoints.size());
    }

    // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
    // Note that we fake an impossible number of required nodes in the unavailable exception
    // to nail home the point that it's an impossible operation no matter how many nodes are live.
    if (pending_endpoints.size() > 1) {
        throw exceptions::unavailable_exception(fmt::format(
                "Cannot perform LWT operation as there is more than one ({}) pending range movement", pending_endpoints.size()),
                cl_for_paxos, participants + 1, live_endpoints.size());
    }

    bool dead = participants != live_endpoints.size();

    // Apart from the ballot, paxos_state::prepare() also sends the current value of the requested key.
    // If the values received from different replicas match, we skip a separate query stage thus saving
    // one network round trip. To generate less traffic, only closest replicas send data, others send
    // digests that are used to check consistency. For this optimization to work, we need to sort the
    // list of participants by proximity to this instance.
    sort_endpoints_by_proximity(live_endpoints);

    return paxos_participants{std::move(live_endpoints), required_participants, dead};
}


/**
 * Use this method to have these Mutations applied
 * across all replicas. This method will take care
 * of the possibility of a replica being down and hint
 * the data across to some other replica.
 *
 * @param mutations the mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 * @param tr_state trace state handle
 */
future<> storage_proxy::mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters) {
    if (_cdc && _cdc->needs_cdc_augmentation(mutations)) {
        return _cdc->augment_mutation_call(timeout, std::move(mutations), tr_state, cl).then([this, cl, timeout, tr_state, permit = std::move(permit), raw_counters, cdc = _cdc->shared_from_this()](std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>&& t) mutable {
            auto mutations = std::move(std::get<0>(t));
            auto tracker = std::move(std::get<1>(t));
            return _mutate_stage(this, std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), raw_counters, std::move(tracker));
        });
    }
    return _mutate_stage(this, std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), raw_counters, nullptr);
}

future<> storage_proxy::do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker) {
    auto mid = raw_counters ? mutations.begin() : boost::range::partition(mutations, [] (auto&& m) {
        return m.schema()->is_counter();
    });
    return seastar::when_all_succeed(
        mutate_counters(boost::make_iterator_range(mutations.begin(), mid), cl, tr_state, permit, timeout),
        mutate_internal(boost::make_iterator_range(mid, mutations.end()), cl, false, tr_state, permit, timeout, std::move(cdc_tracker))
    ).discard_result();
}

future<> storage_proxy::replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                                      clock_type::time_point timeout, service_permit permit) {
    // FIXME: do not send the mutation to itself, it has already been applied (it is not incorrect to do so, though)
    return mutate_internal(std::array<mutation, 1>{std::move(m)}, cl, true, std::move(tr_state), std::move(permit), timeout);
}

/*
 * Range template parameter can either be range of 'mutation' or a range of 'std::unordered_map<gms::inet_address, mutation>'.
 * create_write_response_handler() has specialization for both types. The one for the former uses keyspace to figure out
 * endpoints to send mutation to, the one for the late uses enpoints that are used as keys for the map.
 */
template<typename Range>
future<>
storage_proxy::mutate_internal(Range mutations, db::consistency_level cl, bool counters, tracing::trace_state_ptr tr_state, service_permit permit,
                               std::optional<clock_type::time_point> timeout_opt, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate cl={}", cl);
    mlogger.trace("mutations={}", mutations);

    // If counters is set it means that we are replicating counter shards. There
    // is no need for special handling anymore, since the leader has already
    // done its job, but we need to return correct db::write_type in case of
    // a timeout so that client doesn't attempt to retry the request.
    auto type = counters ? db::write_type::COUNTER
                         : (std::next(std::begin(mutations)) == std::end(mutations) ? db::write_type::SIMPLE : db::write_type::UNLOGGED_BATCH);
    utils::latency_counter lc;
    lc.start();

    return mutate_prepare(mutations, cl, type, tr_state, std::move(permit)).then([this, cl, timeout_opt, tracker = std::move(cdc_tracker),
            tr_state] (storage_proxy::unique_response_handler_vector ids) mutable {
        register_cdc_operation_result_tracker(ids, tracker);
        return mutate_begin(std::move(ids), cl, tr_state, timeout_opt);
    }).then_wrapped([this, p = shared_from_this(), lc, tr_state] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, get_stats(), std::move(tr_state));
    });
}

future<>
storage_proxy::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
    clock_type::time_point timeout,
    bool should_mutate_atomically, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters) {
    warn(unimplemented::cause::TRIGGERS);
    if (should_mutate_atomically) {
        assert(!raw_counters);
        return mutate_atomically(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit));
    }
    return mutate(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), raw_counters);
}

/**
 * See mutate. Adds additional steps before and after writing a batch.
 * Before writing the batch (but after doing availability check against the FD for the row replicas):
 *      write the entire batch to a batchlog elsewhere in the cluster.
 * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
 *
 * @param mutations the Mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 */
future<>
storage_proxy::mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit) {

    utils::latency_counter lc;
    lc.start();

    class context {
        storage_proxy& _p;
        const locator::token_metadata_ptr _tmptr;
        std::vector<mutation> _mutations;
        lw_shared_ptr<cdc::operation_result_tracker> _cdc_tracker;
        db::consistency_level _cl;
        clock_type::time_point _timeout;
        tracing::trace_state_ptr _trace_state;
        storage_proxy::stats& _stats;
        service_permit _permit;

        const utils::UUID _batch_uuid;
        const inet_address_vector_replica_set _batchlog_endpoints;

    public:
        context(storage_proxy & p, std::vector<mutation>&& mutations, lw_shared_ptr<cdc::operation_result_tracker>&& cdc_tracker, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit)
                : _p(p)
                , _tmptr(p.get_token_metadata_ptr())
                , _mutations(std::move(mutations))
                , _cdc_tracker(std::move(cdc_tracker))
                , _cl(cl)
                , _timeout(timeout)
                , _trace_state(std::move(tr_state))
                , _stats(p.get_stats())
                , _permit(std::move(permit))
                , _batch_uuid(utils::UUID_gen::get_time_UUID())
                , _batchlog_endpoints(
                        [this]() -> inet_address_vector_replica_set {
                            auto local_addr = utils::fb_utilities::get_broadcast_address();
                            auto& topology = _tmptr->get_topology();
                            auto& local_endpoints = topology.get_datacenter_racks().at(get_local_dc());
                            auto local_rack = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(local_addr);
                            auto chosen_endpoints = _p.gossiper().endpoint_filter(local_rack, local_endpoints);

                            if (chosen_endpoints.empty()) {
                                if (_cl == db::consistency_level::ANY) {
                                    return {local_addr};
                                }
                                throw exceptions::unavailable_exception(db::consistency_level::ONE, 1, 0);
                            }
                            return chosen_endpoints;
                        }()) {
                tracing::trace(_trace_state, "Created a batch context");
                tracing::set_batchlog_endpoints(_trace_state, _batchlog_endpoints);
        }

        future<> send_batchlog_mutation(mutation m, db::consistency_level cl = db::consistency_level::ONE) {
            return _p.mutate_prepare<>(std::array<mutation, 1>{std::move(m)}, cl, db::write_type::BATCH_LOG, _permit, [this] (const mutation& m, db::consistency_level cl, db::write_type type, service_permit permit) {
                auto& ks = _p._db.local().find_keyspace(m.schema()->ks_name());
                return _p.create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), _batchlog_endpoints, {}, {}, _trace_state, _stats, std::move(permit));
            }).then([this, cl] (unique_response_handler_vector ids) {
                _p.register_cdc_operation_result_tracker(ids, _cdc_tracker);
                return _p.mutate_begin(std::move(ids), cl, _trace_state, _timeout);
            });
        }
        future<> sync_write_to_batchlog() {
            auto m = _p.get_batchlog_mutation_for(_mutations, _batch_uuid, netw::messaging_service::current_version, db_clock::now());
            tracing::trace(_trace_state, "Sending a batchlog write mutation");
            return send_batchlog_mutation(std::move(m));
        };
        future<> async_remove_from_batchlog() {
            // delete batch
            auto schema = _p._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
            auto key = partition_key::from_exploded(*schema, {uuid_type->decompose(_batch_uuid)});
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            mutation m(schema, key);
            m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));

            tracing::trace(_trace_state, "Sending a batchlog remove mutation");
            return send_batchlog_mutation(std::move(m), db::consistency_level::ANY).handle_exception([] (std::exception_ptr eptr) {
                slogger.error("Failed to remove mutations from batchlog: {}", eptr);
            });
        };

        future<> run() {
            return _p.mutate_prepare(_mutations, _cl, db::write_type::BATCH, _trace_state, _permit).then([this] (unique_response_handler_vector ids) {
                return sync_write_to_batchlog().then([this, ids = std::move(ids)] () mutable {
                    tracing::trace(_trace_state, "Sending batch mutations");
                    _p.register_cdc_operation_result_tracker(ids, _cdc_tracker);
                    return _p.mutate_begin(std::move(ids), _cl, _trace_state, _timeout);
                }).then(std::bind(&context::async_remove_from_batchlog, this));
            });
        }
    };

    auto mk_ctxt = [this, tr_state, timeout, permit = std::move(permit), cl] (std::vector<mutation> mutations, lw_shared_ptr<cdc::operation_result_tracker> tracker) mutable {
      try {
          return make_ready_future<lw_shared_ptr<context>>(make_lw_shared<context>(*this, std::move(mutations), std::move(tracker), cl, timeout, std::move(tr_state), std::move(permit)));
      } catch(...) {
          return make_exception_future<lw_shared_ptr<context>>(std::current_exception());
      }
    };
    auto cleanup = [p = shared_from_this(), lc, tr_state] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, p->get_stats(), std::move(tr_state));
    };

    if (_cdc && _cdc->needs_cdc_augmentation(mutations)) {
        return _cdc->augment_mutation_call(timeout, std::move(mutations), std::move(tr_state), cl).then([this, mk_ctxt = std::move(mk_ctxt), cleanup = std::move(cleanup), cdc = _cdc->shared_from_this()](std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>&& t) mutable {
            auto mutations = std::move(std::get<0>(t));
            auto tracker = std::move(std::get<1>(t));
            return std::move(mk_ctxt)(std::move(mutations), std::move(tracker)).then([this] (lw_shared_ptr<context> ctxt) {
                return ctxt->run().finally([ctxt]{});
            }).then_wrapped(std::move(cleanup));
        });
    }

    return mk_ctxt(std::move(mutations), nullptr).then([this] (lw_shared_ptr<context> ctxt) {
        return ctxt->run().finally([ctxt]{});
    }).then_wrapped(std::move(cleanup));
}

mutation storage_proxy::get_batchlog_mutation_for(const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now) {
    auto schema = local_db().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
    auto key = partition_key::from_singular(*schema, id);
    auto timestamp = api::new_timestamp();
    auto data = [this, &mutations] {
        std::vector<canonical_mutation> fm(mutations.begin(), mutations.end());
        bytes_ostream out;
        for (auto& m : fm) {
            ser::serialize(out, m);
        }
        return to_bytes(out.linearize());
    }();

    mutation m(schema, key);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("version"), version, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("written_at"), now, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("data"), data_value(std::move(data)), timestamp);

    return m;
}

template<typename Range>
bool storage_proxy::cannot_hint(const Range& targets, db::write_type type) const {
    // if hints are disabled we "can always hint" since there's going to be no hint generated in this case
    return hints_enabled(type) && boost::algorithm::any_of(targets, std::bind(&db::hints::manager::too_many_in_flight_hints_for, &_hints_manager, std::placeholders::_1));
}

future<> storage_proxy::send_to_endpoint(
        std::unique_ptr<mutation_holder> m,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        write_stats& stats,
        allow_hints allow_hints) {
    utils::latency_counter lc;
    lc.start();

    std::optional<clock_type::time_point> timeout;
    db::consistency_level cl = allow_hints ? db::consistency_level::ANY : db::consistency_level::ONE;
    if (type == db::write_type::VIEW) {
        // View updates have a near-infinite timeout to avoid incurring the extra work of writting hints
        // and to apply backpressure.
        timeout = clock_type::now() + 5min;
    }
    return mutate_prepare(std::array{std::move(m)}, cl, type, /* does view building should hold a real permit */ empty_service_permit(),
            [this, tr_state, target = std::array{target}, pending_endpoints = std::move(pending_endpoints), &stats] (
                std::unique_ptr<mutation_holder>& m,
                db::consistency_level cl,
                db::write_type type, service_permit permit) mutable {
        inet_address_vector_replica_set targets;
        targets.reserve(pending_endpoints.size() + 1);
        inet_address_vector_topology_change dead_endpoints;
        boost::algorithm::partition_copy(
                boost::range::join(pending_endpoints, target),
                std::inserter(targets, targets.begin()),
                std::back_inserter(dead_endpoints),
                [this] (gms::inet_address ep) { return _gossiper.is_alive(ep); });
        auto& ks = _db.local().find_keyspace(m->schema()->ks_name());
        slogger.trace("Creating write handler with live: {}; dead: {}", targets, dead_endpoints);
        db::assure_sufficient_live_nodes(cl, ks, targets, pending_endpoints);
        return create_write_response_handler(
            ks,
            cl,
            type,
            std::move(m),
            std::move(targets),
            pending_endpoints,
            std::move(dead_endpoints),
            tr_state,
            stats,
            std::move(permit));
    }).then([this, cl, tr_state = std::move(tr_state), timeout = std::move(timeout)] (unique_response_handler_vector ids) mutable {
        return mutate_begin(std::move(ids), cl, std::move(tr_state), std::move(timeout));
    }).then_wrapped([p = shared_from_this(), lc, &stats] (future<>&& f) {
        return p->mutate_end(std::move(f), lc, stats, nullptr);
    });
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        allow_hints allow_hints) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(target),
            std::move(pending_endpoints),
            type,
            std::move(tr_state),
            get_stats(),
            allow_hints);
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        write_stats& stats,
        allow_hints allow_hints) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(target),
            std::move(pending_endpoints),
            type,
            std::move(tr_state),
            stats,
            allow_hints);
}

future<> storage_proxy::send_hint_to_endpoint(frozen_mutation_and_schema fm_a_s, gms::inet_address target) {
    if (!_features.cluster_supports_hinted_handoff_separate_connection()) {
        return send_to_endpoint(
                std::make_unique<shared_mutation>(std::move(fm_a_s)),
                std::move(target),
                { },
                db::write_type::SIMPLE,
                tracing::trace_state_ptr(),
                get_stats(),
                allow_hints::no);
    }

    return send_to_endpoint(
            std::make_unique<hint_mutation>(std::move(fm_a_s)),
            std::move(target),
            { },
            db::write_type::SIMPLE,
            tracing::trace_state_ptr(),
            get_stats(),
            allow_hints::no);
}

future<> storage_proxy::send_hint_to_all_replicas(frozen_mutation_and_schema fm_a_s) {
    if (!_features.cluster_supports_hinted_handoff_separate_connection()) {
        std::array<mutation, 1> ms{fm_a_s.fm.unfreeze(fm_a_s.s)};
        return mutate_internal(std::move(ms), db::consistency_level::ALL, false, nullptr, empty_service_permit());
    }

    std::array<hint_wrapper, 1> ms{hint_wrapper { fm_a_s.fm.unfreeze(fm_a_s.s) }};
    return mutate_internal(std::move(ms), db::consistency_level::ALL, false, nullptr, empty_service_permit());
}

/**
 * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
 * is not available.
 *
 * Note about hints:
 *
 * | Hinted Handoff | Consist. Level |
 * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
 * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
 * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 *
 * @throws OverloadedException if the hints cannot be written/enqueued
 */
 // returned future is ready when sent is complete, not when mutation is executed on all (or any) targets!
void storage_proxy::send_to_live_endpoints(storage_proxy::response_id_type response_id, clock_type::time_point timeout)
{
    // extra-datacenter replicas, grouped by dc
    std::unordered_map<sstring, inet_address_vector_replica_set> dc_groups;
    std::vector<std::pair<const sstring, inet_address_vector_replica_set>> local;
    local.reserve(3);

    auto handler_ptr = get_write_response_handler(response_id);
    auto& stats = handler_ptr->stats();
    auto& handler = *handler_ptr;
    auto& global_stats = handler._proxy->_global_stats;

    for(auto dest: handler.get_targets()) {
        sstring dc = get_dc(dest);
        // read repair writes do not go through coordinator since mutations are per destination
        if (handler.read_repair_write() || dc == get_local_dc()) {
            local.emplace_back("", inet_address_vector_replica_set({dest}));
        } else {
            dc_groups[dc].push_back(dest);
        }
    }

    auto all = boost::range::join(local, dc_groups);
    auto my_address = utils::fb_utilities::get_broadcast_address();

    // lambda for applying mutation locally
    auto lmutate = [handler_ptr, response_id, this, my_address, timeout] () mutable {
        return handler_ptr->apply_locally(timeout, handler_ptr->get_trace_state())
                .then([response_id, this, my_address, h = std::move(handler_ptr), p = shared_from_this()] {
            // make mutation alive until it is processed locally, otherwise it
            // may disappear if write timeouts before this future is ready
            got_response(response_id, my_address, get_view_update_backlog());
        });
    };

    // lambda for applying mutation remotely
    auto rmutate = [this, handler_ptr, timeout, response_id, my_address, &global_stats] (gms::inet_address coordinator, inet_address_vector_replica_set&& forward) {
        auto msize = handler_ptr->get_mutation_size(); // can overestimate for repair writes
        global_stats.queued_write_bytes += msize;

        return handler_ptr->apply_remotely(coordinator, std::move(forward), response_id, timeout, handler_ptr->get_trace_state())
                .finally([this, p = shared_from_this(), h = std::move(handler_ptr), msize, &global_stats] {
            global_stats.queued_write_bytes -= msize;
            unthrottle();
        });
    };

    // OK, now send and/or apply locally
    for (typename decltype(dc_groups)::value_type& dc_targets : all) {
        auto& forward = dc_targets.second;
        // last one in forward list is a coordinator
        auto coordinator = forward.back();
        forward.pop_back();

        size_t forward_size = forward.size();
        future<> f = make_ready_future<>();


        if (handler.is_counter() && coordinator == my_address) {
            got_response(response_id, coordinator, std::nullopt);
        } else {
            if (!handler.read_repair_write()) {
                ++stats.writes_attempts.get_ep_stat(coordinator);
            } else {
                ++stats.read_repair_write_attempts.get_ep_stat(coordinator);
            }

            if (coordinator == my_address) {
                f = futurize_invoke(lmutate);
            } else {
                f = futurize_invoke(rmutate, coordinator, std::move(forward));
            }
        }

        // Waited on indirectly.
        (void)f.handle_exception([response_id, forward_size, coordinator, handler_ptr, p = shared_from_this(), &stats] (std::exception_ptr eptr) {
            ++stats.writes_errors.get_ep_stat(coordinator);
            error err = error::FAILURE;
            std::optional<sstring> msg;
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(timed_out_error&) {
                // from lmutate(). Ignore so that logs are not flooded
                // database total_writes_timedout counter was incremented.
                // It needs to be recorded that the timeout occurred locally though.
                err = error::TIMEOUT;
            } catch(db::virtual_table_update_exception& e) {
                msg = e.grab_cause();
            } catch(...) {
                slogger.error("exception during mutation write to {}: {}", coordinator, std::current_exception());
            }
            p->got_failure_response(response_id, coordinator, forward_size + 1, std::nullopt, err, std::move(msg));
        });
    }
}

// returns number of hints stored
template<typename Range>
size_t storage_proxy::hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets, db::write_type type, tracing::trace_state_ptr tr_state) noexcept
{
    if (hints_enabled(type)) {
        db::hints::manager& hints_manager = hints_manager_for(type);
        return boost::count_if(targets, [this, &mh, tr_state = std::move(tr_state), &hints_manager] (gms::inet_address target) mutable -> bool {
            return mh->store_hint(hints_manager, target, tr_state);
        });
    } else {
        return 0;
    }
}

future<> storage_proxy::schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state,
                                        service_permit permit) {
    if (diffs.empty()) {
        return make_ready_future<>();
    }
    return mutate_internal(diffs | boost::adaptors::map_values, cl, false, std::move(trace_state), std::move(permit));
}

class abstract_read_resolver {
protected:
    db::consistency_level _cl;
    size_t _targets_count;
    promise<> _done_promise; // all target responded
    bool _request_failed = false; // will be true if request fails or timeouts
    timer<storage_proxy::clock_type> _timeout;
    schema_ptr _schema;
    size_t _failed = 0;

    virtual void on_failure(std::exception_ptr ex) = 0;
    virtual void on_timeout() = 0;
    virtual size_t response_count() const = 0;
    virtual void fail_request(std::exception_ptr ex) {
        _request_failed = true;
        _done_promise.set_exception(ex);
        _timeout.cancel();
        on_failure(ex);
    }
public:
    abstract_read_resolver(schema_ptr schema, db::consistency_level cl, size_t target_count, storage_proxy::clock_type::time_point timeout)
        : _cl(cl)
        , _targets_count(target_count)
        , _schema(std::move(schema))
    {
        _timeout.set_callback([this] {
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    virtual void on_error(gms::inet_address ep, bool disconnect) = 0;
    future<> done() {
        return _done_promise.get_future();
    }
    void error(gms::inet_address ep, std::exception_ptr eptr) {
        sstring why;
        bool disconnect = false;
        try {
            std::rethrow_exception(eptr);
        } catch (rpc::closed_error&) {
            // do not report connection closed exception, gossiper does that
            disconnect = true;
        } catch (rpc::timeout_error&) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } catch (semaphore_timed_out&) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } catch (timed_out_error&) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } catch(...) {
            slogger.error("Exception when communicating with {}, to read from {}.{}: {}", ep, _schema->ks_name(), _schema->cf_name(), eptr);
        }

        if (!_request_failed) { // request may fail only once.
            on_error(ep, disconnect);
        }
    }
};

struct digest_read_result {
    foreign_ptr<lw_shared_ptr<query::result>> result;
    bool digests_match;
};

class digest_read_resolver : public abstract_read_resolver {
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<digest_read_result> _cl_promise; // cl is reached
    bool _cl_reported = false;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    utils::small_vector<query::result_digest, 3> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;
    size_t _target_count_for_cl; // _target_count_for_cl < _targets_count if CL=LOCAL and RRD.GLOBAL

    void on_timeout() override {
        fail_request(std::make_exception_ptr(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _block_for, _data_result)));
    }
    void on_failure(std::exception_ptr ex) override {
        if (!_cl_reported) {
            _cl_promise.set_exception(ex);
        }
        // we will not need them any more
        _data_result = foreign_ptr<lw_shared_ptr<query::result>>();
        _digest_results.clear();
    }
    virtual size_t response_count() const override {
        return _digest_results.size();
    }
public:
    digest_read_resolver(schema_ptr schema, db::consistency_level cl, size_t block_for, size_t target_count_for_cl, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, 0, timeout),
                                _block_for(block_for),  _target_count_for_cl(target_count_for_cl) {}
    void add_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<query::result>> result) {
        if (!_request_failed) {
            // if only one target was queried digest_check() will be skipped so we can also skip digest calculation
            _digest_results.emplace_back(_targets_count == 1 ? query::result_digest() : *result->digest());
            _last_modified = std::max(_last_modified, result->last_modified());
            if (!_data_result) {
                _data_result = std::move(result);
            }
            got_response(from);
        }
    }
    void add_digest(gms::inet_address from, query::result_digest digest, api::timestamp_type last_modified) {
        if (!_request_failed) {
            _digest_results.emplace_back(std::move(digest));
            _last_modified = std::max(_last_modified, last_modified);
            got_response(from);
        }
    }
    bool digests_match() const {
        assert(response_count());
        if (response_count() == 1) {
            return true;
        }
        auto& first = *_digest_results.begin();
        return std::find_if(_digest_results.begin() + 1, _digest_results.end(), [&first] (query::result_digest digest) { return digest != first; }) == _digest_results.end();
    }
    bool waiting_for(gms::inet_address ep) {
        return db::is_datacenter_local(_cl) ? fbu::is_me(ep) || db::is_local(ep) : true;
    }
    void got_response(gms::inet_address ep) {
        if (!_cl_reported) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses >= _block_for && _data_result) {
                _cl_reported = true;
                _cl_promise.set_value(digest_read_result{std::move(_data_result), digests_match()});
            }
        }
        if (is_completed()) {
            _timeout.cancel();
            _done_promise.set_value();
        }
    }
    void on_error(gms::inet_address ep, bool disconnect) override {
        if (waiting_for(ep)) {
            _failed++;
        }
        if (disconnect && _block_for == _target_count_for_cl) {
            // if the error is because of a connection disconnect and there is no targets to speculate
            // wait for timeout in hope that the client will issue speculative read
            // FIXME: resolver should have access to all replicas and try another one in this case
            return;
        }
        if (_block_for + _failed > _target_count_for_cl) {
            fail_request(std::make_exception_ptr(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _failed, _block_for, _data_result)));
        }
    }
    future<digest_read_result> has_cl() {
        return _cl_promise.get_future();
    }
    bool has_data() {
        return _data_result;
    }
    void add_wait_targets(size_t targets_count) {
        _targets_count += targets_count;
    }
    bool is_completed() {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }
};

class data_read_resolver : public abstract_read_resolver {
    struct reply {
        gms::inet_address from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        bool reached_end = false;
        reply(gms::inet_address from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        gms::inet_address from;
        std::optional<partition> par;
        bool reached_end;
        bool reached_partition_end;
        version(gms::inet_address from_, std::optional<partition> par_, bool reached_end, bool reached_partition_end)
                : from(std::move(from_)), par(std::move(par_)), reached_end(reached_end), reached_partition_end(reached_partition_end) {}
    };
    struct mutation_and_live_row_count {
        mutation mut;
        uint64_t live_row_count;
    };

    struct primary_key {
        dht::decorated_key partition;
        std::optional<clustering_key> clustering;

        class less_compare_clustering {
            bool _is_reversed;
            clustering_key::less_compare _ck_cmp;
        public:
            less_compare_clustering(const schema& s, bool is_reversed)
                : _is_reversed(is_reversed), _ck_cmp(s) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                if (!b.clustering) {
                    return false;
                }
                if (!a.clustering) {
                    return true;
                }
                if (_is_reversed) {
                    return _ck_cmp(*b.clustering, *a.clustering);
                } else {
                    return _ck_cmp(*a.clustering, *b.clustering);
                }
            }
        };

        class less_compare {
            const schema& _schema;
            less_compare_clustering _ck_cmp;
        public:
            less_compare(const schema& s, bool is_reversed)
                : _schema(s), _ck_cmp(s, is_reversed) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                auto pk_result = a.partition.tri_compare(_schema, b.partition);
                if (pk_result != 0) {
                    return pk_result < 0;
                }
                return _ck_cmp(a, b);
            }
        };
    };

    uint64_t _total_live_count = 0;
    uint64_t _max_live_count = 0;
    uint32_t _short_read_diff = 0;
    uint64_t _max_per_partition_live_count = 0;
    uint32_t _partition_count = 0;
    uint32_t _live_partition_count = 0;
    bool _increase_per_partition_limit = false;
    bool _all_reached_end = true;
    query::short_read _is_short_read;
    std::vector<reply> _data_results;
    std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> _diffs;
private:
    void on_timeout() override {
        fail_request(std::make_exception_ptr(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), _targets_count, response_count() != 0)));
    }
    void on_failure(std::exception_ptr ex) override {
        // we will not need them any more
        _data_results.clear();
    }

    virtual size_t response_count() const override {
        return _data_results.size();
    }

    void register_live_count(const std::vector<version>& replica_versions, uint64_t reconciled_live_rows, uint64_t limit) {
        bool any_not_at_end = boost::algorithm::any_of(replica_versions, [] (const version& v) {
            return !v.reached_partition_end;
        });
        if (any_not_at_end && reconciled_live_rows < limit && limit - reconciled_live_rows > _short_read_diff) {
            _short_read_diff = limit - reconciled_live_rows;
            _max_per_partition_live_count = reconciled_live_rows;
        }
    }
    void find_short_partitions(const std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions,
                               uint64_t per_partition_limit, uint64_t row_limit, uint32_t partition_limit) {
        // Go through the partitions that weren't limited by the total row limit
        // and check whether we got enough rows to satisfy per-partition row
        // limit.
        auto partitions_left = partition_limit;
        auto rows_left = row_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                register_live_count(*pv, row_count, per_partition_limit);
            } else {
                break;
            }
            ++pv;
        }
    }

    static primary_key get_last_row(const schema& s, const partition& p, bool is_reversed) {
        return {p.mut().decorated_key(s), is_reversed ? p.mut().partition().first_row_key() : p.mut().partition().last_row_key()  };
    }

    // Returns the highest row sent by the specified replica, according to the schema and the direction of
    // the query.
    // versions is a table where rows are partitions in descending order and the columns identify the partition
    // sent by a particular replica.
    static primary_key get_last_row(const schema& s, bool is_reversed, const std::vector<std::vector<version>>& versions, uint32_t replica) {
        const partition* last_partition = nullptr;
        // Versions are in the reversed order.
        for (auto&& pv : versions) {
            const std::optional<partition>& p = pv[replica].par;
            if (p) {
                last_partition = &p.value();
                break;
            }
        }
        assert(last_partition);
        return get_last_row(s, *last_partition, is_reversed);
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, const query::read_command& cmd, uint64_t limit, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        auto mp = mutation_partition(s, m.partition());
        auto&& ranges = cmd.slice.row_ranges(s, m.key());
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        mp.compact_for_query(s, cmd.timestamp, ranges, always_return_static_content, is_reversed, limit);
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, mp, is_reversed)};
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, m.partition(), is_reversed)};
    }

    static std::optional<clustering_key> get_last_reconciled_row(const schema& s, const mutation_partition& mp, bool is_reversed) {
        std::optional<clustering_key> ck;
        if (!mp.clustered_rows().empty()) {
            if (is_reversed) {
                ck = mp.clustered_rows().begin()->key();
            } else {
                ck = mp.clustered_rows().rbegin()->key();
            }
        }
        return ck;
    }

    static bool got_incomplete_information_in_partition(const schema& s, const primary_key& last_reconciled_row, const std::vector<version>& versions, bool is_reversed) {
        primary_key::less_compare_clustering ck_cmp(s, is_reversed);
        for (auto&& v : versions) {
            if (!v.par || v.reached_partition_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, *v.par, is_reversed);
            if (ck_cmp(replica_last_row, last_reconciled_row)) {
                return true;
            }
        }
        return false;
    }

    bool got_incomplete_information_across_partitions(const schema& s, const query::read_command& cmd,
                                                      const primary_key& last_reconciled_row, std::vector<mutation_and_live_row_count>& rp,
                                                      const std::vector<std::vector<version>>& versions, bool is_reversed) {
        bool short_reads_allowed = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        primary_key::less_compare cmp(s, is_reversed);
        std::optional<primary_key> shortest_read;
        auto num_replicas = versions[0].size();
        for (uint32_t i = 0; i < num_replicas; ++i) {
            if (versions.front()[i].reached_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, is_reversed, versions, i);
            if (cmp(replica_last_row, last_reconciled_row)) {
                if (short_reads_allowed) {
                    if (!shortest_read || cmp(replica_last_row, *shortest_read)) {
                        shortest_read = std::move(replica_last_row);
                    }
                } else {
                    return true;
                }
            }
        }

        // Short reads are allowed, trim the reconciled result.
        if (shortest_read) {
            _is_short_read = query::short_read::yes;

            // Prepare to remove all partitions past shortest_read
            auto it = rp.begin();
            for (; it != rp.end() && shortest_read->partition.less_compare(s, it->mut.decorated_key()); ++it) { }

            // Remove all clustering rows past shortest_read
            if (it != rp.end() && it->mut.decorated_key().equal(s, shortest_read->partition)) {
                if (!shortest_read->clustering) {
                    ++it;
                } else {
                    std::vector<query::clustering_range> ranges;
                    ranges.emplace_back(is_reversed ? query::clustering_range::make_starting_with(std::move(*shortest_read->clustering))
                                                    : query::clustering_range::make_ending_with(std::move(*shortest_read->clustering)));
                    it->live_row_count = it->mut.partition().compact_for_query(s, cmd.timestamp, ranges, always_return_static_content,
                            is_reversed, query::partition_max_rows);
                }
            }

            // Actually remove all partitions past shortest_read
            rp.erase(rp.begin(), it);

            // Update total live count and live partition count
            _live_partition_count = 0;
            _total_live_count = boost::accumulate(rp, uint64_t(0), [this] (uint64_t lc, const mutation_and_live_row_count& m_a_rc) {
                _live_partition_count += !!m_a_rc.live_row_count;
                return lc + m_a_rc.live_row_count;
            });
        }

        return false;
    }

    bool got_incomplete_information(const schema& s, const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
                            uint64_t original_partition_limit, std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions) {
        // We need to check whether the reconciled result contains all information from all available
        // replicas. It is possible that some of the nodes have returned less rows (because the limit
        // was set and they had some tombstones missing) than the others. In such cases we cannot just
        // merge all results and return that to the client as the replicas that returned less row
        // may have newer data for the rows they did not send than any other node in the cluster.
        //
        // This function is responsible for detecting whether such problem may happen. We get partition
        // and clustering keys of the last row that is going to be returned to the client and check if
        // it is in range of rows returned by each replicas that returned as many rows as they were
        // asked for (if a replica returned less rows it means it returned everything it has).
        auto is_reversed = cmd.slice.options.contains(query::partition_slice::option::reversed);

        auto rows_left = original_row_limit;
        auto partitions_left = original_partition_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left > !!row_count) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                if (original_per_partition_limit < query:: max_rows_if_set) {
                    auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, original_per_partition_limit, is_reversed);
                    if (got_incomplete_information_in_partition(s, last_row, *pv, is_reversed)) {
                        _increase_per_partition_limit = true;
                        return true;
                    }
                }
            } else {
                auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, rows_left, is_reversed);
                return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
            }
            ++pv;
        }
        if (rp.empty()) {
            return false;
        }
        auto&& last_row = get_last_reconciled_row(s, *rp.begin(), is_reversed);
        return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
    }
public:
    data_read_resolver(schema_ptr schema, db::consistency_level cl, size_t targets_count, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, targets_count, timeout) {
        _data_results.reserve(targets_count);
    }
    void add_mutate_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (!_request_failed) {
            _max_live_count = std::max(result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _timeout.cancel();
                _done_promise.set_value();
            }
        }
    }
    void on_error(gms::inet_address ep, bool disconnect) override {
        fail_request(std::make_exception_ptr(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), 1, _targets_count, response_count() != 0)));
    }
    uint32_t max_live_count() const {
        return _max_live_count;
    }
    bool any_partition_short_read() const {
        return _short_read_diff > 0;
    }
    bool increase_per_partition_limit() const {
        return _increase_per_partition_limit;
    }
    uint32_t max_per_partition_live_count() const {
        return _max_per_partition_live_count;
    }
    uint32_t partition_count() const {
        return _partition_count;
    }
    uint32_t live_partition_count() const {
        return _live_partition_count;
    }
    bool all_reached_end() const {
        return _all_reached_end;
    }
    std::optional<reconcilable_result> resolve(schema_ptr schema, const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
            uint32_t original_partition_limit) {
        assert(_data_results.size());

        if (_data_results.size() == 1) {
            // if there is a result only from one node there is nothing to reconcile
            // should happen only for range reads since single key reads will not
            // try to reconcile for CL=ONE
            auto& p = _data_results[0].result;
            return reconcilable_result(p->row_count(), p->partitions(), p->is_short_read());
        }

        const auto& s = *schema;

        // return true if lh > rh
        auto cmp = [&s](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key();
                auto rhk = rh.result->partitions().back().mut().key();
                return lhk.ring_order_tri_compare(s, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        std::vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        for (auto& r : _data_results) {
            _is_short_read = _is_short_read || r.result->is_short_read();
            r.reached_end = !r.result->is_short_read() && r.result->row_count() < cmd.get_row_limit()
                            && (cmd.partition_limit == query::max_partitions
                                || boost::range::count_if(r.result->partitions(), [] (const partition& p) {
                                    return p.row_count();
                                }) < cmd.partition_limit);
            _all_reached_end = _all_reached_end && r.reached_end;
        }

        do {
            // after this sort reply with largest key is at the beginning
            boost::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key();
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_targets_count);
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key().legacy_equal(s, max_key)) {
                    bool reached_partition_end = pit->row_count() < cmd.slice.partition_row_limit();
                    v.emplace_back(r.from, std::move(*pit), r.reached_end, reached_partition_end);
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, std::optional<partition>(), r.reached_end, true);
                }
            }

            boost::sort(v, [] (const version& x, const version& y) {
                return x.from < y.from;
            });
        } while(true);

        std::vector<mutation_and_live_row_count> reconciled_partitions;
        reconciled_partitions.reserve(versions.size());

        // reconcile all versions
        boost::range::transform(boost::make_iterator_range(versions.begin(), versions.end()), std::back_inserter(reconciled_partitions),
                                [this, schema, original_per_partition_limit] (std::vector<version>& v) {
            auto it = boost::range::find_if(v, [] (auto&& ver) {
                    return bool(ver.par);
            });
#if __cplusplus <= 201703L
            using mutation_ref = mutation&;
#else
            using mutation_ref = mutation&&;
#endif
            auto m = boost::accumulate(v, mutation(schema, it->par->mut().key()), [this, schema] (mutation_ref m, const version& ver) {
                if (ver.par) {
                    mutation_application_stats app_stats;
                    m.partition().apply(*schema, ver.par->mut().partition(), *schema, app_stats);
                }
                return std::move(m);
            });
            auto live_row_count = m.live_row_count();
            _total_live_count += live_row_count;
            _live_partition_count += !!live_row_count;
            return mutation_and_live_row_count { std::move(m), live_row_count };
        });
        _partition_count = reconciled_partitions.size();

        bool has_diff = false;

        // calculate differences
        for (auto z : boost::combine(versions, reconciled_partitions)) {
            const mutation& m = z.get<1>().mut;
            for (const version& v : z.get<0>()) {
                auto diff = v.par
                          ? m.partition().difference(schema, v.par->mut().unfreeze(schema).partition())
                          : mutation_partition(*schema, m.partition());
                std::optional<mutation> mdiff;
                if (!diff.empty()) {
                    has_diff = true;
                    mdiff = mutation(schema, m.decorated_key(), std::move(diff));
                }
                if (auto [it, added] = _diffs[m.token()].try_emplace(v.from, std::move(mdiff)); !added) {
                    // should not really happen, but lets try to deal with it
                    if (mdiff) {
                        if (it->second) {
                            it->second.value().apply(std::move(mdiff.value()));
                        } else {
                            it->second = std::move(mdiff);
                        }
                    }
                }
            }
        }

        if (has_diff) {
            if (got_incomplete_information(*schema, cmd, original_row_limit, original_per_partition_limit,
                                           original_partition_limit, reconciled_partitions, versions)) {
                return {};
            }
            // filter out partitions with empty diffs
            for (auto it = _diffs.begin(); it != _diffs.end();) {
                if (boost::algorithm::none_of(it->second | boost::adaptors::map_values, std::mem_fn(&std::optional<mutation>::operator bool))) {
                    it = _diffs.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            _diffs.clear();
        }

        find_short_partitions(reconciled_partitions, versions, original_per_partition_limit, original_row_limit, original_partition_limit);

        bool allow_short_reads = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        if (allow_short_reads && _max_live_count >= original_row_limit && _total_live_count < original_row_limit && _total_live_count) {
            // We ended up with less rows than the client asked for (but at least one),
            // avoid retry and mark as short read instead.
            _is_short_read = query::short_read::yes;
        }

        // build reconcilable_result from reconciled data
        // traverse backwards since large keys are at the start
        utils::chunked_vector<partition> vec;
        auto r = boost::accumulate(reconciled_partitions | boost::adaptors::reversed, std::ref(vec), [] (utils::chunked_vector<partition>& a, const mutation_and_live_row_count& m_a_rc) {
            a.emplace_back(partition(m_a_rc.live_row_count, freeze(m_a_rc.mut)));
            return std::ref(a);
        });

        return reconcilable_result(_total_live_count, std::move(r.get()), _is_short_read);
    }
    auto total_live_count() const {
        return _total_live_count;
    }
    auto get_diffs_for_repair() {
        return std::move(_diffs);
    }
};

class abstract_read_executor : public enable_shared_from_this<abstract_read_executor> {
protected:
    using targets_iterator = inet_address_vector_replica_set::iterator;
    using digest_resolver_ptr = ::shared_ptr<digest_read_resolver>;
    using data_resolver_ptr = ::shared_ptr<data_read_resolver>;
    // Clock type for measuring timeouts.
    using clock_type = storage_proxy::clock_type;
    // Clock type for measuring latencies.
    using latency_clock = utils::latency_counter::clock;

    schema_ptr _schema;
    shared_ptr<storage_proxy> _proxy;
    lw_shared_ptr<query::read_command> _cmd;
    lw_shared_ptr<query::read_command> _retry_cmd;
    dht::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    inet_address_vector_replica_set _targets;
    // Targets that were succesfully used for a data or digest request
    inet_address_vector_replica_set _used_targets;
    promise<foreign_ptr<lw_shared_ptr<query::result>>> _result_promise;
    tracing::trace_state_ptr _trace_state;
    lw_shared_ptr<column_family> _cf;
    bool _foreground = true;
    service_permit _permit; // holds admission permit until operation completes

private:
    void on_read_resolved() noexcept {
        // We could have !_foreground if this is called on behalf of background reconciliation.
        _proxy->get_stats().foreground_reads -= int(_foreground);
        _foreground = false;
    }
public:
    abstract_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, size_t block_for,
            inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state, service_permit permit) :
                           _schema(std::move(s)), _proxy(std::move(proxy)), _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)), _trace_state(std::move(trace_state)),
                           _cf(std::move(cf)), _permit(std::move(permit)) {
        _proxy->get_stats().reads++;
        _proxy->get_stats().foreground_reads++;
    }
    virtual ~abstract_read_executor() {
        _proxy->get_stats().reads--;
        _proxy->get_stats().foreground_reads -= int(_foreground);
    }

    /// Targets that were successfully ised for data and/or digest requests.
    ///
    /// Only filled after the request is finished, call only after
    /// execute()'s future is ready.
    inet_address_vector_replica_set used_targets() const {
        return _used_targets;
    }

protected:
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> make_mutation_data_request(lw_shared_ptr<query::read_command> cmd, gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->get_stats().mutation_data_read_attempts.get_ep_stat(ep);
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_mutation_data: querying locally");
            return _proxy->query_mutations_locally(_schema, cmd, _partition_range, timeout, _trace_state);
        } else {
            tracing::trace(_trace_state, "read_mutation_data: sending a message to /{}", ep);
            return _proxy->_messaging.send_read_mutation_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *cmd, _partition_range).then([this, ep](rpc::tuple<reconcilable_result, rpc::optional<cache_temperature>> result_and_hit_rate) {
                auto&& [result, hit_rate] = result_and_hit_rate;
                tracing::trace(_trace_state, "read_mutation_data: got response from /{}", ep);
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>(rpc::tuple(make_foreign(::make_lw_shared<reconcilable_result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid())));
            });
        }
    }
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>> make_data_request(gms::inet_address ep, clock_type::time_point timeout, bool want_digest) {
        ++_proxy->get_stats().data_read_attempts.get_ep_stat(ep);
        auto opts = want_digest
                  ? query::result_options{query::result_request::result_and_digest, digest_algorithm(*_proxy)}
                  : query::result_options{query::result_request::only_result, query::digest_algorithm::none};
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_data: querying locally");
            return _proxy->query_result_local(_schema, _cmd, _partition_range, opts, _trace_state, timeout);
        } else {
            tracing::trace(_trace_state, "read_data: sending a message to /{}", ep);
            return _proxy->_messaging.send_read_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd, _partition_range, opts.digest_algo).then([this, ep](rpc::tuple<query::result, rpc::optional<cache_temperature>, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t> response) {
                auto&& [result, hit_rate, cache_counter, dma_counter, dma_size] = response;
                tracing::trace(_trace_state, "read_data: got response from /{}", ep);
                tracing::modify_cache_counter(_trace_state, cache_counter);
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>>(rpc::tuple(make_foreign(::make_lw_shared<query::result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid()), cache_counter, dma_counter, dma_size));
            });
        }
    }
    future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>> make_digest_request(gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->get_stats().digest_read_attempts.get_ep_stat(ep);
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_digest: querying locally");
            return _proxy->query_result_local_digest(_schema, _cmd, _partition_range, _trace_state,
                        timeout, digest_algorithm(*_proxy));
        } else {
            tracing::trace(_trace_state, "read_digest: sending a message to /{}", ep);
            return _proxy->_messaging.send_read_digest(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd,
                        _partition_range, digest_algorithm(*_proxy)).then([this, ep] (
                    rpc::tuple<query::result_digest, rpc::optional<api::timestamp_type>, rpc::optional<cache_temperature>> digest_timestamp_hit_rate) {
                auto&& [d, t, hit_rate] = digest_timestamp_hit_rate;
                tracing::trace(_trace_state, "read_digest: got response from /{}", ep);
                return make_ready_future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>>(rpc::tuple(d, t ? t.value() : api::missing_timestamp, hit_rate.value_or(cache_temperature::invalid())));
            });
        }
    }
    void make_mutation_data_requests(lw_shared_ptr<query::read_command> cmd, data_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_mutation_data_request(cmd, ep, timeout).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> f) {
                try {
                    auto v = f.get0();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_mutate_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->get_stats().mutation_data_read_completed.get_ep_stat(ep);
                    register_request_latency(latency_clock::now() - start);
                } catch(...) {
                    ++_proxy->get_stats().mutation_data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        }
    }
    void make_data_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout, bool want_digest) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_data_request(ep, timeout, want_digest).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>> f) {
                try {
                    auto v = f.get0();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->get_stats().data_read_completed.get_ep_stat(ep);
                    _used_targets.push_back(ep);
                    register_request_latency(latency_clock::now() - start);
                } catch(...) {
                    ++_proxy->get_stats().data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        }
    }
    void make_digest_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_digest_request(ep, timeout).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>> f) {
                try {
                    auto v = f.get0();
                    _cf->set_hit_rate(ep, std::get<2>(v));
                    resolver->add_digest(ep, std::get<0>(v), std::get<1>(v));
                    ++_proxy->get_stats().digest_read_completed.get_ep_stat(ep);
                    _used_targets.push_back(ep);
                    register_request_latency(latency_clock::now() - start);
                } catch(...) {
                    ++_proxy->get_stats().digest_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        }
    }
    virtual void make_requests(digest_resolver_ptr resolver, clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        auto want_digest = _targets.size() > 1;
        make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest);
        make_digest_requests(resolver, _targets.begin() + 1, _targets.end(), timeout);
    }
    virtual void got_cl() {}
    uint64_t original_row_limit() const {
        return _cmd->get_row_limit();
    }
    uint64_t original_per_partition_row_limit() const {
        return _cmd->slice.partition_row_limit();
    }
    uint32_t original_partition_limit() const {
        return _cmd->partition_limit;
    }
    virtual void adjust_targets_for_reconciliation() {}
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout, lw_shared_ptr<query::read_command> cmd) {
        adjust_targets_for_reconciliation();
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(_schema, cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        // Waited on indirectly.
        make_mutation_data_requests(cmd, data_resolver, _targets.begin(), _targets.end(), timeout);

        // Waited on indirectly.
        (void)data_resolver->done().then_wrapped([this, exec, data_resolver, cmd = std::move(cmd), cl, timeout] (future<> f) {
            try {
                f.get();
                auto rr_opt = data_resolver->resolve(_schema, *cmd, original_row_limit(), original_per_partition_row_limit(), original_partition_limit()); // reconciliation happens here

                // We generate a retry if at least one node reply with count live columns but after merge we have less
                // than the total number of column we are interested in (which may be < count on a retry).
                // So in particular, if no host returned count live columns, we know it's not a short read.
                bool can_send_short_read = rr_opt && rr_opt->is_short_read() && rr_opt->row_count() > 0;
                if (rr_opt && (can_send_short_read || data_resolver->all_reached_end() || rr_opt->row_count() >= original_row_limit()
                               || data_resolver->live_partition_count() >= original_partition_limit())
                        && !data_resolver->any_partition_short_read()) {
                    auto result = ::make_foreign(::make_lw_shared<query::result>(
                            to_data_query_result(std::move(*rr_opt), _schema, _cmd->slice, _cmd->get_row_limit(), cmd->partition_limit)));
                    // wait for write to complete before returning result to prevent multiple concurrent read requests to
                    // trigger repair multiple times and to prevent quorum read to return an old value, even after a quorum
                    // another read had returned a newer value (but the newer value had not yet been sent to the other replicas)
                    // Waited on indirectly.
                    (void)_proxy->schedule_repair(data_resolver->get_diffs_for_repair(), _cl, _trace_state, _permit).then([this, result = std::move(result)] () mutable {
                        _result_promise.set_value(std::move(result));
                        on_read_resolved();
                    }).handle_exception([this, exec] (std::exception_ptr eptr) {
                        try {
                            std::rethrow_exception(eptr);
                        } catch (mutation_write_timeout_exception&) {
                            // convert write error to read error
                            _result_promise.set_exception(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _block_for - 1, _block_for, true));
                        } catch (...) {
                            _result_promise.set_exception(std::current_exception());
                        }
                        on_read_resolved();
                    });
                } else {
                    _proxy->get_stats().read_retries++;
                    _retry_cmd = make_lw_shared<query::read_command>(*cmd);
                    // We asked t (= cmd->get_row_limit()) live columns and got l (=data_resolver->total_live_count) ones.
                    // From that, we can estimate that on this row, for x requested
                    // columns, only l/t end up live after reconciliation. So for next
                    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
                    auto x = [](uint64_t t, uint64_t l) -> uint64_t {
                        using uint128_t = unsigned __int128;
                        auto ret = std::min<uint128_t>(query::max_rows, l == 0 ? t + 1 : (uint128_t) t * t / l + 1);
                        return static_cast<uint64_t>(ret);
                    };
                    auto all_partitions_x = [](uint64_t x, uint32_t partitions) -> uint64_t {
                        using uint128_t = unsigned __int128;
                        auto ret = std::min<uint128_t>(query::max_rows, (uint128_t) x * partitions);
                        return static_cast<uint64_t>(ret);
                    };
                    if (data_resolver->any_partition_short_read() || data_resolver->increase_per_partition_limit()) {
                        // The number of live rows was bounded by the per partition limit.
                        auto new_partition_limit = x(cmd->slice.partition_row_limit(), data_resolver->max_per_partition_live_count());
                        _retry_cmd->slice.set_partition_row_limit(new_partition_limit);
                        auto new_limit = all_partitions_x(new_partition_limit, data_resolver->partition_count());
                        _retry_cmd->set_row_limit(std::max(cmd->get_row_limit(), new_limit));
                    } else {
                        // The number of live rows was bounded by the total row limit or partition limit.
                        if (cmd->partition_limit != query::max_partitions) {
                            _retry_cmd->partition_limit = std::min<uint64_t>(query::max_partitions, x(cmd->partition_limit, data_resolver->live_partition_count()));
                        }
                        if (cmd->get_row_limit() != query::max_rows) {
                            _retry_cmd->set_row_limit(x(cmd->get_row_limit(), data_resolver->total_live_count()));
                        }
                    }

                    // We may be unable to send a single live row because of replicas bailing out too early.
                    // If that is the case disallow short reads so that we can make progress.
                    if (!data_resolver->total_live_count()) {
                        _retry_cmd->slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }

                    slogger.trace("Retrying query with command {} (previous is {})", *_retry_cmd, *cmd);
                    reconcile(cl, timeout, _retry_cmd);
                }
            } catch (...) {
                _result_promise.set_exception(std::current_exception());
                on_read_resolved();
            }
        });
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout) {
        reconcile(cl, timeout, _cmd);
    }

public:
    future<foreign_ptr<lw_shared_ptr<query::result>>> execute(storage_proxy::clock_type::time_point timeout) {
        if (_targets.empty()) {
            // We may have no targets to read from if a DC with zero replication is queried with LOCACL_QUORUM.
            // Return an empty result in this case
            return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>>(make_foreign(make_lw_shared(query::result())));
        }
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_schema, _cl, _block_for,
                db::is_datacenter_local(_cl) ? db::count_local_endpoints(_targets): _targets.size(), timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver, timeout);

        // Waited on indirectly.
        (void)digest_resolver->has_cl().then_wrapped([exec, digest_resolver, timeout] (future<digest_read_result> f) mutable {
            bool background_repair_check = false;
            try {
                exec->got_cl();

                auto&& [result, digests_match] = f.get0(); // can throw

                if (digests_match) {
                    exec->_result_promise.set_value(std::move(result));
                    if (exec->_block_for < exec->_targets.size()) { // if there are more targets then needed for cl, check digest in background
                        background_repair_check = true;
                    }
                    exec->on_read_resolved();
                } else { // digest mismatch
                    // Do not optimize cross-dc repair if read_timestamp is missing (or just negative)
                    // We're interested in reads that happen within write_timeout of a write,
                    // and comparing a timestamp that is too far causes int overflow (#5556)
                    if (is_datacenter_local(exec->_cl) && exec->_cmd->read_timestamp >= api::timestamp_type(0)) {
                        auto write_timeout = exec->_proxy->_db.local().get_config().write_request_timeout_in_ms() * 1000;
                        auto delta = int64_t(digest_resolver->last_modified()) - int64_t(exec->_cmd->read_timestamp);
                        if (std::abs(delta) <= write_timeout) {
                            exec->_proxy->get_stats().global_read_repairs_canceled_due_to_concurrent_write++;
                            // if CL is local and non matching data is modified less then write_timeout ms ago do only local repair
                            auto i = boost::range::remove_if(exec->_targets, std::not_fn(std::cref(db::is_local)));
                            exec->_targets.erase(i, exec->_targets.end());
                        }
                    }
                    exec->reconcile(exec->_cl, timeout);
                    exec->_proxy->get_stats().read_repair_repaired_blocking++;
                }
            } catch (...) {
                exec->_result_promise.set_exception(std::current_exception());
                exec->on_read_resolved();
            }

            // Waited on indirectly.
            (void)digest_resolver->done().then([exec, digest_resolver, timeout, background_repair_check] () mutable {
                if (background_repair_check && !digest_resolver->digests_match()) {
                    exec->_proxy->get_stats().read_repair_repaired_background++;
                    exec->_result_promise = promise<foreign_ptr<lw_shared_ptr<query::result>>>();
                    exec->reconcile(exec->_cl, timeout);
                    return exec->_result_promise.get_future().discard_result();
                } else {
                    return make_ready_future<>();
                }
            }).handle_exception([] (std::exception_ptr eptr) {
                // ignore any failures during background repair
            });
        });

        return _result_promise.get_future();
    }

    lw_shared_ptr<column_family>& get_cf() {
        return _cf;
    }

    // Maximum latency of a successful request made to a replica (over all requests that finished up to this point).
    // Example usage: gathering latency statistics for deciding on invoking speculative retries.
    std::optional<latency_clock::duration> max_request_latency() const {
        if (_max_request_latency == NO_LATENCY) {
            return std::nullopt;
        }
        return _max_request_latency;
    }

private:
    void register_request_latency(latency_clock::duration d) {
        _max_request_latency = std::max(_max_request_latency, d);
    }

    static constexpr latency_clock::duration NO_LATENCY{-1};
    latency_clock::duration _max_request_latency{NO_LATENCY};
};

class never_speculating_read_executor : public abstract_read_executor {
public:
    never_speculating_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state,
                                    service_permit permit) :
                                        abstract_read_executor(std::move(s), std::move(cf), std::move(proxy), std::move(cmd), std::move(pr), cl, 0, std::move(targets), std::move(trace_state), std::move(permit)) {
        _block_for = _targets.size();
    }
};

// this executor always asks for one additional data reply
class always_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
    virtual void make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest);
        make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout);
    }
};

// this executor sends request to an additional replica after some time below timeout
class speculating_read_executor : public abstract_read_executor {
    timer<storage_proxy::clock_type> _speculate_timer;
public:
    using abstract_read_executor::abstract_read_executor;
    virtual void make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) override {
        _speculate_timer.set_callback([this, resolver, timeout] {
            if (!resolver->is_completed()) { // at the time the callback runs request may be completed already
                resolver->add_wait_targets(1); // we send one more request so wait for it too
                // FIXME: consider disabling for CL=*ONE
                auto send_request = [&] (bool has_data) {
                    if (has_data) {
                        _proxy->get_stats().speculative_digest_reads++;
                        make_digest_requests(resolver, _targets.end() - 1, _targets.end(), timeout);
                    } else {
                        _proxy->get_stats().speculative_data_reads++;
                        make_data_requests(resolver, _targets.end() - 1, _targets.end(), timeout, true);
                    }
                };
                send_request(resolver->has_data());
            }
        });
        auto& sr = _schema->speculative_retry();
        auto t = (sr.get_type() == speculative_retry::type::PERCENTILE) ?
            std::min(_cf->get_coordinator_read_latency_percentile(sr.get_value()), std::chrono::milliseconds(_proxy->get_db().local().get_config().read_request_timeout_in_ms()/2)) :
            std::chrono::milliseconds(unsigned(sr.get_value()));
        _speculate_timer.arm(t);

        // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
        // that the last replica in our list is "extra."
        resolver->add_wait_targets(_targets.size() - 1);
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        if (_block_for < _targets.size() - 1) {
            // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a replica more
            // likely to reply; better to let RR fail than the entire query.
            make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest);
            make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout);
        } else {
            // not doing read repair; all replies are important, so it doesn't matter which nodes we
            // perform data reads against vs digest.
            make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest);
            make_digest_requests(resolver, _targets.begin() + 1, _targets.end() - 1, timeout);
        }
    }
    virtual void got_cl() override {
        _speculate_timer.cancel();
    }
    virtual void adjust_targets_for_reconciliation() override {
        _targets = used_targets();
    }
};

db::read_repair_decision storage_proxy::new_read_repair_decision(const schema& s) {
    if (s.dc_local_read_repair_chance() > 0 || s.read_repair_chance() > 0) {
        double chance = _read_repair_chance(_urandom);
        if (s.read_repair_chance() > chance) {
            return db::read_repair_decision::GLOBAL;
        }

        if (s.dc_local_read_repair_chance() > chance) {
            return db::read_repair_decision::DC_LOCAL;
        }
    }

    return db::read_repair_decision::NONE;
}

::shared_ptr<abstract_read_executor> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd,
        schema_ptr schema,
        dht::partition_range pr,
        db::consistency_level cl,
        db::read_repair_decision repair_decision,
        tracing::trace_state_ptr trace_state,
        const inet_address_vector_replica_set& preferred_endpoints,
        bool& is_read_non_local,
        service_permit permit) {
    const dht::token& token = pr.start()->value().token();
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    speculative_retry::type retry_type = schema->speculative_retry().get_type();
    gms::inet_address extra_replica;

    inet_address_vector_replica_set all_replicas = get_live_sorted_endpoints(ks, token);
    // Check for a non-local read before heat-weighted load balancing
    // reordering of endpoints happens. The local endpoint, if
    // present, is always first in the list, as get_live_sorted_endpoints()
    // orders the list by proximity to the local endpoint.
    is_read_non_local |= !all_replicas.empty() && all_replicas.front() != utils::fb_utilities::get_broadcast_address();

    auto cf = _db.local().find_column_family(schema).shared_from_this();
    inet_address_vector_replica_set target_replicas = db::filter_for_query(cl, ks, all_replicas, preferred_endpoints, repair_decision,
            retry_type == speculative_retry::type::NONE ? nullptr : &extra_replica,
            _db.local().get_config().cache_hit_rate_read_balancing() ? &*cf : nullptr);

    slogger.trace("creating read executor for token {} with all: {} targets: {} rp decision: {}", token, all_replicas, target_replicas, repair_decision);
    tracing::trace(trace_state, "Creating read executor for token {} with all: {} targets: {} repair decision: {}", token, all_replicas, target_replicas, repair_decision);

    // Throw UAE early if we don't have enough replicas.
    try {
        db::assure_sufficient_live_nodes(cl, ks, target_replicas);
    } catch (exceptions::unavailable_exception& ex) {
        slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
        get_stats().read_unavailables.mark();
        throw;
    }

    if (repair_decision != db::read_repair_decision::NONE) {
        get_stats().read_repair_attempts++;
    }

    size_t block_for = db::block_for(ks, cl);
    auto p = shared_from_this();
    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || block_for == all_replicas.size()
            || (repair_decision == db::read_repair_decision::DC_LOCAL && is_datacenter_local(cl) && block_for == target_replicas.size())) {
        return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state), std::move(permit));
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit));
    }

    // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
    if (target_replicas.size() == block_for) { // If RRD.DC_LOCAL extra replica may already be present
        if (is_datacenter_local(cl) && !db::is_local(extra_replica)) {
            slogger.trace("read executor no extra target to speculate");
            return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state), std::move(permit));
        } else {
            target_replicas.push_back(extra_replica);
            slogger.trace("creating read executor with extra target {}", extra_replica);
        }
    }

    if (retry_type == speculative_retry::type::ALWAYS) {
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit));
    } else {// PERCENTILE or CUSTOM.
        return ::make_shared<speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit));
    }
}

future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>>
storage_proxy::query_result_local_digest(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout, query::digest_algorithm da) {
    return query_result_local(std::move(s), std::move(cmd), pr, query::result_options::only_digest(da), std::move(trace_state), timeout).then([] (rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t> result_and_hit_rate) {
        auto&& [result, hit_rate, cache_counter, dma_counter, dma_size] = result_and_hit_rate;
        return make_ready_future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>>(rpc::tuple(*result->digest(), result->last_modified(), hit_rate));
    });
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>>
storage_proxy::query_result_local(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, query::result_options opts,
                                  tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout) {
    cmd->slice.options.set_if<query::partition_slice::option::with_digest>(opts.request != query::result_request::only_result);
    if (pr.is_singular()) {
        unsigned shard = dht::shard_of(*s, pr.start()->value().token());
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _db.invoke_on(shard, _read_smp_service_group, [gs = global_schema_ptr(s), prv = dht::partition_range_vector({pr}) /* FIXME: pr is copied */, cmd, opts, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
            auto trace_state = gt.get();
            tracing::trace(trace_state, "Start querying singular range {}", prv.front());
            return db.query(gs, *cmd, opts, prv, trace_state, timeout).then([trace_state](std::tuple<lw_shared_ptr<query::result>, cache_temperature>&& f_ht) {
                auto&& [f, ht] = f_ht;
                tracing::trace(trace_state, "Querying is done");
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>>(rpc::tuple(make_foreign(std::move(f)), ht, get_cache_counter(trace_state), get_dma_counter(trace_state), get_dma_size(trace_state)));
            });
        });
    } else {
        // FIXME: adjust multishard_mutation_query to accept an smp_service_group and propagate it there
        tracing::trace(trace_state, "Start querying token range {}", pr);
        return query_nonsingular_data_locally(s, cmd, {pr}, opts, trace_state, timeout).then(
                [trace_state = std::move(trace_state)] (rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>&& r_ht) {
            auto&& [r, ht] = r_ht;
            tracing::trace(trace_state, "Querying is done");
            return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, tracing::trace_state_ptr::cache_counter_t, tracing::trace_state_ptr::dma_counter_t, tracing::trace_state_ptr::dma_size_t>>(rpc::tuple(std::move(r), ht, get_cache_counter(trace_state), get_dma_counter(trace_state), get_dma_size(trace_state)));
        });
    }
}

void storage_proxy::handle_read_error(std::exception_ptr eptr, bool range) {
    try {
        std::rethrow_exception(eptr);
    } catch (read_timeout_exception& ex) {
        slogger.debug("Read timeout: received {} of {} required replies, data {}present", ex.received, ex.block_for, ex.data_present ? "" : "not ");
        if (range) {
            get_stats().range_slice_timeouts.mark();
        } else {
            get_stats().read_timeouts.mark();
        }
    } catch (...) {
        slogger.debug("Error during read query {}", eptr);
    }
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query_singular(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    utils::small_vector<std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>, 1> exec;
    exec.reserve(partition_ranges.size());

    schema_ptr schema = local_schema_registry().get(cmd->schema_version);

    db::read_repair_decision repair_decision = query_options.read_repair_decision
        ? *query_options.read_repair_decision : new_read_repair_decision(*schema);

    // Update reads_coordinator_outside_replica_set once per request,
    // not once per partition.
    bool is_read_non_local = false;

    const auto tmptr = get_token_metadata_ptr();
    for (auto&& pr: partition_ranges) {
        if (!pr.is_singular()) {
            co_return coroutine::make_exception(std::runtime_error("mixed singular and non singular range are not supported"));
        }

        auto token_range = dht::token_range::make_singular(pr.start()->value().token());
        auto it = query_options.preferred_replicas.find(token_range);
        const auto replicas = it == query_options.preferred_replicas.end()
            ? inet_address_vector_replica_set{} : replica_ids_to_endpoints(*tmptr, it->second);

        auto read_executor = get_read_executor(cmd, schema, std::move(pr), cl, repair_decision,
                                               query_options.trace_state, replicas, is_read_non_local,
                                               query_options.permit);

        exec.emplace_back(read_executor, std::move(token_range));
    }
    if (is_read_non_local) {
        get_stats().reads_coordinator_outside_replica_set++;
    }

    replicas_per_token_range used_replicas;

    // keeps sp alive for the co-routine lifetime
    auto p = shared_from_this();

    foreign_ptr<lw_shared_ptr<query::result>> result;

    try {
        auto timeout = query_options.timeout(*this);
        auto handle_completion = [&used_replicas, tmptr] (std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>& executor_and_token_range) {
                auto& [rex, token_range] = executor_and_token_range;
                used_replicas.emplace(std::move(token_range), endpoints_to_replica_ids(*tmptr, rex->used_targets()));
                auto latency = rex->max_request_latency();
                if (latency) {
                    rex->get_cf()->add_coordinator_read_latency(*latency);
                }
        };

        if (exec.size() == 1) [[likely]] {
            result = co_await exec[0].first->execute(timeout);
            handle_completion(exec[0]);
        } else {
            auto mapper = [timeout, &handle_completion] (
                    std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>& executor_and_token_range) -> future<foreign_ptr<lw_shared_ptr<query::result>>> {
                auto result = co_await executor_and_token_range.first->execute(timeout);
                handle_completion(executor_and_token_range);
                co_return std::move(result);
            };
            query::result_merger merger(cmd->get_row_limit(), cmd->partition_limit);
            merger.reserve(exec.size());
            result = co_await ::map_reduce(exec.begin(), exec.end(), std::move(mapper), std::move(merger));
        }
    } catch(...) {
        handle_read_error(std::current_exception(), false);
        throw;
    }

    co_return coordinator_query_result(std::move(result), std::move(used_replicas), repair_decision);
}

future<query_partition_key_range_concurrent_result>
storage_proxy::query_partition_key_range_concurrent(storage_proxy::clock_type::time_point timeout,
        std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
        lw_shared_ptr<query::read_command> cmd,
        db::consistency_level cl,
        query_ranges_to_vnodes_generator&& ranges_to_vnodes,
        int concurrency_factor,
        tracing::trace_state_ptr trace_state,
        uint64_t remaining_row_count,
        uint32_t remaining_partition_count,
        replicas_per_token_range preferred_replicas,
        service_permit permit) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    auto p = shared_from_this();
    auto& cf= _db.local().find_column_family(schema);
    auto pcf = _db.local().get_config().cache_hit_rate_read_balancing() ? &cf : nullptr;
    std::unordered_map<abstract_read_executor*, std::vector<dht::token_range>> ranges_per_exec;
    const auto tmptr = get_token_metadata_ptr();

    if (_features.cluster_supports_range_scan_data_variant()) {
        cmd->slice.options.set<query::partition_slice::option::range_scan_data_variant>();
    }

    const auto preferred_replicas_for_range = [this, &preferred_replicas, tmptr] (const dht::partition_range& r) {
        auto it = preferred_replicas.find(r.transform(std::mem_fn(&dht::ring_position::token)));
        return it == preferred_replicas.end() ? inet_address_vector_replica_set{} : replica_ids_to_endpoints(*tmptr, it->second);
    };
    const auto to_token_range = [] (const dht::partition_range& r) { return r.transform(std::mem_fn(&dht::ring_position::token)); };

    dht::partition_range_vector ranges = ranges_to_vnodes(concurrency_factor);
    dht::partition_range_vector::iterator i = ranges.begin();

    // query_ranges_to_vnodes_generator can return less results than requested. If the number of results
    // is small enough or there are a lot of results - concurrentcy_factor which is increased by shifting left can
    // eventualy zero out resulting in an infinite recursion. This line makes sure that concurrency factor is never
    // get stuck on 0 and never increased too much if the number of results remains small.
    concurrency_factor = std::max(size_t(1), ranges.size());

    while (i != ranges.end()) {
        dht::partition_range& range = *i;
        inet_address_vector_replica_set live_endpoints = get_live_sorted_endpoints(ks, end_token(range));
        inet_address_vector_replica_set merged_preferred_replicas = preferred_replicas_for_range(*i);
        inet_address_vector_replica_set filtered_endpoints = filter_for_query(cl, ks, live_endpoints, merged_preferred_replicas, pcf);
        std::vector<dht::token_range> merged_ranges{to_token_range(range)};
        ++i;

        // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
        // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
        // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
        while (i != ranges.end())
        {
            const auto current_range_preferred_replicas = preferred_replicas_for_range(*i);
            dht::partition_range& next_range = *i;
            inet_address_vector_replica_set next_endpoints = get_live_sorted_endpoints(ks, end_token(next_range));
            inet_address_vector_replica_set next_filtered_endpoints = filter_for_query(cl, ks, next_endpoints, current_range_preferred_replicas, pcf);

            // Origin has this to say here:
            // *  If the current range right is the min token, we should stop merging because CFS.getRangeSlice
            // *  don't know how to deal with a wrapping range.
            // *  Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
            // *  the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
            // *  wire compatibility, so It's likely easier not to bother;
            // It obviously not apply for us(?), but lets follow origin for now
            if (end_token(range) == dht::maximum_token()) {
                break;
            }

            // Implementing a proper contiguity check is hard, because it requires
            // is_successor(range_bound<dht::ring_position> a, range_bound<dht::ring_position> b)
            // relation to be defined. It is needed for intervals for which their possibly adjacent
            // bounds are either both exclusive or inclusive.
            // For example: is_adjacent([a, b], [c, d]) requires checking is_successor(b, c).
            // Defining a successor relationship for dht::ring_position is hard, because
            // dht::ring_position can possibly contain partition key.
            // Luckily, a full contiguity check here is not needed.
            // Ranges that we want to merge here are formed by dividing a bigger ranges using
            // query_ranges_to_vnodes_generator. By knowing query_ranges_to_vnodes_generator internals,
            // it can be assumed that usually, mergable ranges are of the form [a, b) [b, c).
            // Therefore, for the most part, contiguity check is reduced to equality & inclusivity test.
            // It's fine, that we don't detect contiguity of some other possibly contiguous
            // ranges (like [a, b] [b+1, c]), because not merging contiguous ranges (as opposed
            // to merging discontiguous ones) is not a correctness problem.
            bool maybe_discontiguous = !next_range.start() || !(
                range.end()->value().equal(*schema, next_range.start()->value()) ?
                (range.end()->is_inclusive() || next_range.start()->is_inclusive()) : false
            );
            // Do not merge ranges that may be discontiguous with each other
            if (maybe_discontiguous) {
                break;
            }

            inet_address_vector_replica_set merged = intersection(live_endpoints, next_endpoints);
            inet_address_vector_replica_set current_merged_preferred_replicas = intersection(merged_preferred_replicas, current_range_preferred_replicas);

            // Check if there is enough endpoint for the merge to be possible.
            if (!is_sufficient_live_nodes(cl, ks, merged)) {
                break;
            }

            inet_address_vector_replica_set filtered_merged = filter_for_query(cl, ks, merged, current_merged_preferred_replicas, pcf);

            // Estimate whether merging will be a win or not
            if (!locator::i_endpoint_snitch::get_local_snitch_ptr()->is_worth_merging_for_range_query(filtered_merged, filtered_endpoints, next_filtered_endpoints)) {
                break;
            } else if (pcf) {
                // check that merged set hit rate is not to low
                auto find_min = [pcf] (const inet_address_vector_replica_set& range) {
                    struct {
                        column_family* cf = nullptr;
                        float operator()(const gms::inet_address& ep) const {
                            return float(cf->get_hit_rate(ep).rate);
                        }
                    } ep_to_hr{pcf};
                    return *boost::range::min_element(range | boost::adaptors::transformed(ep_to_hr));
                };
                auto merged = find_min(filtered_merged) * 1.2; // give merged set 20% boost
                if (merged < find_min(filtered_endpoints) && merged < find_min(next_filtered_endpoints)) {
                    // if lowest cache hits rate of a merged set is smaller than lowest cache hit
                    // rate of un-merged sets then do not merge. The idea is that we better issue
                    // two different range reads with highest chance of hitting a cache then one read that
                    // will cause more IO on contacted nodes
                    break;
                }
            }

            // If we get there, merge this range and the next one
            range = dht::partition_range(range.start(), next_range.end());
            live_endpoints = std::move(merged);
            merged_preferred_replicas = std::move(current_merged_preferred_replicas);
            filtered_endpoints = std::move(filtered_merged);
            ++i;
            merged_ranges.push_back(to_token_range(next_range));
        }
        slogger.trace("creating range read executor with targets {}", filtered_endpoints);
        try {
            db::assure_sufficient_live_nodes(cl, ks, filtered_endpoints);
        } catch(exceptions::unavailable_exception& ex) {
            slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
            get_stats().range_slice_unavailables.mark();
            throw;
        }

        exec.push_back(::make_shared<never_speculating_read_executor>(schema, cf.shared_from_this(), p, cmd, std::move(range), cl, std::move(filtered_endpoints), trace_state, permit));
        ranges_per_exec.emplace(exec.back().get(), std::move(merged_ranges));
    }

    query::result_merger merger(cmd->get_row_limit(), cmd->partition_limit);
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.then([p,
            tmptr,
            exec = std::move(exec),
            results = std::move(results),
            ranges_to_vnodes = std::move(ranges_to_vnodes),
            cl,
            cmd,
            concurrency_factor,
            timeout,
            remaining_row_count,
            remaining_partition_count,
            trace_state = std::move(trace_state),
            preferred_replicas = std::move(preferred_replicas),
            ranges_per_exec = std::move(ranges_per_exec),
            permit = std::move(permit)] (foreign_ptr<lw_shared_ptr<query::result>>&& result) mutable {
        result->ensure_counts();
        remaining_row_count -= result->row_count().value();
        remaining_partition_count -= result->partition_count().value();
        results.emplace_back(std::move(result));
        if (ranges_to_vnodes.empty() || !remaining_row_count || !remaining_partition_count) {
            auto used_replicas = replicas_per_token_range();
            for (auto& e : exec) {
                // We add used replicas in separate per-vnode entries even if
                // they were merged, for two reasons:
                // 1) The list of replicas is determined for each vnode
                // separately and thus this makes lookups more convenient.
                // 2) On the next page the ranges might not be merged.
                auto replica_ids = endpoints_to_replica_ids(*tmptr, e->used_targets());
                for (auto& r : ranges_per_exec[e.get()]) {
                    used_replicas.emplace(std::move(r), replica_ids);
                }
            }
            return make_ready_future<query_partition_key_range_concurrent_result>(query_partition_key_range_concurrent_result{std::move(results), std::move(used_replicas)});
        } else {
            cmd->set_row_limit(remaining_row_count);
            cmd->partition_limit = remaining_partition_count;
            return p->query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, std::move(ranges_to_vnodes),
                    concurrency_factor * 2, std::move(trace_state), remaining_row_count, remaining_partition_count, std::move(preferred_replicas), std::move(permit));
        }
    }).handle_exception([p] (std::exception_ptr eptr) {
        p->handle_read_error(eptr, true);
        return make_exception_future<query_partition_key_range_concurrent_result>(eptr);
    });
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());

    // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
    // expensive in clusters with vnodes)
    query_ranges_to_vnodes_generator ranges_to_vnodes(get_token_metadata_ptr(), schema, std::move(partition_ranges), ks.get_replication_strategy().get_type() == locator::replication_strategy_type::local);

    int result_rows_per_range = 0;
    int concurrency_factor = 1;

    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;

    slogger.debug("Estimated result rows per range: {}; requested rows: {}, concurrent range requests: {}",
            result_rows_per_range, cmd->get_row_limit(), concurrency_factor);

    // The call to `query_partition_key_range_concurrent()` below
    // updates `cmd` directly when processing the results. Under
    // some circumstances, when the query executes without deferring,
    // this updating will happen before the lambda object is constructed
    // and hence the updates will be visible to the lambda. This will
    // result in the merger below trimming the results according to the
    // updated (decremented) limits and causing the paging logic to
    // declare the query exhausted due to the non-full page. To avoid
    // this save the original values of the limits here and pass these
    // to the lambda below.
    const auto row_limit = cmd->get_row_limit();
    const auto partition_limit = cmd->partition_limit;

    return query_partition_key_range_concurrent(query_options.timeout(*this),
            std::move(results),
            cmd,
            cl,
            std::move(ranges_to_vnodes),
            concurrency_factor,
            std::move(query_options.trace_state),
            cmd->get_row_limit(),
            cmd->partition_limit,
            std::move(query_options.preferred_replicas),
            std::move(query_options.permit)).then([row_limit, partition_limit] (
                    query_partition_key_range_concurrent_result result) {
        std::vector<foreign_ptr<lw_shared_ptr<query::result>>>& results = result.result;
        replicas_per_token_range& used_replicas = result.replicas;

        query::result_merger merger(row_limit, partition_limit);
        merger.reserve(results.size());

        for (auto&& r: results) {
            merger(std::move(r));
        }

        return make_ready_future<coordinator_query_result>(coordinator_query_result(merger.get(), std::move(used_replicas)));
    });
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    if (slogger.is_enabled(logging::log_level::trace) || qlogger.is_enabled(logging::log_level::trace)) {
        static thread_local int next_id = 0;
        auto query_id = next_id++;

        slogger.trace("query {}.{} cmd={}, ranges={}, id={}", s->ks_name(), s->cf_name(), *cmd, partition_ranges, query_id);
        return do_query(s, cmd, std::move(partition_ranges), cl, std::move(query_options)).then_wrapped([query_id, cmd, s] (future<coordinator_query_result> f) {
            if (f.failed()) {
                auto ex = f.get_exception();
                slogger.trace("query id={} failed: {}", query_id, ex);
                return make_exception_future<coordinator_query_result>(std::move(ex));
            }
            auto qr = f.get0();
            auto& res = qr.query_result;
            if (res->buf().is_linearized()) {
                res->ensure_counts();
                slogger.trace("query_result id={}, size={}, rows={}, partitions={}", query_id, res->buf().size(), *res->row_count(), *res->partition_count());
            } else {
                slogger.trace("query_result id={}, size={}", query_id, res->buf().size());
            }
            qlogger.trace("id={}, {}", query_id, res->pretty_printer(s, cmd->slice));
            return make_ready_future<coordinator_query_result>(std::move(qr));
        });
    }

    return do_query(s, cmd, std::move(partition_ranges), cl, std::move(query_options));
}

future<storage_proxy::coordinator_query_result>
storage_proxy::do_query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    static auto make_empty = [] {
        return make_ready_future<coordinator_query_result>(make_foreign(make_lw_shared<query::result>()));
    };

    auto& slice = cmd->slice;
    if (partition_ranges.empty() ||
            (slice.default_row_ranges().empty() && !slice.get_specific_ranges())) {
        return make_empty();
    }

    if (db::is_serial_consistency(cl)) {
        return do_query_with_paxos(std::move(s), std::move(cmd), std::move(partition_ranges), cl, std::move(query_options));
    } else {
        utils::latency_counter lc;
        lc.start();
        auto p = shared_from_this();

        if (query::is_single_partition(partition_ranges[0])) { // do not support mixed partitions (yet?)
            try {
                return query_singular(cmd,
                        std::move(partition_ranges),
                        cl,
                        std::move(query_options)).finally([lc, p] () mutable {
                    p->get_stats().read.mark(lc.stop().latency());
                    if (lc.is_start()) {
                        p->get_stats().estimated_read.add(lc.latency());
                    }
                });
            } catch (const no_such_column_family&) {
                get_stats().read.mark(lc.stop().latency());
                return make_empty();
            }
        }

        return query_partition_key_range(cmd,
                std::move(partition_ranges),
                cl,
                std::move(query_options)).finally([lc, p] () mutable {
            p->get_stats().range.mark(lc.stop().latency());
            if (lc.is_start()) {
                p->get_stats().estimated_range.add(lc.latency());
            }
        });
    }
}

// WARNING: the function should be called on a shard that owns the key that is been read
future<storage_proxy::coordinator_query_result>
storage_proxy::do_query_with_paxos(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options) {
    if (partition_ranges.size() != 1 || !query::is_single_partition(partition_ranges[0])) {
        return make_exception_future<storage_proxy::coordinator_query_result>(
                exceptions::invalid_request_exception("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time"));
    }

    if (cas_shard(*s, partition_ranges[0].start()->value().as_decorated_key().token()) != this_shard_id()) {
        return make_exception_future<storage_proxy::coordinator_query_result>(std::logic_error("storage_proxy::do_query_with_paxos called on a wrong shard"));
    }
    // All cas networking operations run with query provided timeout
    db::timeout_clock::time_point timeout = query_options.timeout(*this);
    // When to give up due to contention
    db::timeout_clock::time_point cas_timeout = db::timeout_clock::now() +
            std::chrono::milliseconds(_db.local().get_config().cas_contention_timeout_in_ms());

    struct read_cas_request : public cas_request {
        foreign_ptr<lw_shared_ptr<query::result>> res;
        std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts) {
            res = std::move(qr);
            return std::nullopt;
        }
    };

    auto request = seastar::make_shared<read_cas_request>();

    return cas(std::move(s), request, cmd, std::move(partition_ranges), std::move(query_options),
            cl, db::consistency_level::ANY, timeout, cas_timeout, false).then([this, request] (bool is_applied) mutable {
        return make_ready_future<coordinator_query_result>(std::move(request->res));
    });
}

static lw_shared_ptr<query::read_command> read_nothing_read_command(schema_ptr schema) {
    // Note that because this read-nothing command has an empty slice,
    // storage_proxy::query() returns immediately - without any networking.
    auto partition_slice = query::partition_slice({}, {}, {}, query::partition_slice::option_set());
    return ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            query::max_result_size(query::result_memory_limiter::unlimited_result_size));
}

static read_timeout_exception write_timeout_to_read(schema_ptr s, mutation_write_timeout_exception& ex) {
    return read_timeout_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.block_for, false);
}

static read_failure_exception write_failure_to_read(schema_ptr s, mutation_write_failure_exception& ex) {
    return read_failure_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.failures, ex.block_for, false);
}

static mutation_write_timeout_exception read_timeout_to_write(schema_ptr s, read_timeout_exception& ex) {
    return mutation_write_timeout_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.block_for, db::write_type::CAS);
}

static mutation_write_failure_exception read_failure_to_write(schema_ptr s, read_failure_exception& ex) {
    return mutation_write_failure_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.failures, ex.block_for, db::write_type::CAS);
}

/**
 * Apply mutations if and only if the current values in the row for the given key
 * match the provided conditions. The algorithm is "raw" Paxos: that is, Paxos
 * minus leader election -- any node in the cluster may propose changes for any row,
 * which (that is, the row) is the unit of values being proposed, not single columns.
 *
 * The Paxos cohort is only the replicas for the given key, not the entire cluster.
 * So we expect performance to be reasonable, but CAS is still intended to be used
 * "when you really need it," not for all your updates.
 *
 * There are three phases to Paxos:
 *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
 *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
 *     accepted.
 *  2. Accept: if a majority of replicas respond, the coordinator asks replicas to accept the value of the
 *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
 *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
 *     value.
 *
 * Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
 * so here is our approach:
 *  3a. The coordinator sends a commit message to all replicas with the ballot and value.
 *  3b. Because of 1-2, this will be the highest-seen commit ballot. The replicas will note that,
 *      and send it with subsequent promise replies. This allows us to discard acceptance records
 *      for successfully committed replicas, without allowing incomplete proposals to commit erroneously
 *      later on.
 *
 * Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
 * values) between the prepare and accept phases. This gives us a slightly longer window for another
 * coordinator to come along and trump our own promise with a newer one but is otherwise safe.
 *
 * NOTE: `cmd` argument can be nullptr, in which case it's guaranteed that this function would not perform
 * any reads of commited values (in case user of the function is not interested in them).
 *
 * WARNING: the function should be called on a shard that owns the key cas() operates on
 */
future<bool> storage_proxy::cas(schema_ptr schema, shared_ptr<cas_request> request, lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges, storage_proxy::coordinator_query_options query_options,
        db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn,
        clock_type::time_point write_timeout, clock_type::time_point cas_timeout, bool write) {

    assert(partition_ranges.size() == 1);
    assert(query::is_single_partition(partition_ranges[0]));

    db::validate_for_cas(cl_for_paxos);
    db::validate_for_cas_learn(cl_for_learn, schema->ks_name());

    if (cas_shard(*schema, partition_ranges[0].start()->value().as_decorated_key().token()) != this_shard_id()) {
        co_return coroutine::make_exception(std::logic_error("storage_proxy::cas called on a wrong shard"));
    }

    // In case a nullptr is passed to this function (i.e. the caller isn't interested in
    // existing value) we fabricate an "empty"  read_command that does nothing,
    // i.e. appropriate calls to storage_proxy::query immediately return an
    // empty query::result object without accessing any data.
    if (!cmd) {
        cmd = read_nothing_read_command(schema);
    }

    shared_ptr<paxos_response_handler> handler;
    try {
        handler = seastar::make_shared<paxos_response_handler>(shared_from_this(),
                query_options.trace_state, query_options.permit,
                partition_ranges[0].start()->value().as_decorated_key(),
                schema, cmd, cl_for_paxos, cl_for_learn, write_timeout, cas_timeout);
    } catch (exceptions::unavailable_exception& ex) {
        write ?  get_stats().cas_write_unavailables.mark() : get_stats().cas_read_unavailables.mark();
        throw;
    }

    db::consistency_level cl = cl_for_paxos == db::consistency_level::LOCAL_SERIAL ?
        db::consistency_level::LOCAL_QUORUM : db::consistency_level::QUORUM;

    unsigned contentions;

    dht::token token = partition_ranges[0].start()->value().as_decorated_key().token();
    utils::latency_counter lc;
    lc.start();

    bool condition_met;

    try {
        auto update_stats = seastar::defer ([&] {
            get_stats().cas_foreground--;
            write ? get_stats().cas_write.mark(lc.stop().latency()) : get_stats().cas_read.mark(lc.stop().latency());
            if (lc.is_start()) {
                write ? get_stats().estimated_cas_write.add(lc.latency()) :
                        get_stats().estimated_cas_read.add(lc.latency());
            }
            if (contentions > 0) {
                write ? get_stats().cas_write_contention.add(contentions) : get_stats().cas_read_contention.add(contentions);
            }
        });

        paxos::paxos_state::guard l = co_await paxos::paxos_state::get_cas_lock(token, write_timeout);

        while (true) {
            // Finish the previous PAXOS round, if any, and, as a side effect, compute
            // a ballot (round identifier) which is a) unique b) has good chances of being
            // recent enough.
            auto [ballot, qr] = co_await handler->begin_and_repair_paxos(query_options.cstate, contentions, write);
            // Read the current values and check they validate the conditions.
            if (qr) {
                paxos::paxos_state::logger.debug("CAS[{}]: Using prefetched values for CAS precondition",
                        handler->id());
                tracing::trace(handler->tr_state, "Using prefetched values for CAS precondition");
            } else {
                paxos::paxos_state::logger.debug("CAS[{}]: Reading existing values for CAS precondition",
                        handler->id());
                tracing::trace(handler->tr_state, "Reading existing values for CAS precondition");
                ++get_stats().cas_failed_read_round_optimization;

                auto pr = partition_ranges; // cannot move original because it can be reused during retry
                auto cqr = co_await query(schema, cmd, std::move(pr), cl, query_options);
                qr = std::move(cqr.query_result);
            }

            auto mutation = request->apply(std::move(qr), cmd->slice, utils::UUID_gen::micros_timestamp(ballot));
            condition_met = true;
            if (!mutation) {
                if (write) {
                    paxos::paxos_state::logger.debug("CAS[{}] precondition does not match current values", handler->id());
                    tracing::trace(handler->tr_state, "CAS precondition does not match current values");
                    ++get_stats().cas_write_condition_not_met;
                    condition_met = false;
                }
                // If a condition is not met we still need to complete paxos round to achieve
                // linearizability otherwise next write attempt may read differnt value as described
                // in https://github.com/scylladb/scylla/issues/6299
                // Let's use empty mutation as a value and proceed
                mutation.emplace(handler->schema(), handler->key());
                // since the value we are writing is dummy we may use minimal consistency level for learn
                handler->set_cl_for_learn(db::consistency_level::ANY);
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] precondition is met; proposing client-requested updates for {}",
                        handler->id(), ballot);
                tracing::trace(handler->tr_state, "CAS precondition is met; proposing client-requested updates for {}", ballot);
            }

            auto proposal = make_lw_shared<paxos::proposal>(ballot, freeze(*mutation));

            bool is_accepted = co_await handler->accept_proposal(proposal);
            if (is_accepted) {
                // The majority (aka a QUORUM) has promised the coordinator to
                // accept the action associated with the computed ballot.
                // Apply the mutation.
                try {
                  co_await handler->learn_decision(std::move(proposal));
                } catch (unavailable_exception& e) {
                    // if learning stage encountered unavailablity error lets re-map it to a write error
                    // since unavailable error means that operation has never ever started which is not
                    // the case here
                    schema_ptr schema = handler->schema();
                    throw mutation_write_timeout_exception(schema->ks_name(), schema->cf_name(),
                                          e.consistency, e.alive, e.required, db::write_type::CAS);
                }
                paxos::paxos_state::logger.debug("CAS[{}] successful", handler->id());
                tracing::trace(handler->tr_state, "CAS successful");
                break;
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] PAXOS proposal not accepted (pre-empted by a higher ballot)",
                        handler->id());
                tracing::trace(handler->tr_state, "PAXOS proposal not accepted (pre-empted by a higher ballot)");
                ++contentions;
                co_await sleep_approx_50ms();
            }
        }
    } catch (read_failure_exception& ex) {
        write ? throw read_failure_to_write(schema, ex) : throw;
    } catch (read_timeout_exception& ex) {
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw read_timeout_to_write(schema, ex);
        } else {
            get_stats().cas_read_timeouts.mark();
            throw;
        }
    } catch (mutation_write_failure_exception& ex) {
        write ? throw : throw write_failure_to_read(schema, ex);
    } catch (mutation_write_timeout_exception& ex) {
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw;
        } else {
            get_stats().cas_read_timeouts.mark();
            throw write_timeout_to_read(schema, ex);
        }
    } catch (exceptions::unavailable_exception& ex) {
        write ? get_stats().cas_write_unavailables.mark() :  get_stats().cas_read_unavailables.mark();
        throw;
    } catch (seastar::semaphore_timed_out& ex) {
        paxos::paxos_state::logger.trace("CAS[{}]: timeout while waiting for row lock {}", handler->id());
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw mutation_write_timeout_exception(schema->ks_name(), schema->cf_name(), cl_for_paxos, 0,  handler->block_for(), db::write_type::CAS);
        } else {
            get_stats().cas_read_timeouts.mark();
            throw read_timeout_exception(schema->ks_name(), schema->cf_name(), cl_for_paxos, 0,  handler->block_for(), 0);
        }
    }

    co_return condition_met;
}

inet_address_vector_replica_set storage_proxy::get_live_endpoints(keyspace& ks, const dht::token& token) const {
    auto erm = ks.get_effective_replication_map();
    inet_address_vector_replica_set eps = erm->get_natural_endpoints_without_node_being_replaced(token);
    auto itend = boost::range::remove_if(eps, std::not1(std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &_gossiper)));
    eps.erase(itend, eps.end());
    return eps;
}

void storage_proxy::sort_endpoints_by_proximity(inet_address_vector_replica_set& eps) {
    locator::i_endpoint_snitch::get_local_snitch_ptr()->sort_by_proximity(utils::fb_utilities::get_broadcast_address(), eps);
    // FIXME: before dynamic snitch is implement put local address (if present) at the beginning
    auto it = boost::range::find(eps, utils::fb_utilities::get_broadcast_address());
    if (it != eps.end() && it != eps.begin()) {
        std::iter_swap(it, eps.begin());
    }
}

inet_address_vector_replica_set storage_proxy::get_live_sorted_endpoints(keyspace& ks, const dht::token& token) const {
    auto eps = get_live_endpoints(ks, token);
    sort_endpoints_by_proximity(eps);
    return eps;
}

inet_address_vector_replica_set storage_proxy::intersection(const inet_address_vector_replica_set& l1, const inet_address_vector_replica_set& l2) {
    inet_address_vector_replica_set inter;
    inter.reserve(l1.size());
    std::remove_copy_if(l1.begin(), l1.end(), std::back_inserter(inter), [&l2] (const gms::inet_address& a) {
        return std::find(l2.begin(), l2.end(), a) == l2.end();
    });
    return inter;
}

query_ranges_to_vnodes_generator::query_ranges_to_vnodes_generator(const locator::token_metadata_ptr tmptr, schema_ptr s, dht::partition_range_vector ranges, bool local) :
        _s(s), _ranges(std::move(ranges)), _i(_ranges.begin()), _local(local), _tmptr(std::move(tmptr)) {}

dht::partition_range_vector query_ranges_to_vnodes_generator::operator()(size_t n) {
    n = std::min(n, size_t(1024));

    dht::partition_range_vector result;
    result.reserve(n);
    while (_i != _ranges.end() && result.size() != n) {
        process_one_range(n, result);
    }
    return result;
}

bool query_ranges_to_vnodes_generator::empty() const {
    return _ranges.end() == _i;
}

/**
 * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
 * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
 */
void query_ranges_to_vnodes_generator::process_one_range(size_t n, dht::partition_range_vector& ranges) {
    dht::ring_position_comparator cmp(*_s);
    dht::partition_range& cr = *_i;

    auto get_remainder = [this, &cr] {
        _i++;
       return std::move(cr);
    };

    auto add_range = [&ranges] (dht::partition_range&& r) {
        ranges.emplace_back(std::move(r));
    };

    if (_local) { // if the range is local no need to divide to vnodes
        add_range(get_remainder());
        return;
    }

    // special case for bounds containing exactly 1 token
    if (start_token(cr) == end_token(cr)) {
        if (start_token(cr).is_minimum()) {
            _i++; // empty range? Move to the next one
            return;
        }
        add_range(get_remainder());
        return;
    }

    // divide the queryRange into pieces delimited by the ring
    auto ring_iter = _tmptr->ring_range(cr.start());
    for (const dht::token& upper_bound_token : ring_iter) {
        /*
         * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
         *   - if remainder is tokens, then we'll just split using the provided token.
         *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
         *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
         *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
         *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
         *     keys having 15 as token and B include none of those (since that is what our node owns).
         * asSplitValue() abstracts that choice.
         */

        dht::ring_position split_point(upper_bound_token, dht::ring_position::token_bound::end);
        if (!cr.contains(split_point, cmp)) {
            break; // no more splits
        }


        // We shouldn't attempt to split on upper bound, because it may result in
        // an ambiguous range of the form (x; x]
        if (end_token(cr) == upper_bound_token) {
            break;
        }

        std::pair<dht::partition_range, dht::partition_range> splits =
                cr.split(split_point, cmp);

        add_range(std::move(splits.first));
        cr = std::move(splits.second);
        if (ranges.size() == n) {
            // we have enough ranges
            break;
        }
    }

    if (ranges.size() < n) {
        add_range(get_remainder());
    }
}

bool storage_proxy::hints_enabled(db::write_type type) const noexcept {
    return (!_hints_manager.is_disabled_for_all() && type != db::write_type::CAS) || type == db::write_type::VIEW;
}

db::hints::manager& storage_proxy::hints_manager_for(db::write_type type) {
    return type == db::write_type::VIEW ? _hints_for_views_manager : _hints_manager;
}

future<> storage_proxy::truncate_blocking(sstring keyspace, sstring cfname) {
    slogger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);

    if (!_gossiper.get_unreachable_token_owners().empty()) {
        slogger.info("Cannot perform truncate, some hosts are down");
        // Since the truncate operation is so aggressive and is typically only
        // invoked by an admin, for simplicity we require that all nodes are up
        // to perform the operation.
        auto live_members = _gossiper.get_live_members().size();

        return make_exception_future<>(exceptions::unavailable_exception(db::consistency_level::ALL,
                live_members + _gossiper.get_unreachable_members().size(),
                live_members));
    }

    auto all_endpoints = _gossiper.get_live_token_owners();
    auto& ms = _messaging;
    auto timeout = std::chrono::milliseconds(_db.local().get_config().truncate_request_timeout_in_ms());

    slogger.trace("Enqueuing truncate messages to hosts {}", all_endpoints);

    return parallel_for_each(all_endpoints, [keyspace, cfname, &ms, timeout](auto ep) {
        return ms.send_truncate(netw::messaging_service::msg_addr{ep, 0}, timeout, keyspace, cfname);
    }).handle_exception([cfname](auto ep) {
       try {
           std::rethrow_exception(ep);
       } catch (rpc::timeout_error& e) {
           slogger.trace("Truncation of {} timed out: {}", cfname, e.what());
           throw;
       } catch (...) {
           throw;
       }
    });
}

void storage_proxy::init_messaging_service(shared_ptr<migration_manager> mm) {
    auto& ms = _messaging;
    ms.register_counter_mutation([&ms, mm] (const rpc::client_info& cinfo, rpc::opt_time_point t, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);

        tracing::trace_state_ptr trace_state_ptr;
        if (trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        return do_with(std::vector<frozen_mutation_and_schema>(),
                       [cl, src_addr, timeout = *t, fms = std::move(fms), trace_state_ptr = std::move(trace_state_ptr), &ms, mm] (std::vector<frozen_mutation_and_schema>& mutations) mutable {
            return parallel_for_each(std::move(fms), [&mutations, src_addr, &ms, mm] (frozen_mutation& fm) {
                // FIXME: optimise for cases when all fms are in the same schema
                auto schema_version = fm.schema_version();
                return mm->get_schema_for_write(schema_version, std::move(src_addr), ms).then([&mutations, fm = std::move(fm)] (schema_ptr s) mutable {
                    mutations.emplace_back(frozen_mutation_and_schema { std::move(fm), std::move(s) });
                });
            }).then([trace_state_ptr = std::move(trace_state_ptr), &mutations, cl, timeout] {
                auto sp = get_local_shared_storage_proxy();
                return sp->mutate_counters_on_leader(std::move(mutations), cl, timeout, std::move(trace_state_ptr), /* FIXME: rpc should also pass a permit down to callbacks */ empty_service_permit());
            });
        });
    });

    static auto handle_write = [] (netw::messaging_service::msg_addr src_addr, service::migration_manager& mm, rpc::opt_time_point t,
                      utils::UUID schema_version, auto in, std::vector<gms::inet_address> forward, gms::inet_address reply_to,
                      unsigned shard, storage_proxy::response_id_type response_id, std::optional<tracing::trace_info> trace_info,
                      auto&& apply_fn, auto&& forward_fn) {
        tracing::trace_state_ptr trace_state_ptr;

        if (trace_info) {
            tracing::trace_info& tr_info = *trace_info;
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(tr_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        storage_proxy::clock_type::time_point timeout;
        if (!t) {
            auto timeout_in_ms = get_local_shared_storage_proxy()->_db.local().get_config().write_request_timeout_in_ms();
            timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        } else {
            timeout = *t;
        }

        return do_with(std::move(in), get_local_shared_storage_proxy(), size_t(0), [src_addr = std::move(src_addr),
                       forward = std::move(forward), reply_to, shard, response_id, trace_state_ptr, timeout,
                       schema_version, apply_fn = std::move(apply_fn), forward_fn = std::move(forward_fn), &mm]
                       (const auto& m, shared_ptr<storage_proxy>& p, size_t& errors) mutable {
            ++p->get_stats().received_mutations;
            p->get_stats().forwarded_mutations += forward.size();
            return when_all(
                // mutate_locally() may throw, putting it into apply() converts exception to a future.
                futurize_invoke([timeout, &p, &m, reply_to, shard, src_addr = std::move(src_addr), schema_version,
                                      apply_fn = std::move(apply_fn), trace_state_ptr, &mm] () mutable {
                    // FIXME: get_schema_for_write() doesn't timeout
                    return mm.get_schema_for_write(schema_version, netw::messaging_service::msg_addr{reply_to, shard}, p->_messaging)
                                         .then([&m, &p, timeout, apply_fn = std::move(apply_fn), trace_state_ptr] (schema_ptr s) mutable {
                        return apply_fn(p, trace_state_ptr, std::move(s), m, timeout);
                    });
                }).then([&p, reply_to, shard, response_id, trace_state_ptr] () {
                    // We wait for send_mutation_done to complete, otherwise, if reply_to is busy, we will accumulate
                    // lots of unsent responses, which can OOM our shard.
                    //
                    // Usually we will return immediately, since this work only involves appending data to the connection
                    // send buffer.
                    tracing::trace(trace_state_ptr, "Sending mutation_done to /{}", reply_to);
                    return p->_messaging.send_mutation_done(
                            netw::messaging_service::msg_addr{reply_to, shard},
                            shard,
                            response_id,
                            p->get_view_update_backlog()).then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, &p, &errors] (std::exception_ptr eptr) {
                    seastar::log_level l = seastar::log_level::warn;
                    try {
                        std::rethrow_exception(eptr);
                    } catch (timed_out_error&) {
                        // ignore timeouts so that logs are not flooded.
                        // database total_writes_timedout counter was incremented.
                        l = seastar::log_level::debug;
                    } catch (...) {
                        // ignore
                    }
                    slogger.log(l, "Failed to apply mutation from {}#{}: {}", reply_to, shard, eptr);
                    errors++;
                }),
                parallel_for_each(forward.begin(), forward.end(), [reply_to, shard, response_id, &m, &p, trace_state_ptr,
                                  timeout, &errors, forward_fn = std::move(forward_fn)] (gms::inet_address forward) {
                    tracing::trace(trace_state_ptr, "Forwarding a mutation to /{}", forward);
                    return forward_fn(p, netw::messaging_service::msg_addr{forward, 0}, timeout, m, reply_to, shard, response_id,
                                      tracing::make_trace_info(trace_state_ptr))
                            .then_wrapped([&p, &errors] (future<> f) {
                        if (f.failed()) {
                            ++p->get_stats().forwarding_errors;
                            errors++;
                        };
                        f.ignore_ready_future();
                    });
                })
            ).then_wrapped([trace_state_ptr, reply_to, shard, response_id, &errors, &p] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore results, since we'll be returning them via MUTATION_DONE/MUTATION_FAILURE verbs
                auto fut = make_ready_future<seastar::rpc::no_wait_type>(netw::messaging_service::no_wait());
                if (errors) {
                    tracing::trace(trace_state_ptr, "Sending mutation_failure with {} failures to /{}", errors, reply_to);
                    fut = p->_messaging.send_mutation_failed(
                            netw::messaging_service::msg_addr{reply_to, shard},
                            shard,
                            response_id,
                            errors,
                            p->get_view_update_backlog()).then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                        return netw::messaging_service::no_wait();
                    });
                }
                return fut.finally([trace_state_ptr] {
                    tracing::trace(trace_state_ptr, "Mutation handling is done");
                });
            });
        });
    };

    auto receive_mutation_handler = [] (shared_ptr<service::migration_manager> mm, smp_service_group smp_grp, const rpc::client_info& cinfo, rpc::opt_time_point t, frozen_mutation in, std::vector<gms::inet_address> forward,
            gms::inet_address reply_to, unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);

        utils::UUID schema_version = in.schema_version();
        return handle_write(src_addr, *mm, t, schema_version, std::move(in), std::move(forward), reply_to, shard, response_id,
                trace_info ? *trace_info : std::nullopt,
                /* apply_fn */ [smp_grp] (shared_ptr<storage_proxy>& p, tracing::trace_state_ptr tr_state, schema_ptr s, const frozen_mutation& m,
                        clock_type::time_point timeout) {
                    return p->mutate_locally(std::move(s), m, std::move(tr_state), db::commitlog::force_sync::no, timeout, smp_grp);
                },
                /* forward_fn */ [] (shared_ptr<storage_proxy>& p, netw::messaging_service::msg_addr addr, clock_type::time_point timeout, const frozen_mutation& m,
                        gms::inet_address reply_to, unsigned shard, response_id_type response_id,
                        std::optional<tracing::trace_info> trace_info) {
                    return p->_messaging.send_mutation(addr, timeout, m, {}, reply_to, shard, response_id, std::move(trace_info));
                });
    };
    ms.register_mutation(std::bind_front<>(receive_mutation_handler, mm, _write_smp_service_group));
    ms.register_hint_mutation(std::bind_front<>(receive_mutation_handler, mm, _hints_write_smp_service_group));

    ms.register_paxos_learn([mm] (const rpc::client_info& cinfo, rpc::opt_time_point t, paxos::proposal decision,
            std::vector<gms::inet_address> forward, gms::inet_address reply_to, unsigned shard,
            storage_proxy::response_id_type response_id, std::optional<tracing::trace_info> trace_info) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);

        utils::UUID schema_version = decision.update.schema_version();
        return handle_write(src_addr, *mm, t, schema_version, std::move(decision), std::move(forward), reply_to, shard,
                response_id, trace_info,
               /* apply_fn */ [] (shared_ptr<storage_proxy>& p, tracing::trace_state_ptr tr_state, schema_ptr s,
                       const paxos::proposal& decision, clock_type::time_point timeout) {
                     return paxos::paxos_state::learn(std::move(s), decision, timeout, tr_state);
              },
              /* forward_fn */ [] (shared_ptr<storage_proxy>& p, netw::messaging_service::msg_addr addr, clock_type::time_point timeout, const paxos::proposal& m,
                      gms::inet_address reply_to, unsigned shard, response_id_type response_id,
                      std::optional<tracing::trace_info> trace_info) {
                    return p->_messaging.send_paxos_learn(addr, timeout, m, {}, reply_to, shard, response_id, std::move(trace_info));
              });
    });
    ms.register_mutation_done([this] (const rpc::client_info& cinfo, unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<db::view::update_backlog> backlog) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return container().invoke_on(shard, _write_ack_smp_service_group, [from, response_id, backlog = std::move(backlog)] (storage_proxy& sp) mutable {
            sp.got_response(response_id, from, std::move(backlog));
            return netw::messaging_service::no_wait();
        });
    });
    ms.register_mutation_failed([this] (const rpc::client_info& cinfo, unsigned shard, storage_proxy::response_id_type response_id, size_t num_failed, rpc::optional<db::view::update_backlog> backlog) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return container().invoke_on(shard, _write_ack_smp_service_group, [from, response_id, num_failed, backlog = std::move(backlog)] (storage_proxy& sp) mutable {
            sp.got_failure_response(response_id, from, num_failed, std::move(backlog), error::FAILURE, std::nullopt);
            return netw::messaging_service::no_wait();
        });
    });
    ms.register_read_data([mm] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_data: message received from /{}", src_addr.addr);
        }
        auto da = oda.value_or(query::digest_algorithm::MD5);
        auto sp = get_local_shared_storage_proxy();
        if (!cmd.max_result_size) {
            auto& cfg = sp->local_db().get_config();
            cmd.max_result_size.emplace(cfg.max_memory_for_unlimited_query_soft_limit(), cfg.max_memory_for_unlimited_query_hard_limit());
        }
        return tracing::start(trace_state_ptr).then([pr = std::move(pr), sp = std::move(sp), trace_state_ptr, &cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), da, t, mm] () {
            return do_with(std::move(pr), std::move(sp), std::move(trace_state_ptr), [&cinfo, cmd = std::move(cmd), src_addr = std::move(src_addr), da, t, mm] (::compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
                p->get_stats().replica_data_reads++;
                auto src_ip = src_addr.addr;
                return mm->get_schema_for_read(cmd->schema_version, std::move(src_addr), p->_messaging).then([cmd, da, &pr, &p, &trace_state_ptr, t] (schema_ptr s) {
                    auto pr2 = ::compat::unwrap(std::move(pr), *s);
                    if (pr2.second) {
                        // this function assumes singular queries but doesn't validate
                        throw std::runtime_error("READ_DATA called with wrapping range");
                    }
                    query::result_options opts;
                    opts.digest_algo = da;
                    opts.request = da == query::digest_algorithm::none ? query::result_request::only_result : query::result_request::result_and_digest;
                    auto timeout = t ? *t : db::no_timeout;
                    return p->query_result_local(std::move(s), cmd, std::move(pr2.first), opts, trace_state_ptr, timeout);
                }).finally([&trace_state_ptr, src_ip] () mutable {
                    tracing::trace(trace_state_ptr, "read_data handling is done, sending a response to /{}", src_ip);
                    return tracing::stop(trace_state_ptr);
                });
            });
        });
    });
    ms.register_read_mutation_data([mm] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_mutation_data: message received from /{}", src_addr.addr);
        }
        if (!cmd.max_result_size) {
            cmd.max_result_size.emplace(cinfo.retrieve_auxiliary<uint64_t>("max_result_size"));
        }
        return do_with(std::move(pr),
                       get_local_shared_storage_proxy(),
                       std::move(trace_state_ptr),
                       ::compat::one_or_two_partition_ranges({}),
                       [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), t, mm] (
                               ::compat::wrapping_partition_range& pr,
                               shared_ptr<storage_proxy>& p,
                               tracing::trace_state_ptr& trace_state_ptr,
                               ::compat::one_or_two_partition_ranges& unwrapped) mutable {
            p->get_stats().replica_mutation_data_reads++;
            auto src_ip = src_addr.addr;
            return mm->get_schema_for_read(cmd->schema_version, std::move(src_addr), p->_messaging).then([cmd, &pr, &p, &trace_state_ptr, &unwrapped, t] (schema_ptr s) mutable {
                unwrapped = ::compat::unwrap(std::move(pr), *s);
                auto timeout = t ? *t : db::no_timeout;
                return p->query_mutations_locally(std::move(s), std::move(cmd), unwrapped, timeout, trace_state_ptr);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_mutation_data handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_read_digest([mm] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_digest: message received from /{}", src_addr.addr);
        }
        auto da = oda.value_or(query::digest_algorithm::MD5);
        if (!cmd.max_result_size) {
            cmd.max_result_size.emplace(cinfo.retrieve_auxiliary<uint64_t>("max_result_size"));
        }
        return do_with(std::move(pr), get_local_shared_storage_proxy(), std::move(trace_state_ptr), [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), da, t, mm] (::compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
            p->get_stats().replica_digest_reads++;
            auto src_ip = src_addr.addr;
            return mm->get_schema_for_read(cmd->schema_version, std::move(src_addr), p->_messaging).then([cmd, &pr, &p, &trace_state_ptr, t, da] (schema_ptr s) {
                auto pr2 = ::compat::unwrap(std::move(pr), *s);
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DIGEST called with wrapping range");
                }
                auto timeout = t ? *t : db::no_timeout;
                return p->query_result_local_digest(std::move(s), cmd, std::move(pr2.first), trace_state_ptr, timeout, da);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_digest handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_truncate([this](sstring ksname, sstring cfname) {
        return do_with(utils::make_joinpoint([] { return db_clock::now();}),
                        [this, ksname, cfname](auto& tsf) {
            return container().invoke_on_all(_write_smp_service_group, [ksname, cfname, &tsf](storage_proxy& sp) {
                return sp._db.local().truncate(ksname, cfname, [&tsf] { return tsf.value(); });
            });
        });
    });

    // Register PAXOS verb handlers
    ms.register_paxos_prepare([this, mm] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
                query::read_command cmd, partition_key key, utils::UUID ballot, bool only_digest, query::digest_algorithm da,
                std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_prepare: message received from /{} ballot {}", src_ip, ballot);
        }
        if (!cmd.max_result_size) {
            cmd.max_result_size.emplace(cinfo.retrieve_auxiliary<uint64_t>("max_result_size"));
        }

        return mm->get_schema_for_read(cmd.schema_version, src_addr, _messaging).then([this, cmd = std::move(cmd), key = std::move(key), ballot,
                         only_digest, da, timeout, tr_state = std::move(tr_state), src_ip] (schema_ptr schema) mutable {
            dht::token token = dht::get_token(*schema, key);
            unsigned shard = dht::shard_of(*schema, token);
            bool local = shard == this_shard_id();
            get_stats().replica_cross_shard_ops += !local;
            return smp::submit_to(shard, _write_smp_service_group, [gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(std::move(tr_state)),
                                     local, cmd = make_lw_shared<query::read_command>(std::move(cmd)), key = std::move(key),
                                     ballot, only_digest, da, timeout, src_ip] () {
                tracing::trace_state_ptr tr_state = gt;
                return paxos::paxos_state::prepare(tr_state, gs, *cmd, key, ballot, only_digest, da, *timeout).then([src_ip, tr_state] (paxos::prepare_response r) {
                    tracing::trace(tr_state, "paxos_prepare: handling is done, sending a response to /{}", src_ip);
                    return make_foreign(std::make_unique<paxos::prepare_response>(std::move(r)));
                });
            });
        });
    });
    ms.register_paxos_accept([this, mm] (const rpc::client_info& cinfo, rpc::opt_time_point timeout, paxos::proposal proposal,
            std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_accept: message received from /{} ballot {}", src_ip, proposal);
        }

        auto f = mm->get_schema_for_read(proposal.update.schema_version(), src_addr, _messaging).then([this, tr_state = std::move(tr_state),
                                                              proposal = std::move(proposal), timeout] (schema_ptr schema) mutable {
            dht::token token = proposal.update.decorated_key(*schema).token();
            unsigned shard = dht::shard_of(*schema, token);
            bool local = shard == this_shard_id();
            get_stats().replica_cross_shard_ops += !local;
            return smp::submit_to(shard, _write_smp_service_group, [gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(std::move(tr_state)),
                                     local, proposal = std::move(proposal), timeout, token] () {
                return paxos::paxos_state::accept(gt, gs, token, proposal, *timeout);
            });
        });

        if (tr_state.has_tracing()) {
            f = f.finally([tr_state, src_ip] {
                tracing::trace(tr_state, "paxos_accept: handling is done, sending a response to /{}", src_ip);
            });
        }

        return f;
    });
    ms.register_paxos_prune([this, mm] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
                utils::UUID schema_id, partition_key key, utils::UUID ballot, std::optional<tracing::trace_info> trace_info) {
        static thread_local uint16_t pruning = 0;
        static constexpr uint16_t pruning_limit = 1000; // since PRUNE verb is one way replica side has its own queue limit
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_prune: message received from /{} ballot {}", src_ip, ballot);
        }

        if (pruning >= pruning_limit) {
            get_stats().cas_replica_dropped_prune++;
            tracing::trace(tr_state, "paxos_prune: do not prune due to overload", src_ip);
            return make_ready_future<seastar::rpc::no_wait_type>(netw::messaging_service::no_wait());
        }

        pruning++;
        auto d = defer([] { pruning--; });
        return mm->get_schema_for_read(schema_id, src_addr, _messaging).then([this, key = std::move(key), ballot,
                         timeout, tr_state = std::move(tr_state), src_ip, d = std::move(d)] (schema_ptr schema) mutable {
            dht::token token = dht::get_token(*schema, key);
            unsigned shard = dht::shard_of(*schema, token);
            bool local = shard == this_shard_id();
            get_stats().replica_cross_shard_ops += !local;
            return smp::submit_to(shard, _write_smp_service_group, [gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(std::move(tr_state)),
                                     local,  key = std::move(key), ballot, timeout, src_ip, d = std::move(d)] () {
                tracing::trace_state_ptr tr_state = gt;
                return paxos::paxos_state::prune(gs, key, ballot,  *timeout, tr_state).then([src_ip, tr_state] () {
                    tracing::trace(tr_state, "paxos_prune: handling is done, sending a response to /{}", src_ip);
                    return netw::messaging_service::no_wait();
                });
            });
        });
    });
}

future<> storage_proxy::uninit_messaging_service() {
    auto& ms = _messaging;
    return when_all_succeed(
        ms.unregister_counter_mutation(),
        ms.unregister_mutation(),
        ms.unregister_hint_mutation(),
        ms.unregister_mutation_done(),
        ms.unregister_mutation_failed(),
        ms.unregister_read_data(),
        ms.unregister_read_mutation_data(),
        ms.unregister_read_digest(),
        ms.unregister_truncate(),
        ms.unregister_paxos_prepare(),
        ms.unregister_paxos_accept(),
        ms.unregister_paxos_learn(),
        ms.unregister_paxos_prune()
    ).discard_result();

}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state) {
    if (pr.is_singular()) {
        unsigned shard = dht::shard_of(*s, pr.start()->value().token());
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _db.invoke_on(shard, _read_smp_service_group, [cmd, &pr, gs=global_schema_ptr(s), timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
            return db.query_mutations(gs, *cmd, pr, gt, timeout).then([] (std::tuple<reconcilable_result, cache_temperature> result_ht) {
                auto&& [result, ht] = result_ht;
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>(rpc::tuple(make_foreign(make_lw_shared<reconcilable_result>(std::move(result))), ht));
            });
        });
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), {pr}, std::move(trace_state), timeout);
    }
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const ::compat::one_or_two_partition_ranges& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state) {
    if (!pr.second) {
        return query_mutations_locally(std::move(s), std::move(cmd), pr.first, timeout, std::move(trace_state));
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), pr, std::move(trace_state), timeout);
    }
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_nonsingular_mutations_locally(schema_ptr s,
                                                   lw_shared_ptr<query::read_command> cmd,
                                                   const dht::partition_range_vector&& prs,
                                                   tracing::trace_state_ptr trace_state,
                                                   storage_proxy::clock_type::time_point timeout) {
    return do_with(cmd, std::move(prs), [this, timeout, s = std::move(s), trace_state = std::move(trace_state)] (lw_shared_ptr<query::read_command>& cmd,
                const dht::partition_range_vector& prs) mutable {
        return query_mutations_on_all_shards(_db, std::move(s), *cmd, prs, std::move(trace_state), timeout).then([] (std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> t) {
            return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>(std::move(t));
        });
    });
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>
storage_proxy::query_nonsingular_data_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& prs,
        query::result_options opts, tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout) {
    auto ranges = std::move(prs);
    auto local_cmd = cmd;
    rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> ret;
    if (local_cmd->slice.options.contains(query::partition_slice::option::range_scan_data_variant)) {
        ret = co_await query_data_on_all_shards(_db, std::move(s), *local_cmd, ranges, opts, std::move(trace_state), timeout);
    } else {
        auto res = co_await query_mutations_on_all_shards(_db, s, *local_cmd, ranges, std::move(trace_state), timeout);
        ret = rpc::tuple(make_foreign(make_lw_shared<query::result>(to_data_query_result(std::move(*std::get<0>(res)), std::move(s), local_cmd->slice,
                local_cmd->get_row_limit(), local_cmd->partition_limit, opts))), std::get<1>(res));
    }
    co_return ret;
}

future<> storage_proxy::start_hints_manager() {
    future<> f = make_ready_future<>();
    if (!_hints_manager.is_disabled_for_all()) {
        f = _hints_resource_manager.register_manager(_hints_manager);
    }
    return f.then([this] {
        return _hints_resource_manager.register_manager(_hints_for_views_manager);
    }).then([this] {
        return _hints_resource_manager.start(shared_from_this(), _gossiper.shared_from_this());
    });
}

void storage_proxy::allow_replaying_hints() noexcept {
    return _hints_resource_manager.allow_replaying();
}

future<> storage_proxy::change_hints_host_filter(db::hints::host_filter new_filter) {
    if (new_filter == _hints_manager.get_host_filter()) {
        return make_ready_future<>();
    }

    return _hints_directory_initializer.ensure_created_and_verified().then([this] {
        return _hints_directory_initializer.ensure_rebalanced();
    }).then([this] {
        // This function is idempotent
        return _hints_resource_manager.register_manager(_hints_manager);
    }).then([this, new_filter = std::move(new_filter)] () mutable {
        return _hints_manager.change_host_filter(std::move(new_filter));
    });
}

const db::hints::host_filter& storage_proxy::get_hints_host_filter() const {
    return _hints_manager.get_host_filter();
}

future<db::hints::sync_point> storage_proxy::create_hint_sync_point(const std::vector<gms::inet_address> target_hosts) const {
    db::hints::sync_point spoint;
    spoint.regular_per_shard_rps.resize(smp::count);
    spoint.mv_per_shard_rps.resize(smp::count);
    spoint.host_id = db::system_keyspace::get_local_host_id();
    co_await parallel_for_each(boost::irange<unsigned>(0, smp::count), [this, &target_hosts, &spoint] (unsigned shard) {
        const auto& sharded_sp = container();
        // sharded::invoke_on does not have a const-method version, so we cannot use it here
        return smp::submit_to(shard, [&sharded_sp, &target_hosts] {
            const storage_proxy& sp = sharded_sp.local();
            auto regular_rp = sp._hints_manager.calculate_current_sync_point(target_hosts);
            auto mv_rp = sp._hints_for_views_manager.calculate_current_sync_point(target_hosts);
            return std::make_pair(std::move(regular_rp), std::move(mv_rp));
        }).then([shard, &spoint] (auto p) {
            spoint.regular_per_shard_rps[shard] = std::move(p.first);
            spoint.mv_per_shard_rps[shard] = std::move(p.second);
        });
    });
    co_return spoint;
}

future<> storage_proxy::wait_for_hint_sync_point(const db::hints::sync_point spoint, clock_type::time_point deadline) {
    const utils::UUID my_host_id = db::system_keyspace::get_local_host_id();
    if (spoint.host_id != my_host_id) {
        throw std::runtime_error(format("The hint sync point was created on another node, with host ID {}. This node's host ID is {}",
                spoint.host_id, my_host_id));
    }

    std::vector<abort_source> sources;
    sources.resize(smp::count);

    // If the timer is triggered, it will spawn a discarded future which triggers
    // abort sources on all shards. We need to make sure that this future
    // completes before exiting - we use a gate for that.
    seastar::gate timer_gate;
    seastar::timer<lowres_clock> t;
    t.set_callback([&timer_gate, &sources] {
        // The gate is waited on at the end of the wait_for_hint_sync_point function
        // The gate is guaranteed to be open at this point
        (void)with_gate(timer_gate, [&sources] {
            return smp::invoke_on_all([&sources] {
                unsigned shard = this_shard_id();
                if (!sources[shard].abort_requested()) {
                    sources[shard].request_abort();
                }
            });
        });
    });
    t.arm(deadline);

    bool was_aborted = false;
    unsigned original_shard = this_shard_id();
    co_await container().invoke_on_all([this, original_shard, &sources, &spoint, &was_aborted] (storage_proxy& sp) {
        auto wait_for = [&sources, original_shard, &was_aborted] (db::hints::manager& mgr, const std::vector<db::hints::sync_point::shard_rps>& shard_rps) {
            const unsigned shard = this_shard_id();
            return mgr.wait_for_sync_point(sources[shard], shard_rps[shard]).handle_exception([original_shard, &sources, &was_aborted] (auto eptr) {
                // Make sure other blocking operations are cancelled soon
                // by requesting an abort on all shards
                return smp::invoke_on_all([&sources] {
                    unsigned shard = this_shard_id();
                    if (!sources[shard].abort_requested()) {
                        sources[shard].request_abort();
                    }
                }).then([eptr = std::move(eptr), &was_aborted, original_shard] () mutable {
                    try {
                        std::rethrow_exception(std::move(eptr));
                    } catch (abort_requested_exception&) {
                        return smp::submit_to(original_shard, [&was_aborted] { was_aborted = true; });
                    } catch (...) {
                        return make_exception_future<>(std::current_exception());
                    }
                    return make_ready_future<>();
                });
            });
        };

        return when_all_succeed(
            wait_for(sp._hints_manager, spoint.regular_per_shard_rps),
            wait_for(sp._hints_for_views_manager, spoint.mv_per_shard_rps)
        ).discard_result();
    }).finally([&t, &timer_gate] {
        t.cancel();
        return timer_gate.close();
    });

    if (was_aborted) {
        throw timed_out_error{};
    }

    co_return;
}

void storage_proxy::on_join_cluster(const gms::inet_address& endpoint) {};

void storage_proxy::on_leave_cluster(const gms::inet_address& endpoint) {
    _hints_manager.drain_for(endpoint);
    _hints_for_views_manager.drain_for(endpoint);
}

void storage_proxy::on_up(const gms::inet_address& endpoint) {};

void storage_proxy::retire_view_response_handlers(noncopyable_function<bool(const abstract_write_response_handler&)> filter_fun) {
    assert(thread::running_in_thread());
    auto it = _view_update_handlers_list->begin();
    while (it != _view_update_handlers_list->end()) {
        auto guard = it->shared_from_this();
        if (filter_fun(*it) && _response_handlers.contains(it->id())) {
            it->timeout_cb();
        }
        ++it;
        if (seastar::thread::should_yield()) {
            view_update_handlers_list::iterator_guard ig{*_view_update_handlers_list, it};
            seastar::thread::yield();
        }
    }
}

void storage_proxy::on_down(const gms::inet_address& endpoint) {
    return retire_view_response_handlers([endpoint] (const abstract_write_response_handler& handler) {
        const auto& targets = handler.get_targets();
        return boost::find(targets, endpoint) != targets.end();
    });
};

future<> storage_proxy::drain_on_shutdown() {
    //NOTE: the thread is spawned here because there are delicate lifetime issues to consider
    // and writing them down with plain futures is error-prone.
    return async([this] {
        retire_view_response_handlers([] (const abstract_write_response_handler&) { return true; });
        _hints_resource_manager.stop().get();
    });
}

future<>
storage_proxy::stop() {
    return make_ready_future<>();
}

locator::token_metadata_ptr storage_proxy::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

}
