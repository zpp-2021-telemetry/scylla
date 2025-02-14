/*
 * Copyright 2021-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <chrono>
#include <cstdint>
#include <optional>
#include <seastar/core/sstring.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <boost/multiprecision/cpp_int.hpp>

#include "log.hh"
#include "gc_clock.hh"
#include "database.hh"
#include "service_permit.hh"
#include "timestamp.hh"
#include "service/storage_proxy.hh"
#include "service/pager/paging_state.hh"
#include "service/pager/query_pagers.hh"
#include "gms/feature_service.hh"
#include "sstables/types.hh"
#include "types.hh"
#include "types/map.hh"
#include "utils/rjson.hh"
#include "utils/big_decimal.hh"
#include "utils/fb_utilities.hh"
#include "cql3/selection/selection.hh"
#include "cql3/values.hh"
#include "cql3/query_options.hh"
#include "cql3/column_identifier.hh"
#include "alternator/executor.hh"
#include "alternator/controller.hh"
#include "alternator/serialization.hh"
#include "dht/sharder.hh"

#include "ttl.hh"

static logging::logger tlogger("alternator_ttl");

namespace alternator {

// We write the expiration-time attribute enabled on a table using a
// tag TTL_TAG_KEY.
// Currently, the *value* of this tag is simply the name of the attribute,
// and the expiration scanner interprets it as an Alternator attribute name -
// It can refer to a real column or if that doesn't exist, to a member of
// the ":attrs" map column. Although this is designed for Alternator, it may
// be good enough for CQL as well (there, the ":attrs" column won't exist).
static const sstring TTL_TAG_KEY("system:ttl_attribute");

future<executor::request_return_type> executor::update_time_to_live(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_time_to_live++;
    if (!_proxy.get_db().local().features().cluster_supports_alternator_ttl()) {
        co_return api_error::unknown_operation("UpdateTimeToLive not yet supported. Experimental support is available if the 'alternator_ttl' experimental feature is enabled on all nodes.");
    }

    schema_ptr schema = get_table(_proxy, request);
    rjson::value* spec = rjson::find(request, "TimeToLiveSpecification");
    if (!spec || !spec->IsObject()) {
        co_return api_error::validation("UpdateTimeToLive missing mandatory TimeToLiveSpecification");
    }
    const rjson::value* v = rjson::find(*spec, "Enabled");
    if (!v || !v->IsBool()) {
        co_return api_error::validation("UpdateTimeToLive requires boolean Enabled");
    }
    bool enabled = v->GetBool();
    v = rjson::find(*spec, "AttributeName");
    if (!v || !v->IsString()) {
        co_return api_error::validation("UpdateTimeToLive requires string AttributeName");
    }
    // Although the DynamoDB documentation specifies that attribute names
    // should be between 1 and 64K bytes, in practice, it only allows
    // between 1 and 255 bytes. There are no other limitations on which
    // characters are allowed in the name.
    if (v->GetStringLength() < 1 || v->GetStringLength() > 255) {
        co_return api_error::validation("The length of AttributeName must be between 1 and 255");
    }
    sstring attribute_name(v->GetString(), v->GetStringLength());

    std::map<sstring, sstring> tags_map = get_tags_of_table(schema);
    if (enabled) {
        if (tags_map.contains(TTL_TAG_KEY)) {
            co_return api_error::validation("TTL is already enabled");
        }
        tags_map[TTL_TAG_KEY] = attribute_name;
    } else {
        auto i = tags_map.find(TTL_TAG_KEY);
        if (i == tags_map.end()) {
            co_return api_error::validation("TTL is already disabled");
        } else if (i->second != attribute_name) {
            co_return api_error::validation(format(
                "Requested to disable TTL on attribute {}, but a different attribute {} is enabled.",
                attribute_name, i->second));
        }
        tags_map.erase(TTL_TAG_KEY);
    }
    co_await update_tags(_mm, schema, std::move(tags_map));
    // Prepare the response, which contains a TimeToLiveSpecification
    // basically identical to the request's
    rjson::value response = rjson::empty_object();
    rjson::add(response, "TimeToLiveSpecification", std::move(*spec));
    co_return make_jsonable(std::move(response));
}

future<executor::request_return_type> executor::describe_time_to_live(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_time_to_live++;
    if (!_proxy.get_db().local().features().cluster_supports_alternator_ttl()) {
        co_return api_error::unknown_operation("DescribeTimeToLive not yet supported. Experimental support is available if the 'alternator_ttl' experimental feature is enabled on all nodes.");
    }
    schema_ptr schema = get_table(_proxy, request);
    std::map<sstring, sstring> tags_map = get_tags_of_table(schema);
    rjson::value desc = rjson::empty_object();
    auto i = tags_map.find(TTL_TAG_KEY);
    if (i == tags_map.end()) {
        rjson::add(desc, "TimeToLiveStatus", "DISABLED");
    } else {
        rjson::add(desc, "TimeToLiveStatus", "ENABLED");
        rjson::add(desc, "AttributeName", rjson::from_string(i->second));
    }
    rjson::value response = rjson::empty_object();
    rjson::add(response, "TimeToLiveDescription", std::move(desc));
    co_return make_jsonable(std::move(response));
}

// expiration_service is a sharded service responsible for cleaning up expired
// items in all tables with per-item expiration enabled. Currently, this means
// Alternator tables with TTL configured via a UpdateTimeToLeave request.
//
// Here is a brief overview of how the expiration service works:
//
// An expiration thread on each shard periodically scans the items (i.e.,
// rows) owned by this shard, looking for items whose chosen expiration-time
// attribute indicates they are expired, and deletes those items.
// The expiration-time "attribute" can be either an actual Scylla column
// (must be numeric) or an Alternator "attribute" - i.e., an element in
// the ATTRS_COLUMN_NAME map<utf8,bytes> column where the numeric expiration
// time is encoded in DynamoDB's JSON encoding inside the bytes value.
// To avoid scanning the same items RF times in RF replicas, only one node is
// responsible for scanning a token range at a time. Normally, this is the
// node owning this range as a "primary range" (the first node in the ring
// with this range), but when this node is down, other nodes may take over
// (FIXME: this is not implemented yet).
// An expiration thread is reponsible for all tables which need expiration
// scans. FIXME: explain how this is done with multiple tables - parallel,
// staggered, or what?
// The expiration thread scans item using CL=QUORUM to ensures that it reads
// a consistent expiration-time attribute. This means that the items are read
// locally and in addition QUORUM-1 additional nodes (one additional node
// when RF=3) need to read the data and send digests.
// FIXME: explain if we can read the exact attribute or the entire map.
// When the expiration thread decides that an item has expired and wants
// to delete it, it does it using a CL=QUORUM write. This allows this
// deletion to be visible for consistent (quorum) reads. The deletion,
// like user deletions, will also appear on the CDC log and therefore
// Alternator Streams if enabled (FIXME: explain how we mark the
// deletion different from user deletes. We don't do it yet.).
expiration_service::expiration_service(database& db, service::storage_proxy& proxy)
        : _db(db)
        , _proxy(proxy)
{
    //FIXME: add metrics for the service
    //setup_metrics();
}

// Convert the big_decimal used to represent expiration time to an integer.
// Any fractional part is dropped. If the number is negative or invalid,
// 0 is returned, and if it's too high, the maximum unsigned long is returned.
static unsigned long bigdecimal_to_ul(const big_decimal& bd) {
    // The big_decimal format has an integer mantissa of arbitrary length
    // "unscaled_value" and then a (power of 10) exponent "scale".
    if (bd.unscaled_value() <= 0) {
        return 0;
    }
    if (bd.scale() == 0) {
        // The fast path, when the expiration time is an integer, scale==0.
        return static_cast<unsigned long>(bd.unscaled_value());
    }
    // Because the mantissa can be of arbitrary length, we work on it
    // as a string. TODO: find a less ugly algorithm.
    auto str = bd.unscaled_value().str();
    if (bd.scale() > 0) {
        int len = str.length();
        if (len < bd.scale()) {
            return 0;
        }
        str = str.substr(0, len-bd.scale());
    } else {
        if (bd.scale() < -20) {
            return std::numeric_limits<unsigned long>::max();
        }
        for (int i = 0; i < -bd.scale(); i++) {
            str.push_back('0');
        }
    }
    // strtoul() returns ULONG_MAX if the number is too large, or 0 if not
    // a number.
    return strtoul(str.c_str(), nullptr, 10);
}

// The following is_expired() functions all check if an item with the given
// expiration time has expired, according to the DynamoDB API rules.
// The rules are:
// 1. If the expiration time attribute's value is not a number type,
//    the item is not expired.
// 2. The expiration time is measured in seconds since the UNIX epoch.
// 3. If the expiration time is more than 5 years in the past, it is assumed
//    to be malformed and ignored - and the item does not expire.
static bool is_expired(gc_clock::time_point expiration_time, gc_clock::time_point now) {
    return expiration_time <= now &&
           expiration_time > now - std::chrono::years(5);
}

static bool is_expired(const big_decimal& expiration_time, gc_clock::time_point now) {
    unsigned long t = bigdecimal_to_ul(expiration_time);
    // We assume - and the assumption turns out to be correct - that the
    // epoch of gc_clock::time_point and the one used by the DynamoDB protocol
    // are the same (the UNIX epoch in UTC). The resolution (seconds) is also
    // the same.
    return is_expired(gc_clock::time_point(gc_clock::duration(std::chrono::seconds(t))), now);
}
static bool is_expired(const rjson::value& expiration_time, gc_clock::time_point now) {
    std::optional<big_decimal> n = try_unwrap_number(expiration_time);
    return n && is_expired(*n, now);
}

// expire_item() expires an item - i.e., deletes it as appropriate for
// expiration - with CL=QUORUM and (FIXME!) in a way Alternator Streams
// understands it is an expiration event - not a user-initiated deletion.
static future<> expire_item(service::storage_proxy& proxy,
                            const service::query_state& qs,
                            const std::vector<bytes_opt>& row,
                            schema_ptr schema,
                            api::timestamp_type ts) {
    // Prepare the row key to delete
    // NOTICE: the order of columns is guaranteed by the fact that selection::wildcard
    // is used, which indicates that columns appear in the order defined by
    // schema::all_columns_in_select_order() - partition key columns goes first,
    // immediately followed by clustering key columns
    std::vector<bytes> exploded_pk;
    const unsigned pk_size = schema->partition_key_size();
    const unsigned ck_size = schema->clustering_key_size();
    for (unsigned c = 0; c < pk_size; ++c) {
        const auto& row_c = row[c];
        if (!row_c) {
            // This shouldn't happen - all key columns must have values.
            // But if it ever happens, let's just *not* expire the item.
            // FIXME: log or increment a metric if this happens.
            return make_ready_future<>();
        }
        exploded_pk.push_back(*row_c);
    }
    auto pk = partition_key::from_exploded(exploded_pk);
    mutation m(schema, pk);
    // If there's no clustering key, a tombstone should be created directly
    // on a partition, not on a clustering row - otherwise it will look like
    // an open-ended range tombstone, which will crash on KA/LA sstable format.
    // See issue #6035
    if (ck_size == 0) {
        m.partition().apply(tombstone(ts, gc_clock::now()));
    } else {
        std::vector<bytes> exploded_ck;
        for (unsigned c = pk_size; c < pk_size + ck_size; ++c) {
            const auto& row_c = row[c];
            if (!row_c) {
                // This shouldn't happen - all key columns must have values.
                // But if it ever happens, let's just *not* expire the item.
                // FIXME: log or increment a metric if this happens.
                return make_ready_future<>();
            }
            exploded_ck.push_back(*row_c);
        }
        auto ck = clustering_key::from_exploded(exploded_ck);
        m.partition().clustered_row(*schema, ck).apply(tombstone(ts, gc_clock::now()));
    }
    return proxy.mutate(std::vector<mutation>{std::move(m)},
        db::consistency_level::LOCAL_QUORUM,
        executor::default_timeout(), // FIXME - which timeout?
        qs.get_trace_state(), qs.get_permit());
}

// A class for iterating over all the token ranges *owned* by this shard.
// We consider a token *owned* by this shard if:
// 1. This node is a replica for this token.
// 2. Moreover, this node is the *primary* replica of the token (i.e., the
//    first replica in the ring).
// 3. In this node, this shard is responsible for this token.
// We will use this definition of which shard in the cluster owns which tokens
// to split the expiration scanner's work between all the shards of the
// system.
// When some of the nodes in the system are down, nobody will be expiring
// tokens owned by them, so we will need to separately detect when this
// happens and let living nodes take over the work that a dead node cannot
// do.
// FIXME: need to decide how to choose primary ranges in multi-DC setup!
// We could call get_primary_ranges_within_dc() below instead of get_primary_ranges().
class token_ranges_owned_by_this_shard {
    schema_ptr _s;
    // _token_ranges will contain a list of token ranges owned by this node.
    // We'll further need to split each such range to the pieces owned by
    // the current shard, using _intersecter.
    dht::token_range_vector _token_ranges;
    dht::token_range_vector::const_iterator _range_it;
    std::optional<dht::selective_token_range_sharder> _intersecter;
public:
    // FIXME: add a constructor to start iteration in the middle, to continue
    // a scan after reboot. A simpler option is to begin the scan at a random
    // position.
    token_ranges_owned_by_this_shard(database& db, schema_ptr s)
        :  _s(s)
        , _token_ranges(db.find_keyspace(s->ks_name()).
            get_effective_replication_map()->get_primary_ranges(
            utils::fb_utilities::get_broadcast_address()))
        , _range_it(_token_ranges.cbegin())
    {}

    // Return the next token_range owned by this shard, or nullopt when the
    // iteration ends.
    std::optional<dht::token_range> next() {
        // We may need three or more iterations in the following loop if a
        // vnode doesn't intersect with the given shard at all (such a small
        // vnode is unlikely, but possible). The loop cannot be infinite
        // because each iteration of the loop advances _range_it.
        for (;;) {
            if (_intersecter) {
                std::optional<dht::token_range> ret = _intersecter->next();
                if (ret) {
                    return ret;
                }
                // done with this range, go to next one
                ++_range_it;
                _intersecter = std::nullopt;
            }
            if (_range_it == _token_ranges.cend()) {
                return std::nullopt;
            }
            _intersecter.emplace(_s->get_sharder(), *_range_it, this_shard_id());
        }
    }

    // Same as next(), just return a partition_range instead of token_range
    std::optional<dht::partition_range> next_partition_range() {
        std::optional<dht::token_range> ret = next();
        if (ret) {
            return dht::to_partition_range(*ret);
        } else {
            return std::nullopt;
        }
    }
};

// Precomputed information needed to perform a scan on partition ranges
struct scan_ranges_context {
    schema_ptr s;
    bytes column_name;
    std::optional<std::string> member;

    ::shared_ptr<cql3::selection::selection> selection;
    std::unique_ptr<service::query_state> query_state_ptr;
    std::unique_ptr<cql3::query_options> query_options;
    ::lw_shared_ptr<query::read_command> command;

    scan_ranges_context(schema_ptr s, service::storage_proxy& proxy, bytes column_name, std::optional<std::string> member)
        : s(s)
        , column_name(column_name)
        , member(member)
    {
        // FIXME: don't read the entire items - read only parts of it.
        // We must read the key columns (to be able to delete) and also
        // the requested attribute. If the requested attribute is a map's
        // member we may be forced to read the entire map - but it would
        // be good if we can read only the single item of the map - it
        // should be possible (and a must for issue #7751!).
        lw_shared_ptr<service::pager::paging_state> paging_state = nullptr;
        auto regular_columns = boost::copy_range<query::column_id_vector>(
            s->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
        selection = cql3::selection::selection::wildcard(s);
        query::partition_slice::option_set opts = selection->get_query_options();
        opts.set<query::partition_slice::option::allow_short_read>();
        std::vector<query::clustering_range> ck_bounds{query::clustering_range::make_open_ended_both_sides()};
        auto partition_slice = query::partition_slice(std::move(ck_bounds), {}, std::move(regular_columns), opts);
        command = ::make_lw_shared<query::read_command>(s->id(), s->version(), partition_slice, proxy.get_max_result_size(partition_slice));
        executor::client_state client_state{executor::client_state::internal_tag()};
        tracing::trace_state_ptr trace_state;
        // FIXME: is empty_service_permit() the right thing?
        // view builder has _permit = _db.get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "view_builder", db::no_timeout))
        query_state_ptr = std::make_unique<service::query_state>(client_state, trace_state, empty_service_permit());
        // FIXME: What should we do on multi-DC? Will we run the expiration on the same ranges on all
        // DCs or only once for each range? If the latter, we need to change the CLs in the
        // scanner and deleter.
        db::consistency_level cl = db::consistency_level::LOCAL_QUORUM;
        query_options = std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>{});
        query_options = std::make_unique<cql3::query_options>(std::move(query_options), std::move(paging_state));
    }
};

// Scan data in a list of token ranges in one table, looking for expired
// items and deleting them.
// Because of issue #9167, partition_ranges must have a single partition
// for this code to work correctly.
static future<> scan_table_ranges(
        service::storage_proxy& proxy,
        const scan_ranges_context& scan_ctx,
        dht::partition_range_vector&& partition_ranges,
        abort_source& abort_source)
{
    const schema_ptr& s = scan_ctx.s;
    assert (partition_ranges.size() == 1); // otherwise issue #9167 will cause incorrect results.
    auto p = service::pager::query_pagers::pager(s, scan_ctx.selection, *scan_ctx.query_state_ptr,
            *scan_ctx.query_options, scan_ctx.command, std::move(partition_ranges), nullptr);
    while (!p->is_exhausted()) {
        if (abort_source.abort_requested()) {
            co_return;
        }
        // We don't to limit page size in number of rows because there is a
        // builtin limit of the page's size in bytes. Setting this limit to 1
        // is useful for debugging the paging code with moderate-size data.
        uint32_t limit = std::numeric_limits<uint32_t>::max();
        // FIXME: which timeout?
        // FIXME: if read times out, need to retry it.
        std::unique_ptr<cql3::result_set> rs = co_await p->fetch_page(limit, gc_clock::now(), executor::default_timeout());
        auto rows = rs->rows();
        auto meta = rs->get_metadata().get_names();
        std::optional<unsigned> expiration_column;
        for (unsigned i = 0; i < meta.size(); i++) {
            const cql3::column_specification& col = *meta[i];
            if (col.name->name() == scan_ctx.column_name) {
                expiration_column = i;
                break;
            }
        }
        if (!expiration_column) {
            continue;
        }
        for (const auto& row : rows) {
            const bytes_opt& cell = row[*expiration_column];
            if (!cell) {
                continue;
            }
            auto v = meta[*expiration_column]->type->deserialize(*cell);
            bool expired = false;
            // FIXME: don't recalculate "now" all the time
            auto now = gc_clock::now();
            if (scan_ctx.member) {
                // In this case, the expiration-time attribute we're
                // looking for is a member in a map, saved serialized
                // into bytes using Alternator's serialization (basically
                // a JSON serialized into bytes)
                // FIXME: is it possible to find a specific member of a map
                // without iterating through it like we do here and compare
                // the key?
                for (const auto& entry : value_cast<map_type_impl::native_type>(v)) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (value_cast<sstring>(entry.first) == *scan_ctx.member) {
                        bytes value = value_cast<bytes>(entry.second);
                        rjson::value json = deserialize_item(value);
                        expired = is_expired(json, now);
                        break;
                    }
                }
            } else {
                // For a real column to contain an expiration time, it
                // must be a numeric type.
                // FIXME: Currently we only support decimal_type (which is
                // what Alternator uses), but other numeric types can be
                // supported as well to make this feature more useful in CQL.
                // Note that kind::decimal is also checked above.
                big_decimal n = value_cast<big_decimal>(v);
                expired = is_expired(n, now);
            }
            if (expired) {
                // FIXME: maybe don't recalculate new_timestamp() all the time
                // FIXME: if expire_item() throws on timeout, we need to retry it.
                auto ts = api::new_timestamp();
                co_await expire_item(proxy, *scan_ctx.query_state_ptr, row, s, ts);
            }
        }
        // FIXME: once in a while, persist p->state(), so on reboot
        // we don't start from scratch.
    }
}

// scan_table() scans data in one table "owned" by this shard, looking for
// expired items and deleting them.
// We consider each node to "own" its primary token ranges, i.e., the tokens
// that this node is their first replica in the ring. Inside the node, each
// shard "owns" subranges of the node's token ranges - according to the node's
// sharding algorithm.
// When a node goes down, the token ranges owned by it will not be scanned
// and items in those token ranges will not expire, so in the future (FIXME)
// this function should additionally work on token ranges whose primary owner
// is down and this node is the range's secondary owner.
// If the TTL (expiration-time scanning) feature is not enabled for this
// table, scan_table() returns false without doing anything. Remember that the
// TTL feature may be enabled later so this function will need to be called
// again when the feature is enabled.
// Currently this function scans the entire table (or, rather the parts owned
// by this shard) at full rate, once. In the future (FIXME) we should consider
// how to pace this scan, how and when to repeat it, how to interleave or
// parallelize scanning of multiple tables, and how to continue scans after a
// reboot.
static future<bool> scan_table(
    service::storage_proxy& proxy,
    database& db,
    schema_ptr s,
    abort_source& abort_source)
{
    // Check if an expiration-time attribute is enabled for this table.
    // If not, just return false immediately.
    std::optional<std::string> attribute_name = find_tag(*s, TTL_TAG_KEY);
    if (!attribute_name) {
        co_return false;
    }
    // attribute_name may be one of the schema's columns (in Alternator, this
    // means it's a key column), or an element in Alternator's attrs map
    // encoded in Alternator's JSON encoding.
    // FIXME: To make this less Alternators-specific, we should encode in the
    // single key's value three things:
    // 1. The name of a column
    // 2. Optionally if column is a map, a member in the map
    // 3. The deserializer for the value: CQL or Alternator (JSON).
    // The deserializer can be guessed: If the given column or map item is
    // numeric, it can be used directly. If it is a "bytes" type, it needs to
    // be deserialized using Alternator's deserializer.
    bytes column_name = to_bytes(*attribute_name);
    const column_definition *cd = s->get_column_definition(column_name);
    std::optional<std::string> member;
    if (!cd) {
        member = std::move(attribute_name);
        column_name = bytes(executor::ATTRS_COLUMN_NAME);
        cd = s->get_column_definition(column_name);
        tlogger.info("table {} TTL enabled with attribute {} in {}", s->cf_name(), *member, executor::ATTRS_COLUMN_NAME);
    } else {
        tlogger.info("table {} TTL enabled with attribute {}", s->cf_name(), *attribute_name);
    }
    if (!cd) {
        tlogger.info("table {} TTL column is missing, not scanning", s->cf_name());
        co_return false;
    }
    data_type column_type = cd->type;
    // Verify that the column has the right type: If "member" exists
    // the column must be a map, and if it doesn't, the column must
    // (currently) be a decimal_type. If the column has the wrong type
    // nothing can get expired in this table, and it's pointless to
    // scan it.
    if ((member && column_type->get_kind() != abstract_type::kind::map) ||
        (!member && column_type->get_kind() != abstract_type::kind::decimal)) {
        tlogger.info("table {} TTL column has unsupported type, not scanning", s->cf_name());
        co_return false;
    }
    // FIXME: need to persist position in the scan, and start from it instead
    // of the beginning. Alternatively/additionally, can scan from a random
    // position.
    // FIXME: need to pace the scan, not do it all at once.
    // FIXME: consider if we should ask the scan without caching?
    // can we use cache but not fill it?
    scan_ranges_context scan_ctx{s, proxy, std::move(column_name), std::move(member)};
    token_ranges_owned_by_this_shard my_ranges(db, s);
    while (std::optional<dht::partition_range> range = my_ranges.next_partition_range()) {
        // Note that because of issue #9167 we need to run a separate
        // query on each partition range, and can't pass several of
        // them into one partition_range_vector.
        dht::partition_range_vector partition_ranges = {*range};
        // FIXME: if scanning a single range fails, including network errors,
        // we fail the entire scan (and rescan from the beginning). Need to
        // reconsider this. Saving the scan position might be a good enough
        // solution for this problem.
        co_await scan_table_ranges(proxy, scan_ctx, std::move(partition_ranges), abort_source);
    }
    co_return true;
}


future<> expiration_service::run() {
    // FIXME: don't just tight-loop, think about timing, pace, and
    // store position in durable storage, etc.
    // FIXME: think about working on different tables in parallel.
    // also need to notice when a new table is added, a table is
    // deleted or when ttl is enabled or disabled for a table!
    for (;;) {
        // _db.get_column_families() may change under our feet during a
        // long-living loop, so we must keep our own copy of the list of
        // schemas.
        std::vector<schema_ptr> schemas;
        for (const auto& cf : _db.get_column_families()) {
            schemas.push_back(cf.second->schema());
        }
        for (schema_ptr s : schemas) {
            co_await coroutine::maybe_yield();
            if (shutting_down()) {
                co_return;
            }
            try {
                co_await scan_table(_proxy, _db, s, _abort_source);
            } catch (...) {
                // The scan of a table may fail in the middle for many
                // reasons, including network failure and even the table
                // being removed. We'll continue scanning this table later
                // (if it still exists). In any case it's important to catch
                // the exception and not let the scanning service die for
                // good.
                // If the table has been deleted, it is expected that the scan
                // will fail at some point, and even a warning is excessive.
                if (_db.has_schema(s->ks_name(), s->cf_name())) {
                    tlogger.warn("table {}.{} expiration scan failed: {}",
                        s->ks_name(), s->cf_name(), std::current_exception());
                } else {
                    tlogger.info("expiration scan failed when table {}.{} was deleted",
                        s->ks_name(), s->cf_name());
                }
            }
        }
        // FIXME: replace this silly 1-second sleep by something smarter.
        try {
            co_await seastar::sleep_abortable(std::chrono::seconds(1), _abort_source);
        } catch(seastar::sleep_aborted&) {}
    }
}

future<> expiration_service::start() {
    // Called by main() on each shard to start the expiration-service
    // thread. Just runs run() in the background and allows stop().
    if (_db.features().cluster_supports_alternator_ttl()) {
        if (!shutting_down()) {
            _end = run().handle_exception([] (std::exception_ptr ep) {
                tlogger.error("expiration_service failed: {}", ep);
            });
        }
    }
    return make_ready_future<>();
}

future<> expiration_service::stop() {
    if (_abort_source.abort_requested()) {
        throw std::logic_error("expiration_service::stop() called a second time");
    }
    _abort_source.request_abort();
    if (!_end) {
        // if _end is was not set, start() was never called
        return make_ready_future<>();
    }
    return std::move(*_end);
}

} // namespace alternator
