
/*
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace cql_transport {
namespace messages {

class result_message {
    std::vector<sstring> _warnings;
    std::map<sstring, bytes> _custom_payload;
public:
    class visitor;
    class visitor_base;

    virtual ~result_message() {}

    virtual void accept(visitor&) const = 0;

    void add_warning(sstring w) {
        _warnings.push_back(std::move(w));
    }

    const std::vector<sstring>& warnings() const {
        return _warnings;
    }

    void add_to_custom_payload(sstring key, bytes val) {
        _custom_payload[key] = val;
    }

    const std::map<sstring, bytes>& custom_payload() const {
        return _custom_payload;
    }

    virtual std::optional<unsigned> move_to_shard() const {
        return std::nullopt;
    }
    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class prepared;
    class schema_change;
    class rows;
    class bounce_to_shard;
};

std::ostream& operator<<(std::ostream& os, const result_message& msg);

}
}
