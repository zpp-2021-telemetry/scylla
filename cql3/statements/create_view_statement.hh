/*
 * This file is part of Scylla.
 * Copyright (C) 2016-present ScyllaDB
 *
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

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_properties.hh"
#include "cql3/cf_name.hh"

#include <seastar/core/shared_ptr.hh>

#include <utility>
#include <vector>
#include <optional>

namespace cql3 {

class query_processor;
class relation;

namespace selection {
    class raw_selector;
} // namespace selection

namespace statements {

/** A <code>CREATE MATERIALIZED VIEW</code> parsed from a CQL query statement. */
class create_view_statement : public schema_altering_statement {
private:
    // Mutable because announce_migration() modifies it while marked as const
    mutable cf_name _base_name;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    std::vector<::shared_ptr<relation>> _where_clause;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _partition_keys;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _clustering_keys;
    cf_properties _properties;
    bool _if_not_exists;

public:
    create_view_statement(
            cf_name view_name,
            cf_name base_name,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            std::vector<::shared_ptr<relation>> where_clause,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> clustering_keys,
            bool if_not_exists);

    auto& properties() {
        return _properties;
    }

    // Functions we need to override to subclass schema_altering_statement
    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(query_processor& qp) const override;
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    inline cql_statement::cql_statement_type get_statement_type() const override {
        return cql_statement::cql_statement_type::CREATE_VIEW;
    }
    // FIXME: continue here. See create_table_statement.hh and CreateViewStatement.java
};

}
}
