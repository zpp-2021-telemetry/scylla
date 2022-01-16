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
 * Copyright (C) 2014-present ScyllaDB
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

#pragma once

#include "transport/messages_fwd.hh"
#include "cql3/cql_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class use_statement : public cql_statement_no_metadata {
private:
    const seastar::sstring _keyspace;

public:
    use_statement(seastar::sstring keyspace);

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on_keyspace(const seastar::sstring& ks_name) const override;

    virtual bool depends_on_column_family(const seastar::sstring& cf_name) const override;

    virtual seastar::future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options) const override;

    inline cql_statement::cql_statement_type get_statement_type() const override {
        return cql_statement::cql_statement_type::USE;
    }
};

}

}
