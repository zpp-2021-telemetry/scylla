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

#include "timeout_config.hh"

namespace service {

class storage_proxy;
class query_state;
class client_state;

}

namespace cql_transport {

namespace messages {

class result_message;

}

}

namespace cql3 {

class query_processor;

class metadata;
seastar::shared_ptr<const metadata> make_empty_metadata();

class query_options;

class cql_statement {
    timeout_config_selector _timeout_config_selector;
public:
    // CQL statement text
    seastar::sstring raw_cql_statement;

    explicit cql_statement(timeout_config_selector timeout_selector) : _timeout_config_selector(timeout_selector) {}

    virtual ~cql_statement()
    { }

    timeout_config_selector get_timeout_config_selector() const { return _timeout_config_selector; }

    virtual uint32_t get_bound_terms() const = 0;

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    virtual seastar::future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const = 0;

    /**
     * Perform additional validation required by the statment.
     * To be overriden by subclasses if needed.
     *
     * @param state the current client state
     */
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) const = 0;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
        execute(query_processor& qp, service::query_state& state, const query_options& options) const = 0;

    virtual bool depends_on_keyspace(const seastar::sstring& ks_name) const = 0;

    virtual bool depends_on_column_family(const seastar::sstring& cf_name) const = 0;

    virtual seastar::shared_ptr<const metadata> get_result_metadata() const = 0;

    virtual bool is_conditional() const {
        return false;
    }

    enum class cql_statement_type {
        ALTER_ROLE,
        CREATE_ROLE,
        DROP_ROLE,
        LIST_USERS,
        GRANT_ROLE,
        LIST_PERMISSIONS,
        LIST_ROLES,
        GRANT,
        REVOKE,
        REVOKE_ROLE,
        ALTER_KEYSPACE,
        ALTER_TABLE,
        ALTER_TYPE,
        ALTER_VIEW,
        CREATE_INDEX,
        CREATE_KEYSPACE,
        CREATE_TABLE,
        CREATE_TYPE,
        CREATE_VIEW,
        DROP_INDEX,
        DROP_KEYSPACE,
        DROP_TABLE,
        DROP_TYPE,
        DROP_VIEW,
        CREATE_FUNCTION,
        CREATE_AGGREGATE,
        DROP_FUNCTION,
        DROP_AGGREGATE,
        ALTER_SERVICE_LEVEL,
        ATTACH_SERVICE_LEVEL,
        CREATE_SERVICE_LEVEL,
        DETACH_SERVICE_LEVEL,
        DROP_SERVICE_LEVEL,
        LIST_SERVICE_LEVEL_ATTACHMENTS,
        LIST_SERVICE_LEVEL,
        TRUNCATE,
        USE,
        PRIMARY_KEY_SELECT,
        INDEXED_TABLE_SELECT,
        UPDATE,
        DELETE,
        BATCH
    };

    static seastar::sstring cql_statement_type_name(cql_statement_type st) {
        switch (st) {
            case cql_statement_type::ALTER_ROLE: return "ALTER_ROLE";
            case cql_statement_type::CREATE_ROLE: return "CREATE_ROLE";
            case cql_statement_type::DROP_ROLE: return "DROP_ROLE";
            case cql_statement_type::LIST_USERS: return "LIST_USERS";
            case cql_statement_type::GRANT_ROLE: return "GRANT_ROLE";
            case cql_statement_type::LIST_PERMISSIONS: return "LIST_PERMISSIONS";
            case cql_statement_type::LIST_ROLES: return "LIST_ROLES";
            case cql_statement_type::GRANT: return "GRANT";
            case cql_statement_type::REVOKE: return "REVOKE";
            case cql_statement_type::REVOKE_ROLE: return "REVOKE_ROLE";
            case cql_statement_type::ALTER_KEYSPACE: return "ALTER_KEYSPACE";
            case cql_statement_type::ALTER_TABLE: return "ALTER_TABLE";
            case cql_statement_type::ALTER_TYPE: return "ALTER_TYPE";
            case cql_statement_type::ALTER_VIEW: return "ALTER_VIEW";
            case cql_statement_type::CREATE_INDEX: return "CREATE_INDEX";
            case cql_statement_type::CREATE_KEYSPACE: return "CREATE_KEYSPACE";
            case cql_statement_type::CREATE_TABLE: return "CREATE_TABLE";
            case cql_statement_type::CREATE_TYPE: return "CREATE_TYPE";
            case cql_statement_type::CREATE_VIEW: return "CREATE_VIEW";
            case cql_statement_type::DROP_INDEX: return "DROP_INDEX";
            case cql_statement_type::DROP_KEYSPACE: return "DROP_KEYSPACE";
            case cql_statement_type::DROP_TABLE: return "DROP_TABLE";
            case cql_statement_type::DROP_TYPE: return "DROP_TYPE";
            case cql_statement_type::DROP_VIEW: return "DROP_VIEW";
            case cql_statement_type::CREATE_FUNCTION: return "CREATE_FUNCTION";
            case cql_statement_type::CREATE_AGGREGATE: return "CREATE_AGGREGATE";
            case cql_statement_type::DROP_FUNCTION: return "DROP_FUNCTION";
            case cql_statement_type::DROP_AGGREGATE: return "DROP_AGGREGATE";
            case cql_statement_type::ALTER_SERVICE_LEVEL: return "ALTER_SERVICE_LEVEL";
            case cql_statement_type::ATTACH_SERVICE_LEVEL: return "ATTACH_SERVICE_LEVEL";
            case cql_statement_type::CREATE_SERVICE_LEVEL: return "CREATE_SERVICE_LEVEL";
            case cql_statement_type::DETACH_SERVICE_LEVEL: return "DETACH_SERVICE_LEVEL";
            case cql_statement_type::DROP_SERVICE_LEVEL: return "DROP_SERVICE_LEVEL";
            case cql_statement_type::LIST_SERVICE_LEVEL_ATTACHMENTS: return "LIST_SERVICE_LEVEL_ATTACHMENTS";
            case cql_statement_type::LIST_SERVICE_LEVEL: return "LIST_SERVICE_LEVEL";
            case cql_statement_type::TRUNCATE: return "TRUNCATE";
            case cql_statement_type::USE: return "USE";
            case cql_statement_type::PRIMARY_KEY_SELECT: return "PRIMARY_KEY_SELECT";
            case cql_statement_type::INDEXED_TABLE_SELECT: return "INDEXED_TABLE_SELECT";
            case cql_statement_type::UPDATE: return "UPDATE";
            case cql_statement_type::DELETE: return "DELETE";
            case cql_statement_type::BATCH: return "BATCH";
        }
    }

    virtual cql_statement_type get_statement_type() const = 0;
};

class cql_statement_no_metadata : public cql_statement {
public:
    using cql_statement::cql_statement;
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override {
        return make_empty_metadata();
    }
};

// Conditional modification statements and batches
// return a result set and have metadata, while same
// statements without conditions do not.
class cql_statement_opt_metadata : public cql_statement {
protected:
    // Result set metadata, may be empty for simple updates and batches
    seastar::shared_ptr<metadata> _metadata;
public:
    using cql_statement::cql_statement;
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override {
        if (_metadata) {
            return _metadata;
        }
        return make_empty_metadata();
    }
};

}
