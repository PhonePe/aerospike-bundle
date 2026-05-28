/**
 * Copyright (c) 2026 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.aerospike.interceptors;

import lombok.Getter;

@Getter
public enum AerospikeOperation {
    WRITE("write"),
    WRITE_ASYNC("write"),
    SCAN("scan"),
    APPEND("append"),
    READ("read"),
    READ_ASYNC("read"),
    EXISTS("exists"),
    BATCH_EXISTS("batch-exists"),
    BATCH_READ("batch-read"),
    PREPEND("prepend"),
    BATCH_GET("batch-get"),
    ADD("add"),
    DELETE("delete"),
    BATCH_DELETE("batch-delete"),
    GET_HEADER("get-header"),
    TRUNCATE("truncate"),
    TOUCH("touch"),
    OPERATE("operate"),
    BATCH_OPERATE("batch-operate"),
    CREATE_INDEX("create-index"),
    QUERY_AGGREGATE("query-aggregate"),
    QUERY("query"),
    QUERY_PARTITIONS("query-partitions"),
    EXECUTE("execute"),
    BATCH_EXECUTE("batch-execute"),
    SCAN_NODE("scanNode"),
    QUERY_AGGREGATE_NODE("queryAggregateNode"),
    QUERY_NODE("queryNode"),
    SCAN_PARTITIONS("scanPartitions"),
    REGISTER("register"),
    CREATE_ROLE("createRole"),
    DROP_ROLE("dropRole"),
    REVOKE_ROLES("revokeRoles"),
    GRANT_ROLES("grantRoles"),
    CHANGE_PASSWORD("changePassword"),
    QUERY_ROLE("queryRole"),
    CREATE_USER("createUser"),
    DROP_USER("dropUser"),
    QUERY_USERS("queryUsers"),
    SET_QUOTAS("setQuotas"),
    SET_WHITELIST("setWhitelist"),
    SET_XDR_FILTER("setXDRFilter"),
    INFO("info"),
    REVOKE_PRIVILEGES("revokePrivileges"),
    GRANT_PRIVILEGES("grantPrivileges"),
    DROP_INDEX("dropIndex"),
    REMOVE_UDF("removeUdf"),
    REGISTER_UDF_STRING("registerUdfString")
    ;

    private final String name;

    AerospikeOperation(final String name) {
        this.name = name;
    }
}

