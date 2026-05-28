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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.Statement;
import lombok.Builder;
import lombok.val;

import java.util.Arrays;

@lombok.Value
@Builder
public class AerospikeInterceptorContext {
    String namespace;
    String setName;
    AerospikeOperation operation;
    AerospikeRequestInfo requestInfo;

    public static AerospikeInterceptorContext getInterceptorContext(final AerospikeOperation operation) {
        return getInterceptorContext(null, null, operation);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final Key key,
                                                                    final AerospikeOperation operation) {
        return getInterceptorContext(key.namespace, key.setName, operation, getKeys(key));
    }

    public static AerospikeInterceptorContext getInterceptorContext(final Key key,
                                                                    final AerospikeOperation operation,
                                                                    final Bin... bins) {
        return getInterceptorContext(key.namespace, key.setName, operation, getKeys(key), bins);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final Statement statement,
                                                                    final AerospikeOperation operation) {
        return getInterceptorContext(statement.getNamespace(), statement.getSetName(), operation);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final Key[] keys,
                                                                    final AerospikeOperation operation) {
        if (keys.length > 0) {
            return getInterceptorContext(keys[0].namespace, keys[0].setName, operation, getKeys(keys));
        }
        return getInterceptorContext(operation);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final String namespace,
                                                                    final String setName,
                                                                    final AerospikeOperation operation) {
        return getInterceptorContext(namespace, setName, operation, null, null);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final String namespace,
                                                                    final String setName,
                                                                    final AerospikeOperation operation,
                                                                    final String[] keys) {
        return getInterceptorContext(namespace, setName, operation, keys, null);
    }

    public static AerospikeInterceptorContext getInterceptorContext(final String namespace,
                                                                    final String setName,
                                                                    final AerospikeOperation operation,
                                                                    final String[] keys,
                                                                    final Bin[] bins) {
        val requestInfo = AerospikeRequestInfo.builder().keys(keys).bins(bins).build();
        return AerospikeInterceptorContext.builder()
                .namespace(namespace)
                .setName(setName)
                .operation(operation)
                .requestInfo(requestInfo)
                .build();
    }

    private static String[] getKeys(final Key key) {
        return new String[]{key.userKey.toString()};
    }

    private static String[] getKeys(final Key[] keys) {
        return Arrays.stream(keys).map(key -> key.userKey.toString()).toArray(String[]::new);
    }
}
