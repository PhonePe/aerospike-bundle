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

package com.phonepe.aerospike.interceptors.internal;

import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;

import java.util.function.Function;

public class TerminalOperationInterceptor extends AerospikeInterceptor {

    @Override
    public <T> T execute(AerospikeInterceptorContext context, Function<AerospikeInterceptorContext, T> supplier) {
        return proceed(context, supplier);
    }
}
