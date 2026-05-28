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

import java.util.function.Function;

public abstract class AerospikeInterceptor {

    @Getter
    private AerospikeInterceptor next;

    /**
     * Any mutation in AerospikeInterceptorContext will be visible to subsequent interceptors in the chain.
     * This might also impact the order of interceptors in the chain.
     */
    public abstract <T> T execute(final AerospikeInterceptorContext context,
                                  final Function<AerospikeInterceptorContext, T> supplier);


    public final AerospikeInterceptor setNext(final AerospikeInterceptor next) {
        this.next = next;
        return this;
    }

    protected final <T> T proceed(final AerospikeInterceptorContext context, final Function<AerospikeInterceptorContext, T> function) {
        if(null == next) {
            return function.apply(context);
        }
        return next.execute(context, function);
    }
}
