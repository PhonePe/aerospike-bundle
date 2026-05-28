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

package com.phonepe.aerospike.bundle;

import com.codahale.metrics.MetricRegistry;
import com.phonepe.aerospike.config.AerospikeBundleConfig;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;
import com.phonepe.aerospike.interceptors.internal.TerminalOperationInterceptor;
import com.phonepe.aerospike.interceptors.internal.metrics.MetricInterceptor;
import io.dropwizard.Configuration;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;
import java.util.function.Supplier;

public class AerospikeBundleTest {

    private static class DummyInterceptor extends AerospikeInterceptor {
        @Override
        public <T> T execute(AerospikeInterceptorContext context, Function<AerospikeInterceptorContext, T> function) {
            return null;
        }
    }

    @Test
    public void testRegisterInterceptor() {
        val bundle = new AerospikeBundle<Configuration>() {
            @Override
            protected AerospikeBundleConfig configuration(Configuration configuration) {
                return null;
            }

            @Override
            protected Supplier<DualModeASReadWriteConfig> refreshDualModeASReadWriteConfig(Configuration configuration) {
                return () -> null;
            }
        };
        val dummyInterceptor = new DummyInterceptor();
        bundle.registerInterceptor(dummyInterceptor);
        Assert.assertEquals(1, bundle.getInterceptors().size());
        Assert.assertEquals(dummyInterceptor, bundle.getInterceptors().get(0));
    }

    @Test
    public void testInterceptor() {
        val bundle = new AerospikeBundle<Configuration>() {
            @Override
            protected AerospikeBundleConfig configuration(Configuration configuration) {
                return null;
            }

            @Override
            protected Supplier<DualModeASReadWriteConfig> refreshDualModeASReadWriteConfig(Configuration configuration) {
                return () -> null;
            }
        };
        val dummyInterceptor = new DummyInterceptor();
        bundle.registerInterceptor(dummyInterceptor);

        val rootInterceptor = bundle.getRootInterceptor(new AerospikeConfiguration(), new MetricRegistry());
        Assert.assertTrue(rootInterceptor instanceof MetricInterceptor);

        Assert.assertTrue(dummyInterceptor.getNext() instanceof TerminalOperationInterceptor);

        val terminalInterceptor = dummyInterceptor.getNext();
        Assert.assertNull(terminalInterceptor.getNext());
    }
}
