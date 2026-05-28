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

package com.phonepe.aerospike.interceptors.internal.metrics;

import com.codahale.metrics.MetricRegistry;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.interceptors.AerospikeOperation;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricInterceptorTest {

    private MetricRegistry metricRegistry;

    @Before
    public void setup() {
        this.metricRegistry = new MetricRegistry();
    }

    @Test
    public void testWhenMetricsNotEnabled() {
        val metricInterceptor = new MetricInterceptor(AerospikeConfiguration.builder().metricsEnabled(false).build(),
                metricRegistry);
        assertEquals(terminate(), metricInterceptor.execute(null, context -> terminate()));
        Assert.assertTrue(metricRegistry.getMetrics().isEmpty());
    }

    @Test
    public void testWhenContextOnlyHasOperation() {
        val metricInterceptor = new MetricInterceptor(AerospikeConfiguration.builder().metricsEnabled(true).build(),
                metricRegistry);
        assertEquals(terminate(), metricInterceptor.execute(AerospikeInterceptorContext.builder()
                .operation(AerospikeOperation.READ)
                .build(), context -> terminate()));
        val metrics = metricRegistry.getMetrics();
        assertEquals(4, metrics.size());

        val metricData = metricInterceptor.getMetricCache().get(MetricKeyData.builder().operation(AerospikeOperation.READ.getName()).build());
        Assert.assertNotNull(metricData);
        validateMetrics(metricData, 1, 0);
    }

    @Test
    public void testWhenContextHasOperationAndNamespace() {
        val metricInterceptor = new MetricInterceptor(AerospikeConfiguration.builder().metricsEnabled(true).build(),
                metricRegistry);
        AerospikeOperation operation = AerospikeOperation.READ;
        val namespace = "testNamespace";
        assertEquals(terminate(), metricInterceptor.execute(AerospikeInterceptorContext.builder()
                .operation(operation)
                .namespace(namespace)
                .build(), context -> terminate()));
        val metrics = metricRegistry.getMetrics();
        assertEquals(4, metrics.size());
        val metricData = metricInterceptor.getMetricCache().get(MetricKeyData.builder()
                .namespace(namespace)
                .operation(operation.getName())
                .build());
        Assert.assertNotNull(metricData);
        validateMetrics(metricData, 1, 0);
    }

    @Test
    public void testWhenContextHasOperationNamespaceAndSet() {
        val metricInterceptor = new MetricInterceptor(AerospikeConfiguration.builder().metricsEnabled(true).build(),
                metricRegistry);
        AerospikeOperation operation = AerospikeOperation.READ;
        val namespace = "testNamespace";
        val setName = "setName";
        assertEquals(terminate(), metricInterceptor.execute(AerospikeInterceptorContext.builder()
                .operation(operation)
                .namespace(namespace)
                .setName(setName)
                .build(), context -> terminate()));
        val metrics = metricRegistry.getMetrics();
        assertEquals(4, metrics.size());

        val metricData = metricInterceptor.getMetricCache().get(MetricKeyData.builder()
                .setName(setName)
                .namespace(namespace)
                .operation(operation.getName())
                .build());
        Assert.assertNotNull(metricData);
        validateMetrics(metricData, 1, 0);
    }

    @Test
    public void testExecutionWithException() {
        val metricInterceptor = new MetricInterceptor(AerospikeConfiguration.builder().metricsEnabled(true).build(),
                metricRegistry);
        AerospikeOperation operation = AerospikeOperation.READ;
        val namespace = "testNamespace";
        val setName = "setName";
        try {
            metricInterceptor.execute(AerospikeInterceptorContext.builder()
                    .operation(operation)
                    .namespace(namespace)
                    .setName(setName)
                    .build(), context -> terminateWithException());
        } catch (RuntimeException e) {

        }
        val metrics = metricRegistry.getMetrics();
        assertEquals(4, metrics.size());

        val metricData = metricInterceptor.getMetricCache().get(MetricKeyData.builder()
                .setName(setName)
                .namespace(namespace)
                .operation(operation.getName())
                .build());
        Assert.assertNotNull(metricData);
        validateMetrics(metricData, 0, 1);
    }

    private void validateMetrics(final MetricData metricData,
                                 final int successCount,
                                 final int failedCount) {
        assertEquals(1, metricData.getTotal().getCount());
        assertEquals(1, metricData.getTimer().getCount());
        assertEquals(successCount, metricData.getSuccess().getCount());
        assertEquals(failedCount, metricData.getFailed().getCount());
    }

    private Integer terminate() {
        return 1;
    }


    private Integer terminateWithException() {
        throw new RuntimeException();
    }

}
