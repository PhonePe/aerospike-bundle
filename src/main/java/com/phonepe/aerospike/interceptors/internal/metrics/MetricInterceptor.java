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
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class MetricInterceptor extends AerospikeInterceptor {

    private final AerospikeConfiguration aerospikeConfiguration;

    private final MetricRegistry metricRegistry;

    @Getter
    private final Map<MetricKeyData, MetricData> metricCache = new ConcurrentHashMap<>();

    public MetricInterceptor(final AerospikeConfiguration aerospikeConfiguration,
                             final MetricRegistry metricRegistry) {
        this.aerospikeConfiguration = aerospikeConfiguration;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public <T> T execute(AerospikeInterceptorContext context, Function<AerospikeInterceptorContext, T> function) {
        if (!aerospikeConfiguration.isMetricsEnabled()) {
            return proceed(context, function);
        }
        val metricData = getMetricData(context);
        metricData.getTotal().mark();
        val timer = metricData.getTimer().time();
        try {
            val response = proceed(context, function);
            metricData.getSuccess().mark();
            return response;
        } catch (Throwable t) {
            metricData.getFailed().mark();
            throw t;
        } finally {
            timer.stop();
        }
    }

    private MetricData getMetricData(final AerospikeInterceptorContext context) {
        val metricKeyData = MetricKeyData.builder()
                .setName(context.getSetName())
                .namespace(context.getNamespace())
                .operation(context.getOperation().getName())
                .build();
        return metricCache.computeIfAbsent(metricKeyData, key ->
                getMetricData(MetricUtil.getMetricPrefix(metricKeyData)));
    }

    private MetricData getMetricData(final String metricPrefix) {
        return MetricData.builder()
                .timer(metricRegistry.timer(MetricRegistry.name(metricPrefix, "latency"),
                        () -> new Timer(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS))))
                .success(metricRegistry.meter(MetricRegistry.name(metricPrefix, "success")))
                .failed(metricRegistry.meter(MetricRegistry.name(metricPrefix, "failed")))
                .total(metricRegistry.meter(MetricRegistry.name(metricPrefix, "total")))
                .build();
    }
}
