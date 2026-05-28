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

import io.dropwizard.util.Strings;
import lombok.experimental.UtilityClass;
import lombok.val;

@UtilityClass
public class MetricUtil {

    private static final String AEROSPIKE_PREFIX = "aerospike";
    private static final String DELIMITER = ".";
    private static final String DELIMITER_REPLACEMENT = "_";

    public String getMetricPrefix(final MetricKeyData metricKeyData) {
        if(Strings.isNullOrEmpty(metricKeyData.getSetName()) && Strings.isNullOrEmpty(metricKeyData.getNamespace())) {
            return getMetricPrefix(metricKeyData.getOperation());
        }

        if(Strings.isNullOrEmpty(metricKeyData.getSetName())) {
            return getMetricPrefix(metricKeyData.getNamespace(), metricKeyData.getOperation());
        }

        return getMetricPrefix(metricKeyData.getNamespace(), metricKeyData.getSetName(), metricKeyData.getOperation());
    }

    private String getMetricPrefix(String... metricNames) {
        val metricPrefix = new StringBuilder(AEROSPIKE_PREFIX);
        for (val metricName : metricNames) {
            metricPrefix.append(DELIMITER).append(normalizeString(metricName));
        }
        return metricPrefix.toString();
    }

    private static String normalizeString(final String name) {
        return name.replace(DELIMITER, DELIMITER_REPLACEMENT);
    }
}
