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

package com.phonepe.aerospike.util;

import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.config.read.SingleSourceReadMode;
import com.phonepe.aerospike.config.write.DualWriteMode;
import com.phonepe.aerospike.config.write.SingleSourceWriteMode;
import com.phonepe.aerospike.exception.AerospikeBundleException;
import com.phonepe.aerospike.exception.ResponseCode;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class AerospikeConfigValidationUtilTest {

    private Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v);
        }
        return set;
    }

    @Test
    public void testValidSingleWriteConfig() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("cluster1"))
                .writeMode(new SingleSourceWriteMode("cluster1"))
                .configRefreshInSeconds(10)
                .build();

        // Should not throw
        AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1"));
    }

    @Test
    public void testInvalidReadClusterIdNotInValidSet() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("cluster_unknown"))
                .writeMode(new SingleSourceWriteMode("cluster_unknown"))
                .configRefreshInSeconds(10)
                .build();

        try {
            AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1"));
            Assert.fail("Expected exception");
        } catch (AerospikeBundleException e) {
            Assert.assertEquals(ResponseCode.VALIDATION_ERROR, e.getResponseCode());
        }
    }

    @Test
    public void testInvalidReadClusterIdNotInWriteClusterIds() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("cluster1"))
                .writeMode(new SingleSourceWriteMode("cluster2"))
                .configRefreshInSeconds(10)
                .build();

        try {
            AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1", "cluster2"));
            Assert.fail("Expected exception");
        } catch (AerospikeBundleException e) {
            Assert.assertEquals(ResponseCode.VALIDATION_ERROR, e.getResponseCode());
        }
    }

    @Test
    public void testInvalidEmptyReadClusterId() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode(""))
                .writeMode(new SingleSourceWriteMode("cluster1"))
                .configRefreshInSeconds(10)
                .build();

        try {
            AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1"));
            Assert.fail("Expected exception");
        } catch (AerospikeBundleException e) {
            Assert.assertEquals(ResponseCode.VALIDATION_ERROR, e.getResponseCode());
        }
    }

    @Test
    public void testInvalidWriteClusterIdNotInValidSet() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("cluster1"))
                .writeMode(new DualWriteMode("cluster1", "cluster_invalid", true))
                .configRefreshInSeconds(10)
                .build();

        try {
            AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1"));
            Assert.fail("Expected exception");
        } catch (AerospikeBundleException e) {
            Assert.assertEquals(ResponseCode.VALIDATION_ERROR, e.getResponseCode());
        }
    }

    @Test
    public void testValidDualWriteConfig() {
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("cluster1"))
                .writeMode(new DualWriteMode("cluster1", "cluster2", true))
                .configRefreshInSeconds(10)
                .build();

        // Should not throw
        AerospikeConfigValidationUtil.configValidation(config, setOf("cluster1", "cluster2"));
    }
}
