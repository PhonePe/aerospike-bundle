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

package com.phonepe.aerospike.managed;

import com.aerospike.client.IAerospikeClient;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.config.read.SingleSourceReadMode;
import com.phonepe.aerospike.config.write.DualWriteMode;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class DualModeASClientResolverTest {

    @Test
    public void testWriteASClientDualMode() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, "clusterId2");
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockClient);

        val dualModeASClient = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        IAerospikeClient client = dualModeASClient.primaryWriteASClient();

        assertEquals(mockClient, client);
    }

    @Test
    public void testSecondaryWriteASClient() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "primaryClusterId";
        val secondaryClusterId = "secondaryClusterId";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val mockPrimaryClient = mock(IAerospikeClient.class);
        val mockSecondaryClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockPrimaryClient);
        asClientMap.put(secondaryClusterId, mockSecondaryClient);

        val dualModeASClient = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        val client = dualModeASClient.secondaryWriteASClient((DualWriteMode) dualModeASReadWriteConfig.getWriteMode());

        assertEquals(mockSecondaryClient, client);
    }

    @Test
    public void testConfigRefreshOnValidConfig() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val baseConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val mockPrimaryClient = mock(IAerospikeClient.class);
        val mockSecondaryClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockPrimaryClient);
        asClientMap.put(secondaryClusterId, mockSecondaryClient);
        val dualModeASClient = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);

        assertEquals(baseConfig.toString(), dualModeASReadWriteConfig.toString());
        assertEquals(baseConfig.toString(), dualModeASClient.getDualModeASReadWriteConfig().toString());

        dualModeASReadWriteConfig.setWriteMode(DualWriteMode.builder()
                .primaryClusterId(secondaryClusterId)
                .secondaryClusterId(primaryClusterId)
                .build());
        dualModeASClient.configRefresh();

        // config changed as config is valid
        assertNotEquals(baseConfig.toString(), dualModeASReadWriteConfig.toString());
        assertNotEquals(baseConfig.toString(), dualModeASClient.getDualModeASReadWriteConfig().toString());
    }

    @Test
    public void testConfigRefreshOnInvalidConfig() {
        // Setup
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val baseConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val initialConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val mockPrimaryClient = mock(IAerospikeClient.class);
        val mockSecondaryClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockPrimaryClient);
        asClientMap.put(secondaryClusterId, mockSecondaryClient);

        // Create DualModeASClientResolver with initial config
        val dualModeASClient = new DualModeASClientResolver(() -> initialConfig, asClientMap);

        // Assert initial state
        assertEquals(initialConfig.toString(), dualModeASClient.getDualModeASReadWriteConfig().toString());

        // Update configuration to an invalid state
        val invalidConfig = buildDualModeASReadWriteConfig(primaryClusterId, "someOtherCluster"); // Simulate invalid config
        dualModeASClient.setDualModeASReadWriteConfigSupplier( () -> invalidConfig); // Set supplier to return invalid config

        // Refresh the configuration
        dualModeASClient.configRefresh();

        // Assert that the configuration has not changed
        assertEquals(initialConfig.toString(), dualModeASClient.getDualModeASReadWriteConfig().toString());

    }

    private DualModeASReadWriteConfig buildDualModeASReadWriteConfig(final String primaryClusterId, final String secondaryClusterId) {
        return DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode(primaryClusterId))
                .writeMode(DualWriteMode.builder()
                        .primaryClusterId(primaryClusterId)
                        .secondaryClusterId(secondaryClusterId)
                        .build())
                .configRefreshInSeconds(5)
                .build();
    }

}
