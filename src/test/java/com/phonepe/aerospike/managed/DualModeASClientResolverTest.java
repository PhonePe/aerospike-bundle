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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class DualModeASClientResolverTest {

    @Test
    public void testPrimaryASClient() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, "clusterId2");
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockClient);

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        IAerospikeClient client = resolver.primaryASClient();

        assertSame(mockClient, client);
    }

    @Test
    public void testGetAllAerospikeClient() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val mockPrimaryClient = mock(IAerospikeClient.class);
        val mockSecondaryClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockPrimaryClient);
        asClientMap.put(secondaryClusterId, mockSecondaryClient);

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        List<IAerospikeClient> allClients = resolver.getAllAerospikeClient();

        assertEquals(2, allClients.size());
    }

    @Test
    public void testGetClientMap() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, "clusterId2");
        val mockClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockClient);

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        assertEquals(1, resolver.getClientMap().size());
        assertSame(mockClient, resolver.getClientMap().get(primaryClusterId));
    }

    @Test
    public void testConfigRefreshNullSupplier() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        val mockPrimaryClient = mock(IAerospikeClient.class);
        asClientMap.put(primaryClusterId, mockPrimaryClient);
        asClientMap.put(secondaryClusterId, mock(IAerospikeClient.class));

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        resolver.setDualModeASReadWriteConfigSupplier(null);
        // Should not throw
        resolver.configRefresh();
        assertEquals(dualModeASReadWriteConfig.toString(), resolver.getDualModeASReadWriteConfig().toString());
    }

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

    @Test
    public void testStartAndStop() throws InterruptedException {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        asClientMap.put(primaryClusterId, mock(IAerospikeClient.class));
        asClientMap.put(secondaryClusterId, mock(IAerospikeClient.class));

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        resolver.start();
        // Give scheduler a moment to confirm it started
        TimeUnit.MILLISECONDS.sleep(50);
        resolver.stop();
        assertNotNull(resolver.getDualModeASReadWriteConfig());
    }

    @Test
    public void testGetDualModeASReadWriteConfig() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        asClientMap.put(primaryClusterId, mock(IAerospikeClient.class));

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        val config = resolver.getDualModeASReadWriteConfig();
        assertNotNull(config);
        assertEquals(dualModeASReadWriteConfig.toString(), config.toString());
    }

    @Test
    public void testConfigRefreshWithSupplierReturningNull() {
        ConcurrentHashMap<String, IAerospikeClient> asClientMap = new ConcurrentHashMap<>();
        val primaryClusterId = "clusterId1";
        val secondaryClusterId = "clusterId2";
        val dualModeASReadWriteConfig = buildDualModeASReadWriteConfig(primaryClusterId, secondaryClusterId);
        asClientMap.put(primaryClusterId, mock(IAerospikeClient.class));
        asClientMap.put(secondaryClusterId, mock(IAerospikeClient.class));

        val resolver = new DualModeASClientResolver(() -> dualModeASReadWriteConfig, asClientMap);
        // Set supplier that returns null
        resolver.setDualModeASReadWriteConfigSupplier(() -> null);
        resolver.configRefresh();
        // Config should not change
        assertEquals(dualModeASReadWriteConfig.toString(), resolver.getDualModeASReadWriteConfig().toString());
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
