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

package com.phonepe.aerospike.services;

import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.phonepe.aerospike.client.DualModeAerospikeClient;
import com.phonepe.aerospike.managed.DualModeASClientResolver;
import com.phonepe.aerospike.models.XdrLagDetails;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

public class AerospikeXdrServiceTest {

    private Node createMockNode(String hostname, int port) {
        Node node = mock(Node.class);
        Host host = new Host(hostname, port);
        when(node.getHost()).thenReturn(host);
        when(node.getName()).thenReturn(hostname + ":" + port);
        return node;
    }

    @Test
    public void testGetXdrLagSingleClient() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn("dcs=dc1;src-id=0;trace-sample=0");
            infoMock.when(() -> Info.request(eq(node), eq("get-stats:context=xdr;dc=dc1")))
                    .thenReturn("lag=100;state=ACTIVE;in_queue=0");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            Assert.assertEquals(1, result.get("default").size());
            XdrLagDetails details = result.get("default").get(0);
            Assert.assertEquals("default", details.getClusterId());
            Assert.assertEquals("node1:3000", details.getNodeHost());
            Assert.assertEquals("dc1", details.getTargetDc());
            Assert.assertEquals(100L, details.getLag());
            Assert.assertEquals("ACTIVE", details.getStatus());
        }
    }

    @Test
    public void testGetXdrLagDualModeClient() {
        DualModeASClientResolver resolver = mock(DualModeASClientResolver.class);
        ConcurrentHashMap<String, IAerospikeClient> clientMap = new ConcurrentHashMap<>();

        IAerospikeClient client1 = mock(IAerospikeClient.class);
        IAerospikeClient client2 = mock(IAerospikeClient.class);
        when(client1.isConnected()).thenReturn(true);
        when(client2.isConnected()).thenReturn(true);

        Node node1 = createMockNode("host1", 3000);
        Node node2 = createMockNode("host2", 3000);
        when(client1.getNodes()).thenReturn(new Node[]{node1});
        when(client2.getNodes()).thenReturn(new Node[]{node2});

        clientMap.put("cluster1", client1);
        clientMap.put("cluster2", client2);
        when(resolver.getClientMap()).thenReturn(clientMap);

        DualModeAerospikeClient dualClient = new DualModeAerospikeClient(resolver);
        AerospikeXdrService service = new AerospikeXdrService(dualClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node1), eq("get-config:context=xdr")))
                    .thenReturn("dcs=dc1;src-id=0");
            infoMock.when(() -> Info.request(eq(node1), eq("get-stats:context=xdr;dc=dc1")))
                    .thenReturn("lag=50;state=ACTIVE");
            infoMock.when(() -> Info.request(eq(node2), eq("get-config:context=xdr")))
                    .thenReturn("dcs=dc1;src-id=0");
            infoMock.when(() -> Info.request(eq(node2), eq("get-stats:context=xdr;dc=dc1")))
                    .thenReturn("lag=200;state=ACTIVE");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("cluster1"));
            Assert.assertTrue(result.containsKey("cluster2"));
            Assert.assertEquals(1, result.get("cluster1").size());
            Assert.assertEquals(1, result.get("cluster2").size());
            Assert.assertEquals(50L, result.get("cluster1").get(0).getLag());
            Assert.assertEquals(200L, result.get("cluster2").get(0).getLag());
        }
    }

    @Test
    public void testGetXdrLagMultipleDCs() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn("dcs=dc1,dc2;src-id=0");
            infoMock.when(() -> Info.request(eq(node), eq("get-stats:context=xdr;dc=dc1")))
                    .thenReturn("lag=100;state=ACTIVE");
            infoMock.when(() -> Info.request(eq(node), eq("get-stats:context=xdr;dc=dc2")))
                    .thenReturn("lag=200;state=INACTIVE");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            List<XdrLagDetails> details = result.get("default");
            Assert.assertEquals(2, details.size());
            Assert.assertEquals("dc1", details.get(0).getTargetDc());
            Assert.assertEquals(100L, details.get(0).getLag());
            Assert.assertEquals("ACTIVE", details.get(0).getStatus());
            Assert.assertEquals("dc2", details.get(1).getTargetDc());
            Assert.assertEquals(200L, details.get(1).getLag());
            Assert.assertEquals("INACTIVE", details.get(1).getStatus());
        }
    }

    @Test
    public void testGetXdrLagDisconnectedClient() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(false);

        AerospikeXdrService service = new AerospikeXdrService(mockClient);
        Map<String, List<XdrLagDetails>> result = service.getXdrLag();

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetXdrLagNullConfig() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn(null);

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            Assert.assertTrue(result.get("default").isEmpty());
        }
    }

    @Test
    public void testGetXdrLagEmptyConfig() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn("");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            Assert.assertTrue(result.get("default").isEmpty());
        }
    }

    @Test
    public void testGetXdrLagNoDcs() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn("src-id=0;trace-sample=0");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            Assert.assertTrue(result.get("default").isEmpty());
        }
    }

    @Test
    public void testGetXdrLagStatsParseError() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenReturn("dcs=dc1;src-id=0");
            infoMock.when(() -> Info.request(eq(node), eq("get-stats:context=xdr;dc=dc1")))
                    .thenReturn("lag=not_a_number;state=ACTIVE");

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            // Should not crash - the exception is caught, so dc1 entry is skipped
            Assert.assertTrue(result.get("default").isEmpty());
        }
    }

    @Test
    public void testGetXdrLagNodeException() {
        IAerospikeClient mockClient = mock(IAerospikeClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        Node node = createMockNode("node1", 3000);
        when(mockClient.getNodes()).thenReturn(new Node[]{node});

        AerospikeXdrService service = new AerospikeXdrService(mockClient);

        try (MockedStatic<Info> infoMock = mockStatic(Info.class)) {
            infoMock.when(() -> Info.request(eq(node), eq("get-config:context=xdr")))
                    .thenThrow(new RuntimeException("Connection failed"));

            Map<String, List<XdrLagDetails>> result = service.getXdrLag();

            Assert.assertTrue(result.containsKey("default"));
            // Should not crash - exception is caught
            Assert.assertTrue(result.get("default").isEmpty());
        }
    }
}
