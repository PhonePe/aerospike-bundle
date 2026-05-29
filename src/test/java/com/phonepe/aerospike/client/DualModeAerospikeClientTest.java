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

package com.phonepe.aerospike.client;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.config.read.SingleSourceReadMode;
import com.phonepe.aerospike.config.write.DualWriteMode;
import com.phonepe.aerospike.config.write.SingleSourceWriteMode;
import com.phonepe.aerospike.exception.AerospikeBundleException;
import com.phonepe.aerospike.exception.ResponseCode;
import com.phonepe.aerospike.managed.DualModeASClientResolver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class DualModeAerospikeClientTest {

    private DualModeASClientResolver resolver;
    private IAerospikeClient primaryClient;
    private IAerospikeClient secondaryClient;
    private DualModeAerospikeClient dualClient;
    private DualModeASReadWriteConfig singleConfig;
    private DualModeASReadWriteConfig dualConfig;
    private DualWriteMode dualWriteMode;

    @Before
    public void setUp() {
        resolver = mock(DualModeASClientResolver.class);
        primaryClient = mock(IAerospikeClient.class);
        secondaryClient = mock(IAerospikeClient.class);
        dualClient = new DualModeAerospikeClient(resolver);

        when(resolver.primaryASClient()).thenReturn(primaryClient);
        when(resolver.primaryWriteASClient()).thenReturn(primaryClient);

        singleConfig = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("c1"))
                .writeMode(new SingleSourceWriteMode("c1"))
                .configRefreshInSeconds(10)
                .build();

        dualWriteMode = new DualWriteMode("c1", "c2", true);
        dualConfig = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("c1"))
                .writeMode(dualWriteMode)
                .configRefreshInSeconds(10)
                .build();

        when(resolver.secondaryWriteASClient(dualWriteMode)).thenReturn(secondaryClient);
    }

    // --- Policy delegation tests ---

    @Test
    public void testGetReadPolicyDefault() {
        Policy policy = new Policy();
        when(primaryClient.getReadPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getReadPolicyDefault());
    }

    @Test
    public void testGetWritePolicyDefault() {
        WritePolicy policy = new WritePolicy();
        when(primaryClient.getWritePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getWritePolicyDefault());
    }

    @Test
    public void testGetScanPolicyDefault() {
        ScanPolicy policy = new ScanPolicy();
        when(primaryClient.getScanPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getScanPolicyDefault());
    }

    @Test
    public void testGetQueryPolicyDefault() {
        QueryPolicy policy = new QueryPolicy();
        when(primaryClient.getQueryPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getQueryPolicyDefault());
    }

    @Test
    public void testGetBatchPolicyDefault() {
        BatchPolicy policy = new BatchPolicy();
        when(primaryClient.getBatchPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getBatchPolicyDefault());
    }

    @Test
    public void testGetBatchParentPolicyWriteDefault() {
        BatchPolicy policy = new BatchPolicy();
        when(primaryClient.getBatchPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getBatchParentPolicyWriteDefault());
    }

    @Test
    public void testGetBatchWritePolicyDefault() {
        BatchWritePolicy policy = new BatchWritePolicy();
        when(primaryClient.getBatchWritePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getBatchWritePolicyDefault());
    }

    @Test
    public void testGetBatchDeletePolicyDefault() {
        BatchDeletePolicy policy = new BatchDeletePolicy();
        when(primaryClient.getBatchDeletePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getBatchDeletePolicyDefault());
    }

    @Test
    public void testGetBatchUDFPolicyDefault() {
        BatchUDFPolicy policy = new BatchUDFPolicy();
        when(primaryClient.getBatchUDFPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getBatchUDFPolicyDefault());
    }

    @Test
    public void testGetInfoPolicyDefault() {
        InfoPolicy policy = new InfoPolicy();
        when(primaryClient.getInfoPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, dualClient.getInfoPolicyDefault());
    }

    // --- Connection/Node tests ---

    @Test
    public void testIsConnected() {
        when(primaryClient.isConnected()).thenReturn(true);
        Assert.assertTrue(dualClient.isConnected());
    }

    @Test
    public void testGetNodes() {
        Node[] nodes = new Node[]{};
        when(primaryClient.getNodes()).thenReturn(nodes);
        Assert.assertSame(nodes, dualClient.getNodes());
    }

    @Test
    public void testGetNodeNames() {
        List<String> names = Arrays.asList("node1", "node2");
        when(primaryClient.getNodeNames()).thenReturn(names);
        Assert.assertEquals(names, dualClient.getNodeNames());
    }

    @Test
    public void testClose() {
        when(resolver.getAllAerospikeClient()).thenReturn(Arrays.asList(primaryClient, secondaryClient));
        dualClient.close();
        verify(primaryClient).close();
        verify(secondaryClient).close();
    }

    @Test
    public void testGetAsClientResolver() {
        Assert.assertSame(resolver, dualClient.getAsClientResolver());
    }

    // --- Put: single write mode ---

    @Test
    public void testPutSingleWriteMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "v1");
        WritePolicy wp = new WritePolicy();
        dualClient.put(wp, key, bin);
        verify(primaryClient).put(wp, key, bin);
    }

    // --- Put: dual write mode ---

    @Test
    public void testPutDualWriteMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "v1");
        WritePolicy wp = new WritePolicy();
        dualClient.put(wp, key, bin);
        verify(primaryClient).put(wp, key, bin);
        verify(secondaryClient).put(wp, key, bin);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testPutDualWriteModeSecondaryFailsWithErrorEnabled() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        doThrow(new AerospikeException("fail")).when(secondaryClient).put(any(WritePolicy.class), any(Key.class), any(Bin.class));
        Key key = new Key("ns", "set", "k1");
        dualClient.put(new WritePolicy(), key, new Bin("b", "v"));
    }

    @Test
    public void testPutDualWriteModeSecondaryFailsWithErrorDisabled() {
        DualWriteMode dwm = new DualWriteMode("c1", "c2", false);
        DualModeASReadWriteConfig config = DualModeASReadWriteConfig.builder()
                .readMode(new SingleSourceReadMode("c1"))
                .writeMode(dwm)
                .configRefreshInSeconds(10)
                .build();
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(config);
        when(resolver.secondaryWriteASClient(dwm)).thenReturn(secondaryClient);
        doThrow(new AerospikeException("fail")).when(secondaryClient).put(any(WritePolicy.class), any(Key.class), any(Bin.class));
        Key key = new Key("ns", "set", "k1");
        dualClient.put(new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).put(any(WritePolicy.class), eq(key), any(Bin.class));
    }

    // --- Put async ---

    @Test
    public void testPutAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Bin bin = new Bin("b", "v");
        dualClient.put(el, wl, wp, key, bin);
        verify(primaryClient).put(el, wl, wp, key, bin);
    }

    @Test
    public void testPutAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Bin bin = new Bin("b", "v");
        dualClient.put(el, wl, wp, key, bin);
        // Primary is called with PrimaryWriteListener wrapper
        verify(primaryClient).put(eq(el), any(WriteListener.class), eq(wp), eq(key), eq(bin));
    }

    // --- Append ---

    @Test
    public void testAppendSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        dualClient.append(new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).append(any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testAppendDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Bin bin = new Bin("b", "v");
        dualClient.append(wp, key, bin);
        verify(primaryClient).append(wp, key, bin);
        verify(secondaryClient).append(wp, key, bin);
    }

    @Test
    public void testAppendAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.append(el, wl, new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).append(eq(el), eq(wl), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testAppendAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.append(el, wl, new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).append(eq(el), any(WriteListener.class), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    // --- Prepend ---

    @Test
    public void testPrependSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        dualClient.prepend(new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).prepend(any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testPrependDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Bin bin = new Bin("b", "v");
        dualClient.prepend(wp, key, bin);
        verify(primaryClient).prepend(wp, key, bin);
        verify(secondaryClient).prepend(wp, key, bin);
    }

    @Test
    public void testPrependAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.prepend(el, wl, new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).prepend(eq(el), eq(wl), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testPrependAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.prepend(el, wl, new WritePolicy(), key, new Bin("b", "v"));
        verify(primaryClient).prepend(eq(el), any(WriteListener.class), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    // --- Add ---

    @Test
    public void testAddSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        dualClient.add(new WritePolicy(), key, new Bin("b", 1));
        verify(primaryClient).add(any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testAddDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Bin bin = new Bin("b", 1);
        dualClient.add(wp, key, bin);
        verify(primaryClient).add(wp, key, bin);
        verify(secondaryClient).add(wp, key, bin);
    }

    @Test
    public void testAddAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.add(el, wl, new WritePolicy(), key, new Bin("b", 1));
        verify(primaryClient).add(eq(el), eq(wl), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    @Test
    public void testAddAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        dualClient.add(el, wl, new WritePolicy(), key, new Bin("b", 1));
        verify(primaryClient).add(eq(el), any(WriteListener.class), any(WritePolicy.class), eq(key), any(Bin.class));
    }

    // --- Delete ---

    @Test
    public void testDeleteSingleModeSuccess() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.delete(wp, key)).thenReturn(true);
        Assert.assertTrue(dualClient.delete(wp, key));
    }

    @Test
    public void testDeleteDualModeSuccess() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.delete(wp, key)).thenReturn(true);
        when(secondaryClient.delete(wp, key)).thenReturn(true);
        Assert.assertTrue(dualClient.delete(wp, key));
        verify(secondaryClient).delete(wp, key);
    }

    @Test
    public void testDeleteDualModePrimaryReturnsFalse() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.delete(wp, key)).thenReturn(false);
        Assert.assertFalse(dualClient.delete(wp, key));
        verify(secondaryClient, never()).delete(any(), any());
    }

    @Test
    public void testDeleteDualModeSecondaryFails() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.delete(wp, key)).thenReturn(true);
        doThrow(new AerospikeException("fail")).when(secondaryClient).delete(wp, key);
        try {
            dualClient.delete(wp, key);
            Assert.fail("Expected exception");
        } catch (AerospikeBundleException e) {
            Assert.assertEquals(ResponseCode.SECONDARY_CLUSTER_DELETE_ATTEMPT_FAILED, e.getResponseCode());
        }
    }

    @Test
    public void testDeleteAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        DeleteListener dl = mock(DeleteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.delete(el, dl, wp, key);
        verify(primaryClient).delete(el, dl, wp, key);
    }

    @Test
    public void testDeleteAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        DeleteListener dl = mock(DeleteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.delete(el, dl, wp, key);
        verify(primaryClient).delete(eq(el), any(DeleteListener.class), eq(wp), eq(key));
    }

    // --- Batch Delete ---

    @Test
    public void testBatchDeleteDualModeSuccess() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(primaryClient.delete(bp, bdp, keys)).thenReturn(br);
        BatchResults result = dualClient.delete(bp, bdp, keys);
        Assert.assertSame(br, result);
        verify(secondaryClient).delete(bp, bdp, keys);
    }

    @Test
    public void testBatchDeleteAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener l = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, l, bp, bdp, keys);
        verify(primaryClient).delete(el, l, bp, bdp, keys);
    }

    @Test
    public void testBatchDeleteAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener l = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, l, bp, bdp, keys);
        verify(primaryClient).delete(eq(el), any(BatchRecordArrayListener.class), eq(bp), eq(bdp), eq(keys));
    }

    @Test
    public void testBatchDeleteSequenceAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener l = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, l, bp, bdp, keys);
        verify(primaryClient).delete(el, l, bp, bdp, keys);
    }

    @Test
    public void testBatchDeleteSequenceAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener l = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, l, bp, bdp, keys);
        verify(primaryClient).delete(eq(el), any(BatchRecordSequenceListener.class), eq(bp), eq(bdp), eq(keys));
    }

    // --- Truncate ---

    @Test
    public void testTruncateSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        InfoPolicy ip = new InfoPolicy();
        dualClient.truncate(ip, "ns", "set", null);
        verify(primaryClient).truncate(ip, "ns", "set", null);
    }

    @Test
    public void testTruncateDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        InfoPolicy ip = new InfoPolicy();
        dualClient.truncate(ip, "ns", "set", null);
        verify(primaryClient).truncate(ip, "ns", "set", null);
        verify(secondaryClient).truncate(ip, "ns", "set", null);
    }

    // --- Touch ---

    @Test
    public void testTouchSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.touch(wp, key);
        verify(primaryClient).touch(wp, key);
    }

    @Test
    public void testTouchDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.touch(wp, key);
        verify(primaryClient).touch(wp, key);
        verify(secondaryClient).touch(wp, key);
    }

    @Test
    public void testTouchAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.touch(el, wl, wp, key);
        verify(primaryClient).touch(el, wl, wp, key);
    }

    @Test
    public void testTouchAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.touch(el, wl, wp, key);
        verify(primaryClient).touch(eq(el), any(WriteListener.class), eq(wp), eq(key));
    }

    // --- Exists ---

    @Test
    public void testExists() {
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        when(primaryClient.exists(policy, key)).thenReturn(true);
        Assert.assertTrue(dualClient.exists(policy, key));
    }

    @Test
    public void testExistsAsync() {
        EventLoop el = mock(EventLoop.class);
        ExistsListener l = mock(ExistsListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        dualClient.exists(el, l, policy, key);
        verify(primaryClient).exists(el, l, policy, key);
    }

    @Test
    public void testExistsBatch() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        when(primaryClient.exists(bp, keys)).thenReturn(new boolean[]{true});
        boolean[] result = dualClient.exists(bp, keys);
        Assert.assertTrue(result[0]);
    }

    @Test
    public void testExistsBatchAsyncArray() {
        EventLoop el = mock(EventLoop.class);
        ExistsArrayListener l = mock(ExistsArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.exists(el, l, bp, keys);
        verify(primaryClient).exists(el, l, bp, keys);
    }

    @Test
    public void testExistsBatchAsyncSequence() {
        EventLoop el = mock(EventLoop.class);
        ExistsSequenceListener l = mock(ExistsSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.exists(el, l, bp, keys);
        verify(primaryClient).exists(el, l, bp, keys);
    }

    // --- Get ---

    @Test
    public void testGet() {
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        Record testRecord = new Record(null, 1, 1);
        when(primaryClient.get(policy, key)).thenReturn(testRecord);
        Assert.assertSame(testRecord, dualClient.get(policy, key));
    }

    @Test
    public void testGetWithBins() {
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        Record testRecord = new Record(null, 1, 1);
        when(primaryClient.get(policy, key, "bin1")).thenReturn(testRecord);
        Assert.assertSame(testRecord, dualClient.get(policy, key, "bin1"));
    }

    @Test
    public void testGetAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        dualClient.get(el, rl, policy, key);
        verify(primaryClient).get(el, rl, policy, key);
    }

    @Test
    public void testGetAsyncWithBins() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        dualClient.get(el, rl, policy, key, "bin1");
        verify(primaryClient).get(el, rl, policy, key, "bin1");
    }

    @Test
    public void testGetHeader() {
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        Record testRecord = new Record(null, 1, 1);
        when(primaryClient.getHeader(policy, key)).thenReturn(testRecord);
        Assert.assertSame(testRecord, dualClient.getHeader(policy, key));
    }

    @Test
    public void testGetHeaderAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        dualClient.getHeader(el, rl, policy, key);
        verify(primaryClient).getHeader(el, rl, policy, key);
    }

    // --- Batch Get ---

    @Test
    public void testBatchGetList() {
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        when(primaryClient.get(bp, list)).thenReturn(true);
        Assert.assertTrue(dualClient.get(bp, list));
    }

    @Test
    public void testBatchGetListAsync() {
        EventLoop el = mock(EventLoop.class);
        BatchListListener l = mock(BatchListListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        dualClient.get(el, l, bp, list);
        verify(primaryClient).get(el, l, bp, list);
    }

    @Test
    public void testBatchGetSequenceAsync() {
        EventLoop el = mock(EventLoop.class);
        BatchSequenceListener l = mock(BatchSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        dualClient.get(el, l, bp, list);
        verify(primaryClient).get(el, l, bp, list);
    }

    @Test
    public void testBatchGetKeys() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(primaryClient.get(bp, keys)).thenReturn(records);
        Assert.assertSame(records, dualClient.get(bp, keys));
    }

    @Test
    public void testBatchGetKeysWithBins() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(primaryClient.get(bp, keys, "bin1")).thenReturn(records);
        Assert.assertSame(records, dualClient.get(bp, keys, "bin1"));
    }

    @Test
    public void testBatchGetKeysWithOps() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(primaryClient.get(bp, keys, op)).thenReturn(records);
        Assert.assertSame(records, dualClient.get(bp, keys, op));
    }

    @Test
    public void testBatchGetHeaderKeys() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(primaryClient.getHeader(bp, keys)).thenReturn(records);
        Assert.assertSame(records, dualClient.getHeader(bp, keys));
    }

    @Test
    public void testBatchGetKeysAsyncArray() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener l = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.get(el, l, bp, keys);
        verify(primaryClient).get(el, l, bp, keys);
    }

    @Test
    public void testBatchGetKeysAsyncSequence() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener l = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.get(el, l, bp, keys);
        verify(primaryClient).get(el, l, bp, keys);
    }

    @Test
    public void testBatchGetHeaderAsyncArray() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener l = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.getHeader(el, l, bp, keys);
        verify(primaryClient).getHeader(el, l, bp, keys);
    }

    @Test
    public void testBatchGetHeaderAsyncSequence() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener l = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.getHeader(el, l, bp, keys);
        verify(primaryClient).getHeader(el, l, bp, keys);
    }

    // --- Operate ---

    @Test
    public void testOperateSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Operation op = Operation.get();
        Record testRecord = new Record(null, 1, 1);
        when(primaryClient.operate(wp, key, op)).thenReturn(testRecord);
        Assert.assertSame(testRecord, dualClient.operate(wp, key, op));
    }

    @Test
    public void testOperateDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Operation op = Operation.get();
        Record testRecord = new Record(null, 1, 1);
        when(primaryClient.operate(wp, key, op)).thenReturn(testRecord);
        Assert.assertSame(testRecord, dualClient.operate(wp, key, op));
        verify(secondaryClient).operate(wp, key, op);
    }

    @Test
    public void testOperateAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Operation op = Operation.get();
        dualClient.operate(el, rl, wp, key, op);
        verify(primaryClient).operate(el, rl, wp, key, op);
    }

    @Test
    public void testOperateAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Operation op = Operation.get();
        dualClient.operate(el, rl, wp, key, op);
        verify(primaryClient).operate(eq(el), any(RecordListener.class), eq(wp), eq(key), eq(op));
    }

    @Test
    public void testOperateBatchListSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        when(primaryClient.operate(bp, list)).thenReturn(true);
        Assert.assertTrue(dualClient.operate(bp, list));
    }

    @Test
    public void testOperateBatchListDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        when(primaryClient.operate(bp, list)).thenReturn(true);
        Assert.assertTrue(dualClient.operate(bp, list));
        verify(secondaryClient).operate(bp, list);
    }

    @Test
    public void testOperateBatchListAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchOperateListListener l = mock(BatchOperateListListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        dualClient.operate(el, l, bp, list);
        verify(primaryClient).operate(el, l, bp, list);
    }

    @Test
    public void testOperateBatchListAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchOperateListListener l = mock(BatchOperateListListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        dualClient.operate(el, l, bp, list);
        verify(primaryClient).operate(eq(el), any(BatchOperateListListener.class), eq(bp), eq(list));
    }

    @Test
    public void testOperateBatchSequenceAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener l = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        dualClient.operate(el, l, bp, list);
        verify(primaryClient).operate(el, l, bp, list);
    }

    @Test
    public void testOperateBatchSequenceAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener l = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        dualClient.operate(el, l, bp, list);
        verify(primaryClient).operate(eq(el), any(BatchRecordSequenceListener.class), eq(bp), eq(list));
    }

    @Test
    public void testOperateBatchKeysSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(primaryClient.operate(bp, bwp, keys, op)).thenReturn(br);
        Assert.assertSame(br, dualClient.operate(bp, bwp, keys, op));
    }

    @Test
    public void testOperateBatchKeysDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(primaryClient.operate(bp, bwp, keys, op)).thenReturn(br);
        Assert.assertSame(br, dualClient.operate(bp, bwp, keys, op));
        verify(secondaryClient).operate(bp, bwp, keys, op);
    }

    @Test
    public void testOperateBatchKeysArrayAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener l = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        dualClient.operate(el, l, bp, bwp, keys, op);
        verify(primaryClient).operate(el, l, bp, bwp, keys, op);
    }

    @Test
    public void testOperateBatchKeysSequenceAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener l = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        dualClient.operate(el, l, bp, bwp, keys, op);
        verify(primaryClient).operate(el, l, bp, bwp, keys, op);
    }

    // --- Scan ---

    @Test
    public void testScanAll() {
        ScanPolicy sp = new ScanPolicy();
        ScanCallback cb = mock(ScanCallback.class);
        dualClient.scanAll(sp, "ns", "set", cb, "bin1");
        verify(primaryClient).scanAll(sp, "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanAllAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener l = mock(RecordSequenceListener.class);
        ScanPolicy sp = new ScanPolicy();
        dualClient.scanAll(el, l, sp, "ns", "set", "bin1");
        verify(primaryClient).scanAll(el, l, sp, "ns", "set", "bin1");
    }

    @Test
    public void testScanNodeByName() {
        ScanPolicy sp = new ScanPolicy();
        ScanCallback cb = mock(ScanCallback.class);
        dualClient.scanNode(sp, "node1", "ns", "set", cb, "bin1");
        verify(primaryClient).scanNode(sp, "node1", "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanNodeByNode() {
        ScanPolicy sp = new ScanPolicy();
        ScanCallback cb = mock(ScanCallback.class);
        Node node = mock(Node.class);
        dualClient.scanNode(sp, node, "ns", "set", cb, "bin1");
        verify(primaryClient).scanNode(sp, node, "ns", "set", cb, "bin1");
    }

    // --- Query ---

    @Test
    public void testQuery() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        // RecordSet is final, just verify the call is delegated
        dualClient.query(qp, stmt);
        verify(primaryClient).query(qp, stmt);
    }

    @Test
    public void testQueryWithListener() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        QueryListener ql = mock(QueryListener.class);
        dualClient.query(qp, stmt, ql);
        verify(primaryClient).query(qp, stmt, ql);
    }

    // --- Execute (sync, dual) ---

    @Test
    public void testExecuteSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.execute(wp, key, "pkg", "func")).thenReturn("result");
        Object result = dualClient.execute(wp, key, "pkg", "func");
        Assert.assertEquals("result", result);
    }

    @Test
    public void testExecuteDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(primaryClient.execute(wp, key, "pkg", "func")).thenReturn("result");
        Object result = dualClient.execute(wp, key, "pkg", "func");
        Assert.assertEquals("result", result);
        verify(secondaryClient).execute(wp, key, "pkg", "func");
    }

    @Test
    public void testExecuteAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        ExecuteListener l = mock(ExecuteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.execute(el, l, wp, key, "pkg", "func");
        verify(primaryClient).execute(el, l, wp, key, "pkg", "func");
    }

    @Test
    public void testExecuteAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        ExecuteListener l = mock(ExecuteListener.class);
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        dualClient.execute(el, l, wp, key, "pkg", "func");
        verify(primaryClient).execute(eq(el), any(ExecuteListener.class), eq(wp), eq(key), eq("pkg"), eq("func"));
    }

    // --- Unsupported operations in dual mode ---

    @Test(expected = AerospikeBundleException.class)
    public void testRegisterThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.register(null, "path", "serverPath", Language.LUA);
    }

    @Test
    public void testRegisterWorksInSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        when(primaryClient.register(any(), anyString(), anyString(), any(Language.class))).thenReturn(null);
        dualClient.register(null, "path", "serverPath", Language.LUA);
        verify(primaryClient).register(null, "path", "serverPath", Language.LUA);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testRegisterWithClassLoaderThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.register(null, Thread.currentThread().getContextClassLoader(), "path", "serverPath", Language.LUA);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testRegisterUdfStringThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.registerUdfString(null, "code", "serverPath", Language.LUA);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testRemoveUdfThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.removeUdf(new InfoPolicy(), "module");
    }

    @Test
    public void testRemoveUdfSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        InfoPolicy ip = new InfoPolicy();
        dualClient.removeUdf(ip, "module");
        verify(primaryClient).removeUdf(ip, "module");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testExecuteStatementThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.execute(new WritePolicy(), new Statement(), "pkg", "func");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testExecuteStatementOpsThrowsInDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.execute(new WritePolicy(), new Statement(), Operation.get());
    }

    // --- getNode, getClusterStats, getCluster ---

    @Test
    public void testGetNode() {
        Node node = mock(Node.class);
        when(primaryClient.getNode("node1")).thenReturn(node);
        Assert.assertSame(node, dualClient.getNode("node1"));
    }

    @Test
    public void testGetClusterStats() {
        when(primaryClient.getClusterStats()).thenReturn(null);
        Assert.assertNull(dualClient.getClusterStats());
        verify(primaryClient).getClusterStats();
    }

    @Test
    public void testGetCluster() {
        when(primaryClient.getCluster()).thenReturn(null);
        Assert.assertNull(dualClient.getCluster());
        verify(primaryClient).getCluster();
    }

    // --- scanPartitions ---

    @Test
    public void testScanPartitions() {
        ScanPolicy sp = new ScanPolicy();
        PartitionFilter pf = PartitionFilter.all();
        ScanCallback cb = mock(ScanCallback.class);
        dualClient.scanPartitions(sp, pf, "ns", "set", cb, "bin1");
        verify(primaryClient).scanPartitions(sp, pf, "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanPartitionsAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener rsl = mock(RecordSequenceListener.class);
        ScanPolicy sp = new ScanPolicy();
        PartitionFilter pf = PartitionFilter.all();
        dualClient.scanPartitions(el, rsl, sp, pf, "ns", "set", "bin1");
        verify(primaryClient).scanPartitions(el, rsl, sp, pf, "ns", "set", "bin1");
    }

    // --- queryAsync ---

    @Test
    public void testQueryAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener rsl = mock(RecordSequenceListener.class);
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        dualClient.query(el, rsl, qp, stmt);
        verify(primaryClient).query(el, rsl, qp, stmt);
    }

    // --- queryWithPartitionFilter ---

    @Test
    public void testQueryWithPartitionFilter() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        PartitionFilter pf = PartitionFilter.all();
        QueryListener ql = mock(QueryListener.class);
        dualClient.query(qp, stmt, pf, ql);
        verify(primaryClient).query(qp, stmt, pf, ql);
    }

    // --- queryNode ---

    @Test
    public void testQueryNode() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        Node node = mock(Node.class);
        when(primaryClient.queryNode(qp, stmt, node)).thenReturn(null);
        Assert.assertNull(dualClient.queryNode(qp, stmt, node));
    }

    // --- queryPartitions ---

    @Test
    public void testQueryPartitions() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        PartitionFilter pf = PartitionFilter.all();
        when(primaryClient.queryPartitions(qp, stmt, pf)).thenReturn(null);
        Assert.assertNull(dualClient.queryPartitions(qp, stmt, pf));
    }

    @Test
    public void testQueryPartitionsAsync() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener rsl = mock(RecordSequenceListener.class);
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        PartitionFilter pf = PartitionFilter.all();
        dualClient.queryPartitions(el, rsl, qp, stmt, pf);
        verify(primaryClient).queryPartitions(el, rsl, qp, stmt, pf);
    }

    // --- queryAggregate ---

    @Test
    public void testQueryAggregate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        when(primaryClient.queryAggregate(qp, stmt, "pkg", "func", Value.get(1))).thenReturn(null);
        Assert.assertNull(dualClient.queryAggregate(qp, stmt, "pkg", "func", Value.get(1)));
    }

    @Test
    public void testQueryAggregateNoArgs() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        when(primaryClient.queryAggregate(qp, stmt)).thenReturn(null);
        Assert.assertNull(dualClient.queryAggregate(qp, stmt));
    }

    @Test
    public void testQueryAggregateNode() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        Node node = mock(Node.class);
        when(primaryClient.queryAggregateNode(qp, stmt, node)).thenReturn(null);
        Assert.assertNull(dualClient.queryAggregateNode(qp, stmt, node));
    }

    // --- createIndex ---

    @Test
    public void testCreateIndexSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Policy p = new Policy();
        when(primaryClient.createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC)).thenReturn(null);
        dualClient.createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC);
        verify(primaryClient).createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateIndexDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createIndex(new Policy(), "ns", "set", "idx", "bin", IndexType.NUMERIC);
    }

    @Test
    public void testCreateIndexWithCollectionSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Policy p = new Policy();
        when(primaryClient.createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST)).thenReturn(null);
        dualClient.createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
        verify(primaryClient).createIndex(p, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateIndexWithCollectionDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createIndex(new Policy(), "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
    }

    @Test
    public void testCreateIndexAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        IndexListener il = mock(IndexListener.class);
        Policy p = new Policy();
        dualClient.createIndex(el, il, p, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
        verify(primaryClient).createIndex(el, il, p, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateIndexAsyncDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createIndex(mock(EventLoop.class), mock(IndexListener.class), new Policy(), "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.LIST);
    }

    // --- dropIndex ---

    @Test
    public void testDropIndexSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        Policy p = new Policy();
        when(primaryClient.dropIndex(p, "ns", "set", "idx")).thenReturn(null);
        dualClient.dropIndex(p, "ns", "set", "idx");
        verify(primaryClient).dropIndex(p, "ns", "set", "idx");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testDropIndexDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.dropIndex(new Policy(), "ns", "set", "idx");
    }

    @Test
    public void testDropIndexAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        IndexListener il = mock(IndexListener.class);
        Policy p = new Policy();
        dualClient.dropIndex(el, il, p, "ns", "set", "idx");
        verify(primaryClient).dropIndex(el, il, p, "ns", "set", "idx");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testDropIndexAsyncDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.dropIndex(mock(EventLoop.class), mock(IndexListener.class), new Policy(), "ns", "set", "idx");
    }

    // --- info ---

    @Test
    public void testInfo() {
        EventLoop el = mock(EventLoop.class);
        InfoListener il = mock(InfoListener.class);
        InfoPolicy ip = new InfoPolicy();
        Node node = mock(Node.class);
        dualClient.info(el, il, ip, node, "cmd1");
        verify(primaryClient).info(el, il, ip, node, "cmd1");
    }

    // --- setXDRFilter ---

    @Test
    public void testSetXDRFilterSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        InfoPolicy ip = new InfoPolicy();
        dualClient.setXDRFilter(ip, "dc", "ns", null);
        verify(primaryClient).setXDRFilter(ip, "dc", "ns", null);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testSetXDRFilterDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.setXDRFilter(new InfoPolicy(), "dc", "ns", null);
    }

    // --- Admin operations ---

    @Test
    public void testCreateUserSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Collections.singletonList("role1");
        dualClient.createUser(ap, "user1", "pass1", roles);
        verify(primaryClient).createUser(ap, "user1", "pass1", roles);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateUserDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createUser(new AdminPolicy(), "user1", "pass1", Collections.singletonList("role1"));
    }

    @Test
    public void testDropUserSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        dualClient.dropUser(ap, "user1");
        verify(primaryClient).dropUser(ap, "user1");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testDropUserDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.dropUser(new AdminPolicy(), "user1");
    }

    @Test
    public void testChangePasswordSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        dualClient.changePassword(ap, "user1", "newpass");
        verify(primaryClient).changePassword(ap, "user1", "newpass");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testChangePasswordDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.changePassword(new AdminPolicy(), "user1", "newpass");
    }

    @Test
    public void testGrantRolesSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Collections.singletonList("role1");
        dualClient.grantRoles(ap, "user1", roles);
        verify(primaryClient).grantRoles(ap, "user1", roles);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testGrantRolesDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.grantRoles(new AdminPolicy(), "user1", Collections.singletonList("role1"));
    }

    @Test
    public void testRevokeRolesSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Collections.singletonList("role1");
        dualClient.revokeRoles(ap, "user1", roles);
        verify(primaryClient).revokeRoles(ap, "user1", roles);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testRevokeRolesDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.revokeRoles(new AdminPolicy(), "user1", Collections.singletonList("role1"));
    }

    @Test
    public void testCreateRoleSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = Collections.emptyList();
        dualClient.createRole(ap, "role1", privs);
        verify(primaryClient).createRole(ap, "role1", privs);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateRoleDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createRole(new AdminPolicy(), "role1", Collections.emptyList());
    }

    @Test
    public void testCreateRoleWithWhitelistSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = Collections.emptyList();
        List<String> whitelist = Collections.singletonList("10.0.0.1");
        dualClient.createRole(ap, "role1", privs, whitelist);
        verify(primaryClient).createRole(ap, "role1", privs, whitelist);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateRoleWithWhitelistDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createRole(new AdminPolicy(), "role1", Collections.emptyList(), Collections.singletonList("10.0.0.1"));
    }

    @Test
    public void testCreateRoleWithQuotasSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = Collections.emptyList();
        List<String> whitelist = Collections.singletonList("10.0.0.1");
        dualClient.createRole(ap, "role1", privs, whitelist, 100, 200);
        verify(primaryClient).createRole(ap, "role1", privs, whitelist, 100, 200);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testCreateRoleWithQuotasDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.createRole(new AdminPolicy(), "role1", Collections.emptyList(), Collections.singletonList("10.0.0.1"), 100, 200);
    }

    @Test
    public void testDropRoleSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        dualClient.dropRole(ap, "role1");
        verify(primaryClient).dropRole(ap, "role1");
    }

    @Test(expected = AerospikeBundleException.class)
    public void testDropRoleDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.dropRole(new AdminPolicy(), "role1");
    }

    @Test
    public void testGrantPrivilegesSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = Collections.emptyList();
        dualClient.grantPrivileges(ap, "role1", privs);
        verify(primaryClient).grantPrivileges(ap, "role1", privs);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testGrantPrivilegesDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.grantPrivileges(new AdminPolicy(), "role1", Collections.emptyList());
    }

    @Test
    public void testRevokePrivilegesSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = Collections.emptyList();
        dualClient.revokePrivileges(ap, "role1", privs);
        verify(primaryClient).revokePrivileges(ap, "role1", privs);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testRevokePrivilegesDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.revokePrivileges(new AdminPolicy(), "role1", Collections.emptyList());
    }

    @Test
    public void testSetWhitelistSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        List<String> whitelist = Collections.singletonList("10.0.0.1");
        dualClient.setWhitelist(ap, "role1", whitelist);
        verify(primaryClient).setWhitelist(ap, "role1", whitelist);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testSetWhitelistDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.setWhitelist(new AdminPolicy(), "role1", Collections.singletonList("10.0.0.1"));
    }

    @Test
    public void testSetQuotasSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        AdminPolicy ap = new AdminPolicy();
        dualClient.setQuotas(ap, "role1", 100, 200);
        verify(primaryClient).setQuotas(ap, "role1", 100, 200);
    }

    @Test(expected = AerospikeBundleException.class)
    public void testSetQuotasDualModeThrows() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        dualClient.setQuotas(new AdminPolicy(), "role1", 100, 200);
    }

    // --- queryUser/queryUsers/queryRole/queryRoles ---

    @Test
    public void testQueryUser() {
        AdminPolicy ap = new AdminPolicy();
        when(primaryClient.queryUser(ap, "user1")).thenReturn(null);
        Assert.assertNull(dualClient.queryUser(ap, "user1"));
        verify(primaryClient).queryUser(ap, "user1");
    }

    @Test
    public void testQueryUsers() {
        AdminPolicy ap = new AdminPolicy();
        when(primaryClient.queryUsers(ap)).thenReturn(null);
        Assert.assertNull(dualClient.queryUsers(ap));
        verify(primaryClient).queryUsers(ap);
    }

    @Test
    public void testQueryRole() {
        AdminPolicy ap = new AdminPolicy();
        when(primaryClient.queryRole(ap, "role1")).thenReturn(null);
        Assert.assertNull(dualClient.queryRole(ap, "role1"));
        verify(primaryClient).queryRole(ap, "role1");
    }

    @Test
    public void testQueryRoles() {
        AdminPolicy ap = new AdminPolicy();
        when(primaryClient.queryRoles(ap)).thenReturn(null);
        Assert.assertNull(dualClient.queryRoles(ap));
        verify(primaryClient).queryRoles(ap);
    }

    // --- Batch delete async dual mode ---

    @Test
    public void testDeleteBatchArrayAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, listener, bp, bdp, keys);
        verify(primaryClient).delete(eq(el), any(BatchRecordArrayListener.class), eq(bp), eq(bdp), eq(keys));
    }

    @Test
    public void testDeleteBatchSequenceAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.delete(el, listener, bp, bdp, keys);
        verify(primaryClient).delete(eq(el), any(BatchRecordSequenceListener.class), eq(bp), eq(bdp), eq(keys));
    }

    // --- Operate batch keys async dual mode ---

    @Test
    public void testOperateBatchKeysArrayAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.operate(el, listener, bp, bwp, keys, Operation.get());
        verify(primaryClient).operate(eq(el), any(BatchRecordArrayListener.class), eq(bp), eq(bwp), eq(keys), any(Operation.class));
    }

    @Test
    public void testOperateBatchKeysSequenceAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.operate(el, listener, bp, bwp, keys, Operation.get());
        verify(primaryClient).operate(eq(el), any(BatchRecordSequenceListener.class), eq(bp), eq(bwp), eq(keys), any(Operation.class));
    }

    // --- Execute batch async ---

    @Test
    public void testExecuteBatchArrayAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
        verify(primaryClient).execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
    }

    @Test
    public void testExecuteBatchArrayAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
        verify(primaryClient).execute(eq(el), any(BatchRecordArrayListener.class), eq(bp), eq(bup), eq(keys), eq("pkg"), eq("func"), eq(Value.get(1)));
    }

    @Test
    public void testExecuteBatchSequenceAsyncSingleMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(singleConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
        verify(primaryClient).execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
    }

    @Test
    public void testExecuteBatchSequenceAsyncDualMode() {
        when(resolver.getDualModeASReadWriteConfig()).thenReturn(dualConfig);
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        dualClient.execute(el, listener, bp, bup, keys, "pkg", "func", Value.get(1));
        verify(primaryClient).execute(eq(el), any(BatchRecordSequenceListener.class), eq(bp), eq(bup), eq(keys), eq("pkg"), eq("func"), eq(Value.get(1)));
    }
}
