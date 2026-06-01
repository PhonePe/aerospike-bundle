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
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import com.codahale.metrics.MetricRegistry;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static org.mockito.Mockito.*;

public class AerospikeClientWithMetricsTest {

    private IAerospikeClient delegate;
    private AerospikeInterceptor interceptor;
    private MetricRegistry metricRegistry;
    private AerospikeClientWithMetrics client;

    @Before
    public void setUp() {
        delegate = mock(IAerospikeClient.class);
        metricRegistry = new MetricRegistry();
        // Create interceptor that just passes through
        interceptor = new AerospikeInterceptor() {
            @Override
            public <T> T execute(AerospikeInterceptorContext context, Function<AerospikeInterceptorContext, T> supplier) {
                return supplier.apply(context);
            }
        };
        client = new AerospikeClientWithMetrics(delegate, interceptor, metricRegistry, true, true);
    }

    // --- Policy delegation ---

    @Test
    public void testGetReadPolicyDefault() {
        Policy policy = new Policy();
        when(delegate.getReadPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getReadPolicyDefault());
    }

    @Test
    public void testGetWritePolicyDefault() {
        WritePolicy policy = new WritePolicy();
        when(delegate.getWritePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getWritePolicyDefault());
    }

    @Test
    public void testGetScanPolicyDefault() {
        ScanPolicy policy = new ScanPolicy();
        when(delegate.getScanPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getScanPolicyDefault());
    }

    @Test
    public void testGetQueryPolicyDefault() {
        QueryPolicy policy = new QueryPolicy();
        when(delegate.getQueryPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getQueryPolicyDefault());
    }

    @Test
    public void testGetBatchPolicyDefault() {
        BatchPolicy policy = new BatchPolicy();
        when(delegate.getBatchPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getBatchPolicyDefault());
    }

    @Test
    public void testGetBatchParentPolicyWriteDefault() {
        BatchPolicy policy = new BatchPolicy();
        when(delegate.getBatchParentPolicyWriteDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getBatchParentPolicyWriteDefault());
    }

    @Test
    public void testGetBatchWritePolicyDefault() {
        BatchWritePolicy policy = new BatchWritePolicy();
        when(delegate.getBatchWritePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getBatchWritePolicyDefault());
    }

    @Test
    public void testGetBatchDeletePolicyDefault() {
        BatchDeletePolicy policy = new BatchDeletePolicy();
        when(delegate.getBatchDeletePolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getBatchDeletePolicyDefault());
    }

    @Test
    public void testGetBatchUDFPolicyDefault() {
        BatchUDFPolicy policy = new BatchUDFPolicy();
        when(delegate.getBatchUDFPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getBatchUDFPolicyDefault());
    }

    @Test
    public void testGetInfoPolicyDefault() {
        InfoPolicy policy = new InfoPolicy();
        when(delegate.getInfoPolicyDefault()).thenReturn(policy);
        Assert.assertSame(policy, client.getInfoPolicyDefault());
    }

    // --- Connection/node delegation ---

    @Test
    public void testClose() {
        client.close();
        verify(delegate).close();
    }

    @Test
    public void testIsConnected() {
        when(delegate.isConnected()).thenReturn(true);
        Assert.assertTrue(client.isConnected());
    }

    @Test
    public void testGetNodes() {
        Node[] nodes = new Node[]{};
        when(delegate.getNodes()).thenReturn(nodes);
        Assert.assertSame(nodes, client.getNodes());
    }

    @Test
    public void testGetNodeNames() {
        List<String> names = Arrays.asList("n1");
        when(delegate.getNodeNames()).thenReturn(names);
        Assert.assertEquals(names, client.getNodeNames());
    }

    // --- Write operations with interceptor ---

    @Test
    public void testPutCallsDelegateViaInterceptor() {
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "hello");
        WritePolicy wp = new WritePolicy();

        client.put(wp, key, bin);
        verify(delegate).put(wp, key, bin);
    }

    @Test
    public void testPutWithByteBins() {
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", new byte[]{1, 2, 3});
        WritePolicy wp = new WritePolicy();

        client.put(wp, key, bin);
        verify(delegate).put(wp, key, bin);
    }

    // --- Read operations with interceptor ---

    @Test
    public void testGetCallsDelegateViaInterceptor() {
        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        Record testRecord = new Record(null, 1, 1);
        when(delegate.get(policy, key)).thenReturn(testRecord);

        Record result = client.get(policy, key);
        Assert.assertSame(testRecord, result);
    }

    @Test
    public void testGetWithBinsCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        Record testRecord = new Record(null, 1, 1);
        when(delegate.get(policy, key, "bin1")).thenReturn(testRecord);

        Record result = client.get(policy, key, "bin1");
        Assert.assertSame(testRecord, result);
    }

    @Test
    public void testExistsCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        when(delegate.exists(policy, key)).thenReturn(true);

        Assert.assertTrue(client.exists(policy, key));
    }

    // --- Delete ---

    @Test
    public void testDeleteCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(delegate.delete(wp, key)).thenReturn(true);

        Assert.assertTrue(client.delete(wp, key));
    }

    // --- Append ---

    @Test
    public void testAppendCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "val");
        WritePolicy wp = new WritePolicy();

        client.append(wp, key, bin);
        verify(delegate).append(wp, key, bin);
    }

    // --- Touch ---

    @Test
    public void testTouchCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();

        client.touch(wp, key);
        verify(delegate).touch(wp, key);
    }

    // --- Prepend ---

    @Test
    public void testPrependCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "val");
        WritePolicy wp = new WritePolicy();
        client.prepend(wp, key, bin);
        verify(delegate).prepend(wp, key, bin);
    }

    // --- Add ---

    @Test
    public void testAddCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", 1);
        WritePolicy wp = new WritePolicy();
        client.add(wp, key, bin);
        verify(delegate).add(wp, key, bin);
    }

    // --- GetHeader ---

    @Test
    public void testGetHeaderCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        Record testRecord = new Record(null, 1, 1);
        when(delegate.getHeader(policy, key)).thenReturn(testRecord);
        Assert.assertSame(testRecord, client.getHeader(policy, key));
    }

    // --- Truncate ---

    @Test
    public void testTruncateCallsDelegate() {
        InfoPolicy ip = new InfoPolicy();
        client.truncate(ip, "ns", "set", null);
        verify(delegate).truncate(ip, "ns", "set", null);
    }

    // --- Batch operations ---

    @Test
    public void testBatchDeleteCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(delegate.delete(bp, bdp, keys)).thenReturn(br);
        Assert.assertSame(br, client.delete(bp, bdp, keys));
    }

    @Test
    public void testBatchExistsCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        when(delegate.exists(bp, keys)).thenReturn(new boolean[]{true});
        Assert.assertTrue(client.exists(bp, keys)[0]);
    }

    @Test
    public void testBatchGetListCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        when(delegate.get(bp, list)).thenReturn(true);
        Assert.assertTrue(client.get(bp, list));
    }

    @Test
    public void testBatchGetKeysCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(delegate.get(bp, keys)).thenReturn(records);
        Assert.assertSame(records, client.get(bp, keys));
    }

    @Test
    public void testBatchGetKeysWithBinsCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(delegate.get(bp, keys, "bin1")).thenReturn(records);
        Assert.assertSame(records, client.get(bp, keys, "bin1"));
    }

    @Test
    public void testBatchGetKeysWithOpsCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(delegate.get(bp, keys, op)).thenReturn(records);
        Assert.assertSame(records, client.get(bp, keys, op));
    }

    @Test
    public void testBatchGetHeaderCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Record[] records = new Record[]{new Record(null, 1, 1)};
        when(delegate.getHeader(bp, keys)).thenReturn(records);
        Assert.assertSame(records, client.getHeader(bp, keys));
    }

    // --- Operate ---

    @Test
    public void testOperateCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        Operation op = Operation.get();
        Record testRecord = new Record(null, 1, 1);
        when(delegate.operate(wp, key, op)).thenReturn(testRecord);
        Assert.assertSame(testRecord, client.operate(wp, key, op));
    }

    @Test
    public void testOperateBatchListCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        when(delegate.operate(bp, list)).thenReturn(true);
        Assert.assertTrue(client.operate(bp, list));
    }

    @Test
    public void testOperateBatchKeysCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(delegate.operate(bp, bwp, keys, op)).thenReturn(br);
        Assert.assertSame(br, client.operate(bp, bwp, keys, op));
    }

    // --- Execute ---

    @Test
    public void testExecuteCallsDelegate() {
        Key key = new Key("ns", "set", "k1");
        WritePolicy wp = new WritePolicy();
        when(delegate.execute(wp, key, "pkg", "func")).thenReturn("result");
        Assert.assertEquals("result", client.execute(wp, key, "pkg", "func"));
    }

    // --- Scan ---

    @Test
    public void testScanAllCallsDelegate() {
        ScanPolicy sp = new ScanPolicy();
        ScanCallback cb = mock(ScanCallback.class);
        client.scanAll(sp, "ns", "set", cb, "bin1");
        verify(delegate).scanAll(sp, "ns", "set", cb, "bin1");
    }

    // --- Query ---

    @Test
    public void testQueryCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        // RecordSet is final, just verify the call is delegated
        client.query(qp, stmt);
        verify(delegate).query(qp, stmt);
    }

    // --- Size metrics disabled ---

    @Test
    public void testPutWithSizeMetricsDisabled() {
        AerospikeClientWithMetrics clientNoMetrics = new AerospikeClientWithMetrics(
                delegate, interceptor, metricRegistry, false, false);

        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "hello");
        WritePolicy wp = new WritePolicy();

        clientNoMetrics.put(wp, key, bin);
        verify(delegate).put(wp, key, bin);
    }

    // --- Context key population disabled ---

    @Test
    public void testGetWithKeyPopulationDisabled() {
        AerospikeClientWithMetrics clientNoKeys = new AerospikeClientWithMetrics(
                delegate, interceptor, metricRegistry, true, false);

        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        Record testRecord = new Record(null, 1, 1);
        when(delegate.get(policy, key)).thenReturn(testRecord);

        Assert.assertSame(testRecord, clientNoKeys.get(policy, key));
    }

    @Test
    public void testPutWithKeyPopulationDisabled() {
        AerospikeClientWithMetrics clientNoKeys = new AerospikeClientWithMetrics(
                delegate, interceptor, metricRegistry, true, false);

        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "hello");
        WritePolicy wp = new WritePolicy();

        clientNoKeys.put(wp, key, bin);
        verify(delegate).put(wp, key, bin);
    }

    @Test
    public void testBatchExistsWithEmptyKeys() {
        AerospikeClientWithMetrics clientNoKeys = new AerospikeClientWithMetrics(
                delegate, interceptor, metricRegistry, false, false);

        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{};
        when(delegate.exists(bp, keys)).thenReturn(new boolean[]{});
        Assert.assertEquals(0, clientNoKeys.exists(bp, keys).length);
    }

    // --- 1. Async write methods with EventLoop ---

    @Test
    public void testPutAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "val");
        client.put(el, wl, wp, key, bin);
        verify(delegate).put(el, wl, wp, key, bin);
    }

    @Test
    public void testAppendAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "val");
        client.append(el, wl, wp, key, bin);
        verify(delegate).append(el, wl, wp, key, bin);
    }

    @Test
    public void testPrependAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", "val");
        client.prepend(el, wl, wp, key, bin);
        verify(delegate).prepend(el, wl, wp, key, bin);
    }

    @Test
    public void testAddAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Bin bin = new Bin("b1", 1);
        client.add(el, wl, wp, key, bin);
        verify(delegate).add(el, wl, wp, key, bin);
    }

    // --- 2. Async delete methods ---

    @Test
    public void testDeleteAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        DeleteListener dl = mock(DeleteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        client.delete(el, dl, wp, key);
        verify(delegate).delete(el, dl, wp, key);
    }

    @Test
    public void testDeleteAsyncBatchArrayCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.delete(el, listener, bp, bdp, keys);
        verify(delegate).delete(el, listener, bp, bdp, keys);
    }

    @Test
    public void testDeleteAsyncBatchSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchDeletePolicy bdp = new BatchDeletePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.delete(el, listener, bp, bdp, keys);
        verify(delegate).delete(el, listener, bp, bdp, keys);
    }

    // --- 3. Async touch/exists ---

    @Test
    public void testTouchAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        WriteListener wl = mock(WriteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        client.touch(el, wl, wp, key);
        verify(delegate).touch(el, wl, wp, key);
    }

    @Test
    public void testExistsAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        ExistsListener listener = mock(ExistsListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        client.exists(el, listener, policy, key);
        verify(delegate).exists(el, listener, policy, key);
    }

    @Test
    public void testExistsAsyncArrayCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        ExistsArrayListener listener = mock(ExistsArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.exists(el, listener, bp, keys);
        verify(delegate).exists(el, listener, bp, keys);
    }

    @Test
    public void testExistsAsyncSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        ExistsSequenceListener listener = mock(ExistsSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.exists(el, listener, bp, keys);
        verify(delegate).exists(el, listener, bp, keys);
    }

    // --- 4. Async get methods ---

    @Test
    public void testGetAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        client.get(el, rl, policy, key);
        verify(delegate).get(el, rl, policy, key);
    }

    @Test
    public void testGetAsyncWithBinsCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        client.get(el, rl, policy, key, "bin1");
        verify(delegate).get(el, rl, policy, key, "bin1");
    }

    @Test
    public void testGetHeaderAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        Policy policy = new Policy();
        Key key = new Key("ns", "set", "k1");
        client.getHeader(el, rl, policy, key);
        verify(delegate).getHeader(el, rl, policy, key);
    }

    @Test
    public void testGetAsyncBatchListCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchListListener listener = mock(BatchListListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        client.get(el, listener, bp, list);
        verify(delegate).get(el, listener, bp, list);
    }

    @Test
    public void testGetAsyncBatchSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchSequenceListener listener = mock(BatchSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRead> list = new ArrayList<>();
        client.get(el, listener, bp, list);
        verify(delegate).get(el, listener, bp, list);
    }

    @Test
    public void testGetAsyncRecordArrayCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener listener = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.get(el, listener, bp, keys);
        verify(delegate).get(el, listener, bp, keys);
    }

    @Test
    public void testGetAsyncRecordSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.get(el, listener, bp, keys);
        verify(delegate).get(el, listener, bp, keys);
    }

    @Test
    public void testGetAsyncRecordArrayWithBinsCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener listener = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.get(el, listener, bp, keys, "bin1");
        verify(delegate).get(el, listener, bp, keys, "bin1");
    }

    @Test
    public void testGetAsyncRecordSequenceWithBinsCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.get(el, listener, bp, keys, "bin1");
        verify(delegate).get(el, listener, bp, keys, "bin1");
    }

    @Test
    public void testGetAsyncRecordArrayWithOpsCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener listener = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        client.get(el, listener, bp, keys, op);
        verify(delegate).get(el, listener, bp, keys, op);
    }

    @Test
    public void testGetAsyncRecordSequenceWithOpsCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        client.get(el, listener, bp, keys, op);
        verify(delegate).get(el, listener, bp, keys, op);
    }

    @Test
    public void testGetHeaderAsyncArrayCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordArrayListener listener = mock(RecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.getHeader(el, listener, bp, keys);
        verify(delegate).getHeader(el, listener, bp, keys);
    }

    @Test
    public void testGetHeaderAsyncSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        client.getHeader(el, listener, bp, keys);
        verify(delegate).getHeader(el, listener, bp, keys);
    }

    // --- 5. Async operate ---

    @Test
    public void testOperateAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordListener rl = mock(RecordListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Operation op = Operation.get();
        client.operate(el, rl, wp, key, op);
        verify(delegate).operate(el, rl, wp, key, op);
    }

    @Test
    public void testOperateAsyncBatchListCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchOperateListListener listener = mock(BatchOperateListListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        client.operate(el, listener, bp, list);
        verify(delegate).operate(el, listener, bp, list);
    }

    @Test
    public void testOperateAsyncBatchSequenceListCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        List<BatchRecord> list = new ArrayList<>();
        client.operate(el, listener, bp, list);
        verify(delegate).operate(el, listener, bp, list);
    }

    @Test
    public void testOperateAsyncBatchArrayKeysCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        client.operate(el, listener, bp, bwp, keys, op);
        verify(delegate).operate(el, listener, bp, bwp, keys, op);
    }

    @Test
    public void testOperateAsyncBatchSequenceKeysCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchWritePolicy bwp = new BatchWritePolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Operation op = Operation.get();
        client.operate(el, listener, bp, bwp, keys, op);
        verify(delegate).operate(el, listener, bp, bwp, keys, op);
    }

    // --- 6. Scan variants ---

    @Test
    public void testScanAllAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        ScanPolicy sp = new ScanPolicy();
        client.scanAll(el, listener, sp, "ns", "set", "bin1");
        verify(delegate).scanAll(el, listener, sp, "ns", "set", "bin1");
    }

    @Test
    public void testScanNodeByNameCallsDelegate() {
        ScanPolicy sp = new ScanPolicy();
        ScanCallback cb = mock(ScanCallback.class);
        client.scanNode(sp, "node1", "ns", "set", cb, "bin1");
        verify(delegate).scanNode(sp, "node1", "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanNodeByNodeCallsDelegate() {
        ScanPolicy sp = new ScanPolicy();
        Node node = mock(Node.class);
        ScanCallback cb = mock(ScanCallback.class);
        client.scanNode(sp, node, "ns", "set", cb, "bin1");
        verify(delegate).scanNode(sp, node, "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanPartitionsCallsDelegate() {
        ScanPolicy sp = new ScanPolicy();
        PartitionFilter pf = PartitionFilter.all();
        ScanCallback cb = mock(ScanCallback.class);
        client.scanPartitions(sp, pf, "ns", "set", cb, "bin1");
        verify(delegate).scanPartitions(sp, pf, "ns", "set", cb, "bin1");
    }

    @Test
    public void testScanPartitionsAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        ScanPolicy sp = new ScanPolicy();
        PartitionFilter pf = PartitionFilter.all();
        client.scanPartitions(el, listener, sp, pf, "ns", "set", "bin1");
        verify(delegate).scanPartitions(el, listener, sp, pf, "ns", "set", "bin1");
    }

    // --- 7. UDF methods ---

    @Test
    public void testRegisterCallsDelegate() {
        Policy policy = new Policy();
        client.register(policy, "path", "server", Language.LUA);
        verify(delegate).register(policy, "path", "server", Language.LUA);
    }

    @Test
    public void testRegisterWithClassLoaderCallsDelegate() {
        Policy policy = new Policy();
        ClassLoader cl = getClass().getClassLoader();
        client.register(policy, cl, "path", "server", Language.LUA);
        verify(delegate).register(policy, cl, "path", "server", Language.LUA);
    }

    @Test
    public void testRegisterUdfStringCallsDelegate() {
        Policy policy = new Policy();
        client.registerUdfString(policy, "code", "server", Language.LUA);
        verify(delegate).registerUdfString(policy, "code", "server", Language.LUA);
    }

    @Test
    public void testRemoveUdfCallsDelegate() {
        InfoPolicy ip = new InfoPolicy();
        client.removeUdf(ip, "module");
        verify(delegate).removeUdf(ip, "module");
    }

    // --- 8. Execute methods ---

    @Test
    public void testExecuteAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        ExecuteListener listener = mock(ExecuteListener.class);
        WritePolicy wp = new WritePolicy();
        Key key = new Key("ns", "set", "k1");
        Value val = Value.get("val");
        client.execute(el, listener, wp, key, "pkg", "func", val);
        verify(delegate).execute(el, listener, wp, key, "pkg", "func", val);
    }

    @Test
    public void testExecuteBatchCallsDelegate() {
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Value val = Value.get("val");
        BatchResults br = new BatchResults(new BatchRecord[0], true);
        when(delegate.execute(bp, bup, keys, "pkg", "func", val)).thenReturn(br);
        Assert.assertSame(br, client.execute(bp, bup, keys, "pkg", "func", val));
    }

    @Test
    public void testExecuteBatchAsyncArrayCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordArrayListener listener = mock(BatchRecordArrayListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Value val = Value.get("val");
        client.execute(el, listener, bp, bup, keys, "pkg", "func", val);
        verify(delegate).execute(el, listener, bp, bup, keys, "pkg", "func", val);
    }

    @Test
    public void testExecuteBatchAsyncSequenceCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        BatchRecordSequenceListener listener = mock(BatchRecordSequenceListener.class);
        BatchPolicy bp = new BatchPolicy();
        BatchUDFPolicy bup = new BatchUDFPolicy();
        Key[] keys = new Key[]{new Key("ns", "set", "k1")};
        Value val = Value.get("val");
        client.execute(el, listener, bp, bup, keys, "pkg", "func", val);
        verify(delegate).execute(el, listener, bp, bup, keys, "pkg", "func", val);
    }

    @Test
    public void testExecuteStatementCallsDelegate() {
        WritePolicy wp = new WritePolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        Value val = Value.get("val");
        client.execute(wp, stmt, "pkg", "func", val);
        verify(delegate).execute(wp, stmt, "pkg", "func", val);
    }

    @Test
    public void testExecuteStatementWithOpsCallsDelegate() {
        WritePolicy wp = new WritePolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        Operation op = Operation.get();
        client.execute(wp, stmt, op);
        verify(delegate).execute(wp, stmt, op);
    }

    // --- 9. Query variants ---

    @Test
    public void testQueryAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        client.query(el, listener, qp, stmt);
        verify(delegate).query(el, listener, qp, stmt);
    }

    @Test
    public void testQueryWithListenerCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        QueryListener ql = mock(QueryListener.class);
        client.query(qp, stmt, ql);
        verify(delegate).query(qp, stmt, ql);
    }

    @Test
    public void testQueryWithPartitionFilterAndListenerCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        PartitionFilter pf = PartitionFilter.all();
        QueryListener ql = mock(QueryListener.class);
        client.query(qp, stmt, pf, ql);
        verify(delegate).query(qp, stmt, pf, ql);
    }

    @Test
    public void testQueryNodeCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        Node node = mock(Node.class);
        client.queryNode(qp, stmt, node);
        verify(delegate).queryNode(qp, stmt, node);
    }

    @Test
    public void testQueryPartitionsCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        PartitionFilter pf = PartitionFilter.all();
        client.queryPartitions(qp, stmt, pf);
        verify(delegate).queryPartitions(qp, stmt, pf);
    }

    @Test
    public void testQueryPartitionsAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        RecordSequenceListener listener = mock(RecordSequenceListener.class);
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        PartitionFilter pf = PartitionFilter.all();
        client.queryPartitions(el, listener, qp, stmt, pf);
        verify(delegate).queryPartitions(el, listener, qp, stmt, pf);
    }

    @Test
    public void testQueryAggregateWithUdfCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        Value val = Value.get("val");
        client.queryAggregate(qp, stmt, "pkg", "func", val);
        verify(delegate).queryAggregate(qp, stmt, "pkg", "func", val);
    }

    @Test
    public void testQueryAggregateCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        client.queryAggregate(qp, stmt);
        verify(delegate).queryAggregate(qp, stmt);
    }

    @Test
    public void testQueryAggregateNodeCallsDelegate() {
        QueryPolicy qp = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace("ns");
        stmt.setSetName("set");
        Node node = mock(Node.class);
        client.queryAggregateNode(qp, stmt, node);
        verify(delegate).queryAggregateNode(qp, stmt, node);
    }

    // --- 10. Index methods ---

    @Test
    public void testCreateIndexCallsDelegate() {
        Policy policy = new Policy();
        client.createIndex(policy, "ns", "set", "idx", "bin", IndexType.NUMERIC);
        verify(delegate).createIndex(policy, "ns", "set", "idx", "bin", IndexType.NUMERIC);
    }

    @Test
    public void testCreateIndexWithCollectionTypeCallsDelegate() {
        Policy policy = new Policy();
        client.createIndex(policy, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        verify(delegate).createIndex(policy, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.DEFAULT);
    }

    @Test
    public void testCreateIndexAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        IndexListener listener = mock(IndexListener.class);
        Policy policy = new Policy();
        client.createIndex(el, listener, policy, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        verify(delegate).createIndex(el, listener, policy, "ns", "set", "idx", "bin", IndexType.NUMERIC, IndexCollectionType.DEFAULT);
    }

    @Test
    public void testDropIndexCallsDelegate() {
        Policy policy = new Policy();
        client.dropIndex(policy, "ns", "set", "idx");
        verify(delegate).dropIndex(policy, "ns", "set", "idx");
    }

    @Test
    public void testDropIndexAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        IndexListener listener = mock(IndexListener.class);
        Policy policy = new Policy();
        client.dropIndex(el, listener, policy, "ns", "set", "idx");
        verify(delegate).dropIndex(el, listener, policy, "ns", "set", "idx");
    }

    // --- 11. Info/XDR ---

    @Test
    public void testInfoAsyncCallsDelegate() {
        EventLoop el = mock(EventLoop.class);
        InfoListener listener = mock(InfoListener.class);
        InfoPolicy ip = new InfoPolicy();
        Node node = mock(Node.class);
        client.info(el, listener, ip, node, "cmd1");
        verify(delegate).info(el, listener, ip, node, "cmd1");
    }

    @Test
    public void testSetXDRFilterCallsDelegate() {
        InfoPolicy ip = new InfoPolicy();
        client.setXDRFilter(ip, "dc1", "ns", null);
        verify(delegate).setXDRFilter(ip, "dc1", "ns", null);
    }

    // --- 12. Admin methods ---

    @Test
    public void testCreateUserCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Arrays.asList("read");
        client.createUser(ap, "user1", "pass1", roles);
        verify(delegate).createUser(ap, "user1", "pass1", roles);
    }

    @Test
    public void testDropUserCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.dropUser(ap, "user1");
        verify(delegate).dropUser(ap, "user1");
    }

    @Test
    public void testChangePasswordCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.changePassword(ap, "user1", "newpass");
        verify(delegate).changePassword(ap, "user1", "newpass");
    }

    @Test
    public void testGrantRolesCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Arrays.asList("read");
        client.grantRoles(ap, "user1", roles);
        verify(delegate).grantRoles(ap, "user1", roles);
    }

    @Test
    public void testRevokeRolesCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<String> roles = Arrays.asList("read");
        client.revokeRoles(ap, "user1", roles);
        verify(delegate).revokeRoles(ap, "user1", roles);
    }

    @Test
    public void testCreateRoleCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = new ArrayList<>();
        client.createRole(ap, "role1", privs);
        verify(delegate).createRole(ap, "role1", privs);
    }

    @Test
    public void testCreateRoleWithWhitelistCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = new ArrayList<>();
        List<String> whitelist = Arrays.asList("10.0.0.1");
        client.createRole(ap, "role1", privs, whitelist);
        verify(delegate).createRole(ap, "role1", privs, whitelist);
    }

    @Test
    public void testCreateRoleWithQuotasCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = new ArrayList<>();
        List<String> whitelist = Arrays.asList("10.0.0.1");
        client.createRole(ap, "role1", privs, whitelist, 100, 200);
        verify(delegate).createRole(ap, "role1", privs, whitelist, 100, 200);
    }

    @Test
    public void testDropRoleCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.dropRole(ap, "role1");
        verify(delegate).dropRole(ap, "role1");
    }

    @Test
    public void testGrantPrivilegesCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = new ArrayList<>();
        client.grantPrivileges(ap, "role1", privs);
        verify(delegate).grantPrivileges(ap, "role1", privs);
    }

    @Test
    public void testRevokePrivilegesCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Privilege> privs = new ArrayList<>();
        client.revokePrivileges(ap, "role1", privs);
        verify(delegate).revokePrivileges(ap, "role1", privs);
    }

    @Test
    public void testSetWhitelistCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<String> whitelist = Arrays.asList("10.0.0.1");
        client.setWhitelist(ap, "role1", whitelist);
        verify(delegate).setWhitelist(ap, "role1", whitelist);
    }

    @Test
    public void testSetQuotasCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.setQuotas(ap, "role1", 100, 200);
        verify(delegate).setQuotas(ap, "role1", 100, 200);
    }

    @Test
    public void testQueryUserCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.queryUser(ap, "user1");
        verify(delegate).queryUser(ap, "user1");
    }

    @Test
    public void testQueryUsersCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<User> users = new ArrayList<>();
        when(delegate.queryUsers(ap)).thenReturn(users);
        Assert.assertSame(users, client.queryUsers(ap));
    }

    @Test
    public void testQueryRoleCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        client.queryRole(ap, "role1");
        verify(delegate).queryRole(ap, "role1");
    }

    @Test
    public void testQueryRolesCallsDelegate() {
        AdminPolicy ap = new AdminPolicy();
        List<Role> roles = new ArrayList<>();
        when(delegate.queryRoles(ap)).thenReturn(roles);
        Assert.assertSame(roles, client.queryRoles(ap));
    }

    // --- 13. Other uncovered methods ---

    @Test
    public void testGetNodeCallsDelegate() {
        Node node = mock(Node.class);
        when(delegate.getNode("node1")).thenReturn(node);
        Assert.assertSame(node, client.getNode("node1"));
    }

    @Test
    public void testGetClusterStatsCallsDelegate() {
        client.getClusterStats();
        verify(delegate).getClusterStats();
    }

    @Test
    public void testGetClusterCallsDelegate() {
        client.getCluster();
        verify(delegate).getCluster();
    }

    // --- 14. Size metrics test for get (with record) ---

    @Test
    public void testGetWithSizeMetricsRegistersGauge() {
        Key key = new Key("ns", "set", "k1");
        Policy policy = new Policy();
        Map<String, Object> bins = new HashMap<>();
        bins.put("strBin", "hello");
        bins.put("bytesBin", new byte[]{1, 2, 3, 4, 5});
        Record testRecord = new Record(bins, 1, 1);
        when(delegate.get(policy, key)).thenReturn(testRecord);

        Record result = client.get(policy, key);
        Assert.assertSame(testRecord, result);
        // Verify that size metrics were registered (gauges should exist in registry)
        Assert.assertFalse(metricRegistry.getGauges().isEmpty());
    }
}
