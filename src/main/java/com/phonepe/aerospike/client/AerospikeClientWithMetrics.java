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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.codahale.metrics.MetricRegistry;
import com.phonepe.aerospike.interceptors.AerospikeOperation;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.AerospikeInterceptorContext;
import com.phonepe.aerospike.util.ObjectUtils;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static com.phonepe.aerospike.interceptors.AerospikeOperation.ADD;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.APPEND;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_DELETE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_EXECUTE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_EXISTS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_GET;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_OPERATE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_READ;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.CHANGE_PASSWORD;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.CREATE_INDEX;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.CREATE_ROLE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.CREATE_USER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.DELETE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.DROP_INDEX;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.DROP_ROLE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.DROP_USER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.EXECUTE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.EXISTS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.GET_HEADER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.GRANT_PRIVILEGES;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.GRANT_ROLES;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.INFO;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.OPERATE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.PREPEND;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_AGGREGATE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_AGGREGATE_NODE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_NODE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_PARTITIONS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_ROLE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.QUERY_USERS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.READ;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.READ_ASYNC;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.REGISTER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.REGISTER_UDF_STRING;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.REMOVE_UDF;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.REVOKE_PRIVILEGES;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.REVOKE_ROLES;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SCAN;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SCAN_NODE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SCAN_PARTITIONS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SET_QUOTAS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SET_WHITELIST;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SET_XDR_FILTER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.TOUCH;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.TRUNCATE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.WRITE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.WRITE_ASYNC;
import static com.phonepe.aerospike.interceptors.AerospikeInterceptorContext.getInterceptorContext;

public class AerospikeClientWithMetrics implements IAerospikeClient {

    private final IAerospikeClient aerospikeClient;
    private final AerospikeInterceptor aerospikeInterceptor;

    private final MetricRegistry metricRegistry;

    //Currently only added size metrics for String and byte[] bin types. for all others, default size of 0L will be emitted.
    private final boolean sizeMetricsEnabled;

    private final boolean isInterceptorContextKeyPopulationEnabled;

    public AerospikeClientWithMetrics(IAerospikeClient aerospikeClient,
                                      AerospikeInterceptor aerospikeInterceptor,
                                      MetricRegistry metricRegistry,
                                      boolean sizeMetricsEnabled,
                                      boolean isInterceptorContextKeyPopulationEnabled) {
        this.aerospikeInterceptor = aerospikeInterceptor;
        this.aerospikeClient = aerospikeClient;
        this.metricRegistry = metricRegistry;
        this.sizeMetricsEnabled = sizeMetricsEnabled;
        this.isInterceptorContextKeyPopulationEnabled = isInterceptorContextKeyPopulationEnabled;
    }

    @Override
    public Policy getReadPolicyDefault() {
        return aerospikeClient.getReadPolicyDefault();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return aerospikeClient.getWritePolicyDefault();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return aerospikeClient.getScanPolicyDefault();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return aerospikeClient.getQueryPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return aerospikeClient.getBatchPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return aerospikeClient.getBatchParentPolicyWriteDefault();
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return aerospikeClient.getBatchWritePolicyDefault();
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return aerospikeClient.getBatchDeletePolicyDefault();
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return aerospikeClient.getBatchUDFPolicyDefault();
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return aerospikeClient.getInfoPolicyDefault();
    }

    @Override
    public void close() {
        aerospikeClient.close();
    }

    @Override
    public boolean isConnected() {
        return aerospikeClient.isConnected();
    }

    @Override
    public Node[] getNodes() {
        return aerospikeClient.getNodes();
    }

    @Override
    public List<String> getNodeNames() {
        return aerospikeClient.getNodeNames();
    }

    @Override
    public Node getNode(String s) throws AerospikeException.InvalidNode {
        return aerospikeClient.getNode(s);
    }

    @Override
    public ClusterStats getClusterStats() {
        return aerospikeClient.getClusterStats();
    }

    @Override
    public Cluster getCluster() {
        return aerospikeClient.getCluster();
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, WRITE, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            getAndMarkBinSize(key.namespace, key.setName, WRITE, updatedBins);
            aerospikeClient.put(writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, WRITE_ASYNC, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            getAndMarkBinSize(key.namespace, key.setName, WRITE, updatedBins);
            aerospikeClient.put(eventLoop, writeListener, writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void append(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, APPEND, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.append(writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, APPEND, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.append(eventLoop, writeListener, writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void prepend(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, PREPEND, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.prepend(writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, PREPEND, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.prepend(eventLoop, writeListener, writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void add(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, ADD, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.add(writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, ADD, bins), context -> {
            Bin[] updatedBins = ObjectUtils.getOrDefault(context.getRequestInfo().getBins(), bins);
            aerospikeClient.add(eventLoop, writeListener, writePolicy, key, updatedBins);
            return null;
        });
    }

    @Override
    public boolean delete(WritePolicy writePolicy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, DELETE), context -> aerospikeClient.delete(writePolicy, key));
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener deleteListener, WritePolicy writePolicy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, DELETE), context -> {
            aerospikeClient.delete(eventLoop, deleteListener, writePolicy, key);
            return null;
        });
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_DELETE),
                context -> aerospikeClient.delete(batchPolicy, batchDeletePolicy, keys));
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_DELETE), context -> {
            aerospikeClient.delete(eventLoop, batchRecordArrayListener, batchPolicy, batchDeletePolicy, keys);
            return null;
        });
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_DELETE), context -> {
            aerospikeClient.delete(eventLoop, batchRecordSequenceListener, batchPolicy, batchDeletePolicy, keys);
            return null;
        });
    }

    @Override
    public void truncate(InfoPolicy infoPolicy, String s, String s1, Calendar calendar) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, TRUNCATE), context -> {
            aerospikeClient.truncate(infoPolicy, s, s1, calendar);
            return null;
        });
    }

    @Override
    public void touch(WritePolicy writePolicy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, TOUCH), context -> {
            aerospikeClient.touch(writePolicy, key);
            return null;
        });
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, TOUCH), context -> {
            aerospikeClient.touch(eventLoop, writeListener, writePolicy, key);
            return null;
        });
    }

    @Override
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, EXISTS), context -> aerospikeClient.exists(policy, key));
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener existsListener, Policy policy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, EXISTS), context -> {
            aerospikeClient.exists(eventLoop, existsListener, policy, key);
            return null;
        });
    }

    @Override
    public boolean[] exists(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXISTS),
                context -> aerospikeClient.exists(batchPolicy, keys));
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener existsArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXISTS), context -> {
            aerospikeClient.exists(eventLoop, existsArrayListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener existsSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXISTS), context -> {
            aerospikeClient.exists(eventLoop, existsSequenceListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, READ), context -> {
            Record record = aerospikeClient.get(policy, key);
            getAndMarkRecordSize(record, key.namespace, key.setName, READ);
            return record;
        });
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, READ_ASYNC), context -> {
            aerospikeClient.get(eventLoop, recordListener, policy, key);
            return null;
        });
    }

    @Override
    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, READ), context ->
        {
            Record record = aerospikeClient.get(policy, key, binNames);
            getAndMarkRecordSize(record, key.namespace, key.setName, READ);
            return record;
        });
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, READ_ASYNC), context -> {
            aerospikeClient.get(eventLoop, recordListener, policy, key, strings);
            return null;
        });
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, GET_HEADER), context -> aerospikeClient.getHeader(policy, key));
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, GET_HEADER), context -> {
            aerospikeClient.getHeader(eventLoop, recordListener, policy, key);
            return null;
        });
    }

    @Override
    public boolean get(BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(BATCH_GET),
                context -> aerospikeClient.get(batchPolicy, list));
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener batchListListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(BATCH_GET), context -> {
            aerospikeClient.get(eventLoop, batchListListener, batchPolicy, list);
            return null;
        });
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener batchSequenceListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(BATCH_GET), context -> {
            aerospikeClient.get(eventLoop, batchSequenceListener, batchPolicy, list);
            return null;
        });
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ),
                context -> aerospikeClient.get(batchPolicy, keys));
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordArrayListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordSequenceListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ),
                context -> aerospikeClient.get(batchPolicy, keys, strings));
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordArrayListener, batchPolicy, keys, strings);
            return null;
        });
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordSequenceListener, batchPolicy, keys, strings);
            return null;
        });
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ),
                context -> aerospikeClient.get(batchPolicy, keys, operations));
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordArrayListener, batchPolicy, keys, operations);
            return null;
        });
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_READ), context -> {
            aerospikeClient.get(eventLoop, recordSequenceListener, batchPolicy, keys, operations);
            return null;
        });
    }

    @Override
    public Record[] getHeader(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, GET_HEADER),
                context -> aerospikeClient.getHeader(batchPolicy, keys));
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, GET_HEADER), context -> {
            aerospikeClient.getHeader(eventLoop, recordArrayListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, GET_HEADER), context -> {
            aerospikeClient.getHeader(eventLoop, recordSequenceListener, batchPolicy, keys);
            return null;
        });
    }

    @Override
    public Record operate(WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, OPERATE), context -> aerospikeClient.operate(writePolicy, key, operations));
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener recordListener, WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, OPERATE), context -> {
            aerospikeClient.operate(eventLoop, recordListener, writePolicy, key, operations);
            return null;
        });
    }

    @Override
    public boolean operate(BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(BATCH_OPERATE), context -> aerospikeClient.operate(batchPolicy, list));
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener batchOperateListListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(BATCH_OPERATE), context -> {
            aerospikeClient.operate(eventLoop, batchOperateListListener, batchPolicy, list);
            return null;
        });
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(BATCH_OPERATE), context -> {
            aerospikeClient.operate(eventLoop, batchRecordSequenceListener, batchPolicy, list);
            return null;
        });
    }


    @Override
    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_OPERATE),
                context ->  aerospikeClient.operate(batchPolicy, batchWritePolicy, keys, operations));
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_OPERATE), context -> {
            aerospikeClient.operate(eventLoop, batchRecordArrayListener, batchPolicy, batchWritePolicy, keys, operations);
            return null;
        });
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_OPERATE), context -> {
            aerospikeClient.operate(eventLoop, batchRecordSequenceListener, batchPolicy, batchWritePolicy, keys, operations);
            return null;
        });
    }

    @Override
    public void scanAll(ScanPolicy scanPolicy, String namespace, String setName, ScanCallback scanCallback, String... binNames) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(namespace, setName, SCAN), context -> {
            aerospikeClient.scanAll(scanPolicy, namespace, setName, scanCallback, binNames);
            return null;
        });
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, String s, String s1, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, SCAN), context -> {
            aerospikeClient.scanAll(eventLoop, recordSequenceListener, scanPolicy, s, s1, strings);
            return null;
        });
    }

    @Override
    public void scanNode(ScanPolicy scanPolicy, String s, String s1, String s2, ScanCallback scanCallback, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, SCAN_NODE), context -> {
            aerospikeClient.scanNode(scanPolicy, s, s1, s2, scanCallback, strings);
            return null;
        });
    }

    @Override
    public void scanNode(ScanPolicy scanPolicy, Node node, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, SCAN_NODE), context -> {
            aerospikeClient.scanNode(scanPolicy, node, s, s1, scanCallback, strings);
            return null;
        });
    }

    @Override
    public void scanPartitions(ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, SCAN_PARTITIONS), context -> {
            aerospikeClient.scanPartitions(scanPolicy, partitionFilter, s, s1, scanCallback, strings);
            return null;
        });
    }

    @Override
    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, SCAN_PARTITIONS), context -> {
            aerospikeClient.scanPartitions(eventLoop, recordSequenceListener, scanPolicy, partitionFilter, s, s1, strings);
            return null;
        });
    }

    @Override
    public RegisterTask register(Policy policy, String s, String s1, Language language) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(REGISTER),
                context ->  aerospikeClient.register(policy, s, s1, language));
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader classLoader, String s, String s1, Language language) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(REGISTER),
                context ->  aerospikeClient.register(policy, classLoader, s, s1, language));
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String s, String s1, Language language) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(REGISTER_UDF_STRING),
                context ->  aerospikeClient.registerUdfString(policy, s, s1, language));
    }

    @Override
    public void removeUdf(InfoPolicy infoPolicy, String s) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(REMOVE_UDF), context -> {
            aerospikeClient.removeUdf(infoPolicy, s);
            return null;
        });
    }

    @Override
    public Object execute(WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(key, EXECUTE),
                context ->  aerospikeClient.execute(writePolicy, key, s, s1, values));
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener executeListener, WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(key, EXECUTE), context -> {
            aerospikeClient.execute(eventLoop, executeListener, writePolicy, key, s, s1, values);
            return null;
        });
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXECUTE),
                context ->  aerospikeClient.execute(batchPolicy, batchUDFPolicy, keys, s, s1, values));
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXECUTE), context -> {
            aerospikeClient.execute(eventLoop, batchRecordArrayListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
            return null;
        });
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(keys, BATCH_EXECUTE), context -> {
            aerospikeClient.execute(eventLoop, batchRecordSequenceListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
            return null;
        });
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, String s, String s1, Value... values) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, EXECUTE),
                context ->  aerospikeClient.execute(writePolicy, statement, s, s1, values));
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, EXECUTE),
                context ->  aerospikeClient.execute(writePolicy, statement, operations));
    }

    @Override
    public RecordSet query(QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY),
                context ->  aerospikeClient.query(queryPolicy, statement));
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(statement, QUERY), context -> {
            aerospikeClient.query(eventLoop, recordSequenceListener, queryPolicy, statement);
            return null;
        });
    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, QueryListener queryListener) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(statement, QUERY), context -> {
            aerospikeClient.query(queryPolicy, statement, queryListener);
            return null;
        });
    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter, QueryListener queryListener) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(statement, QUERY), context -> {
            aerospikeClient.query(queryPolicy, statement, partitionFilter, queryListener);
            return null;
        });
    }

    @Override
    public RecordSet queryNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY_NODE),
                context ->  aerospikeClient.queryNode(queryPolicy, statement, node));
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY_PARTITIONS),
                context ->  aerospikeClient.queryPartitions(queryPolicy, statement, partitionFilter));
    }

    @Override
    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(statement, QUERY_PARTITIONS), context -> {
            aerospikeClient.queryPartitions(eventLoop, recordSequenceListener, queryPolicy, statement, partitionFilter);
            return null;
        });
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy queryPolicy, Statement statement, String s, String s1, Value... values) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY_AGGREGATE),
                context ->  aerospikeClient.queryAggregate(queryPolicy, statement, s, s1, values));
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY_AGGREGATE),
                context ->  aerospikeClient.queryAggregate(queryPolicy, statement));
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(statement, QUERY_AGGREGATE_NODE),
                context ->  aerospikeClient.queryAggregateNode(queryPolicy, statement, node));
    }

    @Override
    public IndexTask createIndex(Policy policy, String s, String s1, String s2, String s3, IndexType indexType) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(s, s1, CREATE_INDEX),
                context ->  aerospikeClient.createIndex(policy, s, s1, s2, s3, indexType));
    }

    @Override
    public IndexTask createIndex(Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(s, s1, CREATE_INDEX),
                context ->  aerospikeClient.createIndex(policy, s, s1, s2, s3, indexType, indexCollectionType, ctxes));
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, CREATE_INDEX), context -> {
            aerospikeClient.createIndex(eventLoop, indexListener, policy, s, s1, s2, s3, indexType, indexCollectionType, ctxes);
            return null;
        });
    }

    @Override
    public IndexTask dropIndex(Policy policy, String s, String s1, String s2) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(s, s1, DROP_INDEX), context ->  aerospikeClient.dropIndex(policy, s, s1, s2));
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s, s1, DROP_INDEX), context -> {
            aerospikeClient.dropIndex(eventLoop, indexListener, policy, s, s1, s2);
            return null;
        });
    }

    @Override
    public void info(EventLoop eventLoop, InfoListener infoListener, InfoPolicy infoPolicy, Node node, String... strings) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(INFO), context -> {
            aerospikeClient.info(eventLoop, infoListener, infoPolicy, node, strings);
            return null;
        });
    }

    @Override
    public void setXDRFilter(InfoPolicy infoPolicy, String s, String s1, Expression expression) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(s1, null, SET_XDR_FILTER), context -> {
            aerospikeClient.setXDRFilter(infoPolicy, s, s1, expression);
            return null;
        });
    }

    @Override
    public void createUser(AdminPolicy adminPolicy, String s, String s1, List<String> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(CREATE_USER), context -> {
            aerospikeClient.createUser(adminPolicy, s, s1, list);
            return null;
        });
    }

    @Override
    public void dropUser(AdminPolicy adminPolicy, String s) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(DROP_USER), context -> {
            aerospikeClient.dropUser(adminPolicy, s);
            return null;
        });
    }

    @Override
    public void changePassword(AdminPolicy adminPolicy, String s, String s1) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(CHANGE_PASSWORD), context -> {
            aerospikeClient.changePassword(adminPolicy, s, s1);
            return null;
        });
    }

    @Override
    public void grantRoles(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(GRANT_ROLES), context -> {
            aerospikeClient.grantRoles(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void revokeRoles(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(REVOKE_ROLES), context -> {
            aerospikeClient.revokeRoles(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(CREATE_ROLE), context -> {
            aerospikeClient.createRole(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(CREATE_ROLE), context -> {
            aerospikeClient.createRole(adminPolicy, s, list, list1);
            return null;
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1, int i, int i1) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(CREATE_ROLE), context -> {
            aerospikeClient.createRole(adminPolicy, s, list, list1, i, i1);
            return null;
        });
    }

    @Override
    public void dropRole(AdminPolicy adminPolicy, String s) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(DROP_ROLE), context -> {
            aerospikeClient.dropRole(adminPolicy, s);
            return null;
        });
    }

    @Override
    public void grantPrivileges(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(GRANT_PRIVILEGES), context -> {
            aerospikeClient.grantPrivileges(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void revokePrivileges(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(REVOKE_PRIVILEGES), context -> {
            aerospikeClient.revokePrivileges(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void setWhitelist(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(SET_WHITELIST), context -> {
            aerospikeClient.setWhitelist(adminPolicy, s, list);
            return null;
        });
    }

    @Override
    public void setQuotas(AdminPolicy adminPolicy, String s, int i, int i1) throws AerospikeException {
        aerospikeInterceptor.execute(interceptorContext(SET_QUOTAS), context -> {
            aerospikeClient.setQuotas(adminPolicy, s, i, i1);
            return null;
        });
    }

    @Override
    public User queryUser(AdminPolicy adminPolicy, String s) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(QUERY_USERS),
                context ->  aerospikeClient.queryUser(adminPolicy, s));
    }

    @Override
    public List<User> queryUsers(AdminPolicy adminPolicy) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(QUERY_USERS),
                context ->  aerospikeClient.queryUsers(adminPolicy));
    }

    @Override
    public Role queryRole(AdminPolicy adminPolicy, String s) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(QUERY_ROLE),
                context ->  aerospikeClient.queryRole(adminPolicy, s));
    }

    @Override
    public List<Role> queryRoles(AdminPolicy adminPolicy) throws AerospikeException {
        return aerospikeInterceptor.execute(interceptorContext(QUERY_ROLE),
                context ->  aerospikeClient.queryRoles(adminPolicy));
    }


    private AerospikeInterceptorContext interceptorContext(final AerospikeOperation operation) {
        return getInterceptorContext(operation);
    }


    private AerospikeInterceptorContext interceptorContext(final Key key,
                                                           final AerospikeOperation operation) {
        if (isInterceptorContextKeyPopulationEnabled) {
            return getInterceptorContext(key, operation);
        }
        return getInterceptorContext(key.namespace, key.setName, operation);
    }


    private AerospikeInterceptorContext interceptorContext(final Key key,
                                                           final AerospikeOperation operation,
                                                           final Bin... bins) {
        if (isInterceptorContextKeyPopulationEnabled) {
            return getInterceptorContext(key, operation, bins);
        }
        return getInterceptorContext(key.namespace, key.setName, operation);
    }


    private AerospikeInterceptorContext interceptorContext(final Statement statement,
                                                           final AerospikeOperation operation) {
        return getInterceptorContext(statement, operation);
    }


    private AerospikeInterceptorContext interceptorContext(final Key[] keys,
                                                           final AerospikeOperation operation) {
        if (isInterceptorContextKeyPopulationEnabled) {
            return getInterceptorContext(keys, operation);
        }

        if (keys.length > 0) {
            return getInterceptorContext(keys[0].namespace, keys[0].setName, operation);
        }
        return getInterceptorContext(operation);
    }


    private AerospikeInterceptorContext interceptorContext(final String namespace,
                                                           final String setName,
                                                           final AerospikeOperation operation) {
        return getInterceptorContext(namespace, setName, operation);
    }

    private void getAndMarkRecordSize(final Record record,
                              final String namespace,
                              final String setName,
                              final AerospikeOperation operation) {
        if (this.sizeMetricsEnabled) {
            try {
                Map<String, Object> bins = record.bins;
                long estimatedSizeBytes = bins.keySet().stream()
                        .mapToLong(k -> {
                            if (bins.get(k) instanceof String bin) {
                                return 2L * bin.length();
                            } else if (bins.get(k) instanceof byte[] b) {
                                return b.length;
                            } else {
                                return 0L;
                            }
                        })
                        .sum();
                String metricName = MetricRegistry.name("aerospike", namespace, setName, operation.name(), "approxSizeBytes");
                metricRegistry.gauge(metricName, () -> () -> estimatedSizeBytes);
            } catch (Exception ignored) {

            }
        }
    }

    private void getAndMarkBinSize(final String namespace,
                                   final String setName,
                                   final AerospikeOperation operation,
                                   final Bin... bins) {
        if (!this.sizeMetricsEnabled) {
            return;
        }
        try {
            long estimatedSizeBytes = Arrays.stream(bins).mapToLong(bin ->
                    switch (bin.value.getType()) {
                        case ParticleType.BLOB, ParticleType.STRING -> bin.value.estimateSize();
                        default -> 0L;  //Ignore
                    }).sum();
            String metricName = MetricRegistry.name("aerospike", namespace, setName, operation.getName(), "approxSizeBytes");
            metricRegistry.gauge(metricName, () -> () -> estimatedSizeBytes);
        } catch (Exception ignored) {
            // Exception ignored intentionally as size metrics are optional
        }
    }

}
