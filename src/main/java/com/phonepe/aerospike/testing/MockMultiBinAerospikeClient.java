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

package com.phonepe.aerospike.testing;

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
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
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
import com.google.common.collect.Lists;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.internal.TerminalOperationInterceptor;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.phonepe.aerospike.interceptors.AerospikeOperation.ADD;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.APPEND;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.BATCH_GET;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.DELETE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.EXISTS;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.GET_HEADER;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.OPERATE;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.PREPEND;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.READ;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.SCAN;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.TOUCH;
import static com.phonepe.aerospike.interceptors.AerospikeOperation.WRITE;
import static com.phonepe.aerospike.interceptors.AerospikeInterceptorContext.getInterceptorContext;

public class MockMultiBinAerospikeClient implements IAerospikeClient {

    private final Map<Key, Record> data;

    private final AerospikeInterceptor aerospikeInterceptor;

    private final boolean checkGenerations;

    public MockMultiBinAerospikeClient(boolean checkGenerations) {
        this(checkGenerations, new ArrayList<>());
    }

    public MockMultiBinAerospikeClient() {
        this(new ArrayList<>());
    }

    public MockMultiBinAerospikeClient(List<AerospikeInterceptor> interceptors) {
        this(true, interceptors);
    }

    public MockMultiBinAerospikeClient(boolean checkGenerations, List<AerospikeInterceptor> interceptors) {
        this.data = new ConcurrentHashMap<>();
        AerospikeInterceptor rootInterceptor = new TerminalOperationInterceptor();
        for (AerospikeInterceptor interceptor : interceptors) {
            rootInterceptor = interceptor.setNext(rootInterceptor);
        }
        this.checkGenerations = checkGenerations;
        this.aerospikeInterceptor = rootInterceptor;
    }

    public void close() {
        this.data.clear();
    }

    public boolean isConnected() {
        return true;
    }

    public Node[] getNodes() {
        return new Node[0];
    }

    public List<String> getNodeNames() {
        return Lists.newLinkedList();
    }

    public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
        throw new AerospikeException.InvalidNode("");
    }

    public ClusterStats getClusterStats() {
        return null;
    }

    @Override
    public Cluster getCluster() {
        return null;
    }

    private void putData(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (this.checkGenerations && this.existsInternal(key) && policy.generationPolicy == GenerationPolicy.EXPECT_GEN_EQUAL && policy.generation == 0) {
            throw new AerospikeException(3, "Error in saving");
        } else {
            this.data.put(key, new Record(this.convertToMap(bins), 0, 0));
        }
    }

    public void put(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
    }

    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(key, APPEND), context -> {
            if (!this.data.containsKey(key)) {
                this.put(policy, key, bins);
            } else {
                Map<String, Object> recordBins = this.data.get(key).bins;
                for (Bin bin : bins) {
                    recordBins.computeIfPresent(bin.name, (s, o) -> {
                        if (o instanceof String) {
                            return o + bin.value.toString();
                        } else {
                            throw new AerospikeException("Error Code 12: Bin type");
                        }
                    });
                }
            }
            return null;
        });
    }

    public void append(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
    }

    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(key, PREPEND), context -> {
            if (!this.data.containsKey(key)) {
                this.put(policy, key, bins);
            } else {
                Map<String, Object> recordBins = this.data.get(key).bins;
                for (Bin bin : bins) {
                    recordBins.computeIfPresent(bin.name, (s, o) -> {
                        if (o instanceof String) {
                            return bin.value.toString() + o;
                        } else {
                            throw new AerospikeException("Error Code 12: Bin type");
                        }
                    });
                }
            }
            return null;
        });
    }

    public void prepend(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
    }

    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(key, ADD), context -> {
            if (!this.data.containsKey(key)) {
                this.put(policy, key, bins);
            } else {
                Map<String, Object> recordBins = this.data.get(key).bins;
                for (Bin bin : bins) {
                    recordBins.computeIfPresent(bin.name, (s, o) -> {
                        if (o instanceof Integer) {
                            return bin.value.toInteger() + (Integer) o;
                        } else {
                            throw new AerospikeException("Error Code 12: Bin type");
                        }
                    });
                }
            }
            return null;
        });
    }

    public void add(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
    }

    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, DELETE), context -> {
            if (this.data.containsKey(key)) {
                this.data.remove(key);
                return true;
            } else {
                return false;
            }
        });
    }

    public void delete(EventLoop eventLoop, DeleteListener deleteListener, WritePolicy writePolicy, Key key) throws AerospikeException {
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        return null;
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {

    }

    public void truncate(InfoPolicy infoPolicy, String s, String s1, Calendar calendar) throws AerospikeException {
    }

    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(key, TOUCH), context -> {
            if (this.data.containsKey(key)) {
                Record record = this.data.remove(key);
                this.data.put(key, new Record(record.bins, record.generation, policy.expiration));
            } else {
                throw new AerospikeException("Error Code 2: Key not found");
            }
            return null;
        });
    }

    public void touch(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key) throws AerospikeException {
    }

    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, EXISTS), context -> this.existsInternal(key));
    }

    private boolean existsInternal(Key key) throws AerospikeException {
        return this.data.containsKey(key);
    }

    public void exists(EventLoop eventLoop, ExistsListener existsListener, Policy policy, Key key) throws AerospikeException {
    }

    /**
     * @deprecated
     */
    @Deprecated
    public boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(keys, EXISTS), context -> {
            boolean[] result = new boolean[keys.length];
            for (int idx = 0; idx < keys.length; ++idx) {
                result[idx] = this.data.containsKey(keys[idx]);
            }

            return result;
        });
    }

    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return this.exists((Policy) policy, keys);
    }

    public void exists(EventLoop eventLoop, ExistsArrayListener existsArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    public void exists(EventLoop eventLoop, ExistsSequenceListener existsSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    public Record get(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, READ), context -> this.data.get(key));
    }

    private Record getInternal(Key key) throws AerospikeException {
        return this.data.get(key);
    }

    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
    }

    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, READ), context -> {
            Record record = this.data.get(key);
            if (record == null) {
                return null;
            } else {
                Map<String, Object> filteredBins = new HashMap<>();
                for (String bin : binNames) {
                    filteredBins.put(bin, record.bins.get(bin));
                }
                return new Record(filteredBins, record.generation, record.expiration);
            }
        });
    }

    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key, String... strings) throws AerospikeException {
    }

    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, GET_HEADER), context -> {
            Record record = this.data.get(key);
            return record == null ? null : new Record(null, record.generation, record.expiration);
        });
    }

    public void getHeader(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Record[] get(Policy policy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(BATCH_GET), context -> {
            Record[] records = new Record[keys.length];
            for (int idx = 0; idx < records.length; ++idx) {
                records[idx] = this.getInternal(keys[idx]);
            }
            return records;
        });
    }

    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return this.get((Policy) policy, keys);
    }

    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Record[] get(Policy policy, Key[] keys, String... binNames) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(BATCH_GET), context -> {
            Record[] records = new Record[keys.length];
            for (int idx = 0; idx < records.length; ++idx) {
                records[idx] = this.get(policy, keys[idx], binNames);
            }

            return records;
        });
    }

    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        return this.get((Policy) policy, keys, binNames);
    }

    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
    }

    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(keys, BATCH_GET), context -> new Record[0]);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {

    }

    /**
     * @deprecated
     */
    @Deprecated
    public Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
        return aerospikeInterceptor.execute(
                getInterceptorContext(keys, GET_HEADER),
                context -> {
                    Record[] records = new Record[keys.length];

                    for (int idx = 0; idx < records.length; ++idx) {
                        records[idx] = this.getHeader(policy, keys[idx]);
                    }

                    return records;
                });
    }

    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return this.getHeader((Policy) policy, keys);
    }

    public void getHeader(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    public void getHeader(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
    }

    public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        return aerospikeInterceptor.execute(getInterceptorContext(key, OPERATE), context -> {
            Map<String, Object> bins = new HashMap<>();
            int len = operations.length;
            int i = 0;

            while (i < len) {
                Operation operation = operations[i];
                switch (operation.type) {
                    case ADD:
                    case APPEND:
                    case WRITE:
                    case PREPEND:
                        bins.put(operation.binName, operation.value.getObject());
                    default:
                        ++i;
                }
            }

            if (this.data.containsKey(key)) {
                this.data.get(key).bins.putAll(bins);
            } else {
                this.data.put(key, new Record(bins, 0, 0));
            }

            return this.data.get(key);
        });
    }

    public void operate(EventLoop eventLoop, RecordListener recordListener, WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {
    }

    @Override
    public boolean operate(BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        return false;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener batchOperateListListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {

    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {

    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        return null;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {

    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {

    }

    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(namespace, setName, SCAN), context -> {
            this.data.entrySet().stream().filter((keyRecordEntry) ->
                    keyRecordEntry.getKey().namespace.equals(namespace)).filter((keyRecordEntry) ->
                    keyRecordEntry.getKey().setName.equals(setName)).forEach((keyRecordEntry) ->
                    callback.scanCallback(keyRecordEntry.getKey(), keyRecordEntry.getValue()));
            return null;
        });
    }

    public void scanAll(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, String s, String s1, String... strings) throws AerospikeException {
    }

    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");
    }

    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");
    }

    @Override
    public void scanPartitions(ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {

    }

    @Override
    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, String... strings) throws AerospikeException {

    }

    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
        throw new UnsupportedOperationException("register is not supported in MockAerospike");
    }

    public Object execute(Policy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    public void execute(EventLoop eventLoop, ExecuteListener executeListener, WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        return null;
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {

    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {

    }

    public ExecuteTask execute(Policy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, Operation... operations) throws AerospikeException {
        return null;
    }

    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        throw new UnsupportedOperationException("query is not supported in MockAerospike");
    }

    public void query(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, QueryListener queryListener) throws AerospikeException {

    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter, QueryListener queryListener) throws AerospikeException {

    }

    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        throw new UnsupportedOperationException("queryNode is not supported in MockAerospike");
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        return null;
    }

    @Override
    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {

    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        throw new UnsupportedOperationException("queryAggregate is not supported in MockAerospike");
    }

    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException {
        throw new UnsupportedOperationException("createIndex is not supported in MockAerospike");
    }

    @Override
    public IndexTask createIndex(Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {
        return null;
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {

    }

    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {
        throw new UnsupportedOperationException("dropIndex is not supported in MockAerospike");
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2) throws AerospikeException {

    }

    @Override
    public void info(EventLoop eventLoop, InfoListener infoListener, InfoPolicy infoPolicy, Node node, String... strings) throws AerospikeException {

    }

    @Override
    public void setXDRFilter(InfoPolicy infoPolicy, String s, String s1, Expression expression) throws AerospikeException {

    }

    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("createUser is not supported in MockAerospike");
    }

    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        throw new UnsupportedOperationException("dropUser is not supported in MockAerospike");
    }

    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        throw new UnsupportedOperationException("changePassword is not supported in MockAerospike");
    }

    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("grantRoles is not supported in MockAerospike");
    }

    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("revokeRoles is not supported in MockAerospike");
    }

    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("createRole is not supported in MockAerospike");
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1) throws AerospikeException {

    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1, int i, int i1) throws AerospikeException {

    }

    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        throw new UnsupportedOperationException("dropRole is not supported in MockAerospike");
    }

    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("grantPrivileges is not supported in MockAerospike");
    }

    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("revokePrivileges is not supported in MockAerospike");
    }

    @Override
    public void setWhitelist(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {

    }

    @Override
    public void setQuotas(AdminPolicy adminPolicy, String s, int i, int i1) throws AerospikeException {

    }

    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        throw new UnsupportedOperationException("queryUser is not supported in MockAerospike");
    }

    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException("queryUsers is not supported in MockAerospike");
    }

    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        throw new UnsupportedOperationException("queryRole is not supported in MockAerospike");
    }

    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException("queryRoles is not supported in MockAerospike");
    }

    private Map<String, Object> convertToMap(Bin[] bins) {

        Map<String, Object> binMap = new HashMap<>(bins.length);
        for (Bin bin : bins) {
            if (bin.value instanceof Value.BooleanValue || bin.value instanceof Value.IntegerValue) {
                binMap.put(bin.name, bin.value.toLong());
            } else {
                binMap.put(bin.name, bin.value.getObject());
            }
        }

        return binMap;
    }

    public Policy getReadPolicyDefault() {
        return new Policy();
    }

    public WritePolicy getWritePolicyDefault() {
        return new WritePolicy();
    }

    public ScanPolicy getScanPolicyDefault() {
        return new ScanPolicy(getReadPolicyDefault());
    }

    public QueryPolicy getQueryPolicyDefault() {
        return null;
    }

    public BatchPolicy getBatchPolicyDefault() {
        return null;
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return null;
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return null;
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return null;
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return null;
    }

    public InfoPolicy getInfoPolicyDefault() {
        return null;
    }

    public boolean get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        return false;
    }

    public void get(EventLoop eventLoop, BatchListListener batchListListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
    }

    public void get(EventLoop eventLoop, BatchSequenceListener batchSequenceListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
    }

    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) throws AerospikeException {
        return null;
    }

    public RegisterTask registerUdfString(Policy policy, String s, String s1, Language language) throws AerospikeException {
        return null;
    }

    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        return null;
    }

    public ResultSet queryAggregateNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return null;
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        aerospikeInterceptor.execute(getInterceptorContext(key, WRITE), context -> {
            boolean exists = this.existsInternal(key);
            if (invalidCreateEntry(exists, policy) || invalidUpdateEntry(exists, policy)) {
                throw new AerospikeException(3, "Error in saving");
            }
            Record record = getInternal(key);
            if (record == null) {  //Do normal write
                putData(policy, key, bins);
                return null;
            }

            Map<String, Object> currentDataMap = record.bins;
            Arrays.stream(bins).forEach(k -> currentDataMap.put(k.name, k.value));

            Bin[] updatedBins = new Bin[currentDataMap.size()];

            int i = 0;
            for (val entry : currentDataMap.entrySet()) {
                updatedBins[i++] = new Bin(entry.getKey(), entry.getValue());
            }
            putData(policy, key, updatedBins);
            return null;
        });
    }

    private boolean invalidUpdateEntry(final boolean dataExists, final WritePolicy policy) {
        return !dataExists && policy.recordExistsAction == RecordExistsAction.UPDATE_ONLY;
    }

    private boolean invalidCreateEntry(final boolean dataExists, final WritePolicy policy) {
        return dataExists && policy.recordExistsAction == RecordExistsAction.CREATE_ONLY;
    }
}
