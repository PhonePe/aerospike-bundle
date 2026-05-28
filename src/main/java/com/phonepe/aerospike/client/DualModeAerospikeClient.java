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
import com.phonepe.aerospike.client.listeners.batch.PrimaryBatchOperateListListener;
import com.phonepe.aerospike.client.listeners.batch.PrimaryBatchRecordArrayListener;
import com.phonepe.aerospike.client.listeners.batch.PrimaryBatchRecordSequenceListener;
import com.phonepe.aerospike.client.listeners.batch.SecondaryBatchOperateListListener;
import com.phonepe.aerospike.client.listeners.batch.SecondaryBatchRecordArrayListener;
import com.phonepe.aerospike.client.listeners.batch.SecondaryBatchRecordSequenceListener;
import com.phonepe.aerospike.client.listeners.delete.PrimaryDeleteListener;
import com.phonepe.aerospike.client.listeners.delete.SecondaryDeleteListener;
import com.phonepe.aerospike.client.listeners.execute.PrimaryExecuteListener;
import com.phonepe.aerospike.client.listeners.execute.SecondaryExecuteListener;
import com.phonepe.aerospike.client.listeners.write.PrimaryRecordListener;
import com.phonepe.aerospike.client.listeners.write.PrimaryWriteListener;
import com.phonepe.aerospike.client.listeners.write.SecondaryRecordListener;
import com.phonepe.aerospike.client.listeners.write.SecondaryWriteListener;
import com.phonepe.aerospike.config.write.DualWriteMode;
import com.phonepe.aerospike.config.write.SingleSourceWriteMode;
import com.phonepe.aerospike.config.write.WriteModeConfigVisitor;
import com.phonepe.aerospike.config.write.WriteModeVisitorAdaptor;
import com.phonepe.aerospike.exception.AerospikeBundleException;
import com.phonepe.aerospike.exception.ResponseCode;
import com.phonepe.aerospike.managed.DualModeASClientResolver;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Calendar;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
@Data
@Builder
public class DualModeAerospikeClient implements IAerospikeClient {

    private DualModeASClientResolver asClientResolver;

    public DualModeAerospikeClient(final DualModeASClientResolver asClientResolver) {
        this.asClientResolver = asClientResolver;
    }

    @Override
    public Policy getReadPolicyDefault() {
        return asClientResolver.primaryASClient().getReadPolicyDefault();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return asClientResolver.primaryWriteASClient().getWritePolicyDefault();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return asClientResolver.primaryASClient().getScanPolicyDefault();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return asClientResolver.primaryASClient().getQueryPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return asClientResolver.primaryASClient().getBatchPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return asClientResolver.primaryWriteASClient().getBatchPolicyDefault();
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return asClientResolver.primaryWriteASClient().getBatchWritePolicyDefault();
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return asClientResolver.primaryWriteASClient().getBatchDeletePolicyDefault();
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return asClientResolver.primaryASClient().getBatchUDFPolicyDefault();
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return asClientResolver.primaryASClient().getInfoPolicyDefault();
    }

    @Override
    public void close() {
        asClientResolver.getAllAerospikeClient().forEach(IAerospikeClient::close);
    }

    @Override
    public boolean isConnected() {
        return asClientResolver.primaryASClient().isConnected();
    }

    @Override
    public Node[] getNodes() {
        return asClientResolver.primaryASClient().getNodes();
    }

    @Override
    public List<String> getNodeNames() {
        return asClientResolver.primaryASClient().getNodeNames();
    }

    @Override
    public Node getNode(String s) throws AerospikeException.InvalidNode {
        return asClientResolver.primaryASClient().getNode(s);
    }

    @Override
    public ClusterStats getClusterStats() {
        return asClientResolver.primaryASClient().getClusterStats();
    }

    @Override
    public Cluster getCluster() {
        return asClientResolver.primaryASClient().getCluster();
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        asClientResolver.primaryWriteASClient().put(writePolicy, key, bins);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).put(writePolicy, key, bins);
                }
                catch (Exception e) {
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_PUT_ATTEMPT_FAILED, "Failed to store Key : " + key
                                + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().put(eventLoop, writeListener, writePolicy, key, bins);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryWriteListener = new SecondaryWriteListener<>(writeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).put(eventLoop, secondaryWriteListener, writePolicy, key, bins);
                    return null;
                };
                val primaryWriteListener = new PrimaryWriteListener<>(writeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().put(eventLoop, primaryWriteListener, writePolicy, key, bins);
                return null;
            }
        });
    }

    @Override
    public void append(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        asClientResolver.primaryWriteASClient().append(writePolicy, key, bins);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).append(writePolicy, key, bins);
                }
                catch (Exception e) {
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_PUT_ATTEMPT_FAILED, "Failed to " +
                                "append data for Key : " + key + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().append(eventLoop, writeListener, writePolicy, key, bins);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryWriteListener = new SecondaryWriteListener<>(writeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).append(eventLoop, secondaryWriteListener, writePolicy, key, bins);
                    return null;
                };
                val primaryWriteListener = new PrimaryWriteListener<>(writeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().append(eventLoop, primaryWriteListener, writePolicy, key, bins);
                return null;
            }
        });
    }

    @Override
    public void prepend(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        asClientResolver.primaryWriteASClient().prepend(writePolicy, key, bins);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).prepend(writePolicy, key, bins);
                }
                catch (Exception e) {
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_PUT_ATTEMPT_FAILED, "Failed to " +
                                "prepend data for Key : " + key + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {

        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().prepend(eventLoop, writeListener, writePolicy, key, bins);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryWriteListener = new SecondaryWriteListener<>(writeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).prepend(eventLoop, secondaryWriteListener, writePolicy, key, bins);
                    return null;
                };
                val primaryWriteListener = new PrimaryWriteListener<>(writeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().prepend(eventLoop, primaryWriteListener, writePolicy, key, bins);
                return null;
            }
        });
    }

    @Override
    public void add(WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        asClientResolver.primaryWriteASClient().add(writePolicy, key, bins);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).add(writePolicy, key, bins);
                }
                catch (Exception e) {
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_PUT_ATTEMPT_FAILED, "Failed to " +
                                "add data for Key : " + key + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().add(eventLoop, writeListener, writePolicy, key, bins);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryWriteListener = new SecondaryWriteListener<>(writeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).add(eventLoop, secondaryWriteListener, writePolicy, key, bins);
                    return null;
                };
                val primaryWriteListener = new PrimaryWriteListener<>(writeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().add(eventLoop, primaryWriteListener, writePolicy, key, bins);
                return null;
            }
        });
    }

    @Override
    public boolean delete(WritePolicy writePolicy, Key key) throws AerospikeException {
        val deleteOp = asClientResolver.primaryWriteASClient().delete(writePolicy, key);
        if (deleteOp) {
            val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
            dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
                @Override
                public Void visit(final DualWriteMode dualWriteMode) {
                    try {
                        asClientResolver.secondaryWriteASClient(dualWriteMode).delete(writePolicy, key);
                    }
                    catch (Exception e){
                        if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                            throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_DELETE_ATTEMPT_FAILED, "Failed to " +
                                    "delete data for Key : " + key + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                        }
                    }
                    return null;
                }
            });
        }
        return deleteOp;
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener deleteListener, WritePolicy writePolicy, Key key) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().delete(eventLoop, deleteListener, writePolicy, key);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryDeleteListener = new SecondaryDeleteListener<>(deleteListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).delete(eventLoop, secondaryDeleteListener, writePolicy, key);
                    return null;
                };
                val primaryDeleteListener = new PrimaryDeleteListener<>(deleteListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().delete(eventLoop, primaryDeleteListener, writePolicy, key);
                return null;
            }
        });
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        val batchResults = asClientResolver.primaryWriteASClient().delete(batchPolicy, batchDeletePolicy, keys);
        if (batchResults.status) {
            val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
            dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
                @Override
                public Void visit(final DualWriteMode dualWriteMode) {
                    try {
                        asClientResolver.secondaryWriteASClient(dualWriteMode).delete(batchPolicy, batchDeletePolicy, keys);
                    }
                    catch (Exception e){
                        if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                            throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to do " +
                                    "delete operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                        }
                    }
                    return null;
                }
            });
        }
        return batchResults;
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().delete(eventLoop, batchRecordArrayListener, batchPolicy, batchDeletePolicy, keys);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordArrayListener = new SecondaryBatchRecordArrayListener<>(batchRecordArrayListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).delete(eventLoop, secondaryBatchRecordArrayListener, batchPolicy, batchDeletePolicy, keys);
                    return null;
                };
                val primaryBatchRecordArrayListener = new PrimaryBatchRecordArrayListener<>(batchRecordArrayListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().delete(eventLoop, primaryBatchRecordArrayListener, batchPolicy, batchDeletePolicy, keys);
                return null;
            }
        });
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchDeletePolicy batchDeletePolicy, Key[] keys) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().delete(eventLoop, batchRecordSequenceListener, batchPolicy, batchDeletePolicy, keys);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordSequenceListener = new SecondaryBatchRecordSequenceListener<>(batchRecordSequenceListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).delete(eventLoop, secondaryBatchRecordSequenceListener, batchPolicy, batchDeletePolicy, keys);
                    return null;
                };
                val primaryBatchRecordSequenceListener = new PrimaryBatchRecordSequenceListener<>(batchRecordSequenceListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().delete(eventLoop, primaryBatchRecordSequenceListener, batchPolicy, batchDeletePolicy, keys);
                return null;
            }
        });
    }

    @Override
    public void truncate(InfoPolicy infoPolicy, String s, String s1, Calendar calendar) throws AerospikeException {
        asClientResolver.primaryWriteASClient().truncate(infoPolicy, s, s1, calendar);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).truncate(infoPolicy, s, s1, calendar);
                }
                catch (Exception e){
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_DELETE_ATTEMPT_FAILED, "Failed to " +
                                "truncate data in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void touch(WritePolicy writePolicy, Key key) throws AerospikeException {
        asClientResolver.primaryWriteASClient().touch(writePolicy, key);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).touch(writePolicy, key);
                }
                catch (Exception e){
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_DELETE_ATTEMPT_FAILED, "Failed to " +
                                "do touch op for Key : " + key + " in aerospike cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().touch(eventLoop, writeListener, writePolicy, key);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryWriteListener = new SecondaryWriteListener<>(writeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).touch(eventLoop, secondaryWriteListener, writePolicy, key);
                    return null;
                };
                val primaryWriteListener = new PrimaryWriteListener<>(writeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().touch(eventLoop, primaryWriteListener, writePolicy, key);
                return null;
            }
        });
    }

    @Override
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return asClientResolver.primaryASClient().exists(policy, key);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener existsListener, Policy policy, Key key) throws AerospikeException {
        asClientResolver.primaryASClient().exists(eventLoop, existsListener, policy, key);
    }

    @Override
    public boolean[] exists(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return asClientResolver.primaryASClient().exists(batchPolicy, keys);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener existsArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().exists(eventLoop, existsArrayListener, batchPolicy, keys);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener existsSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().exists(eventLoop, existsSequenceListener, batchPolicy, keys);
    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        return asClientResolver.primaryASClient().get(policy, key);
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordListener, policy, key);
    }

    @Override
    public Record get(Policy policy, Key key, String... strings) throws AerospikeException {
        return asClientResolver.primaryASClient().get(policy, key, strings);
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordListener, policy, key, strings);
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        return asClientResolver.primaryASClient().getHeader(policy, key);
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {
        asClientResolver.primaryASClient().getHeader(eventLoop, recordListener, policy, key);
    }

    @Override
    public boolean get(BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        return asClientResolver.primaryASClient().get(batchPolicy, list);
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener batchListListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, batchListListener, batchPolicy, list);
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener batchSequenceListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, batchSequenceListener, batchPolicy, list);
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return asClientResolver.primaryASClient().get(batchPolicy, keys);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordArrayListener, batchPolicy, keys);
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordSequenceListener, batchPolicy, keys);
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        return asClientResolver.primaryASClient().get(batchPolicy, keys, strings);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordArrayListener, batchPolicy, keys, strings);
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordSequenceListener, batchPolicy, keys, strings);
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        return asClientResolver.primaryASClient().get(batchPolicy, keys, operations);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordArrayListener, batchPolicy, keys, operations);
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, Operation... operations) throws AerospikeException {
        asClientResolver.primaryASClient().get(eventLoop, recordSequenceListener, batchPolicy, keys, operations);
    }

    @Override
    public Record[] getHeader(BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        return asClientResolver.primaryASClient().getHeader(batchPolicy, keys);
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().getHeader(eventLoop, recordArrayListener, batchPolicy, keys);
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {
        asClientResolver.primaryASClient().getHeader(eventLoop, recordSequenceListener, batchPolicy, keys);
    }

    @Override
    public Record operate(WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {
        val record = asClientResolver.primaryWriteASClient().operate(writePolicy, key, operations);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(writePolicy, key, operations);
                }
                catch (Exception e){
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to do " +
                                "operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
        return record;
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener recordListener, WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().operate(eventLoop, recordListener, writePolicy, key, operations);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryRecordListener = new SecondaryRecordListener<>(recordListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(eventLoop, secondaryRecordListener, writePolicy, key, operations);
                    return null;
                };
                val primaryRecordListener = new PrimaryRecordListener<>(recordListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().operate(eventLoop, primaryRecordListener, writePolicy, key, operations);
                return null;
            }
        });
    }

    @Override
    public boolean operate(BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        val result = asClientResolver.primaryWriteASClient().operate(batchPolicy, list);
        if (result) {
            val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
            dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
                @Override
                public Void visit(final DualWriteMode dualWriteMode) {
                    try {
                        asClientResolver.secondaryWriteASClient(dualWriteMode).operate(batchPolicy, list);
                    }
                    catch (Exception e){
                        if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                            throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to " +
                                    "operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                        }
                    }
                    return null;
                }
            });
        }
        return result;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener batchOperateListListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().operate(eventLoop, batchOperateListListener, batchPolicy, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordArrayListener = new SecondaryBatchOperateListListener<>(batchOperateListListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(eventLoop, secondaryBatchRecordArrayListener, batchPolicy, list);
                    return null;
                };
                val primaryBatchOperateListListener = new PrimaryBatchOperateListListener<>(batchOperateListListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().operate(eventLoop, primaryBatchOperateListListener, batchPolicy, list);
                return null;
            }
        });
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, List<BatchRecord> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().operate(eventLoop, batchRecordSequenceListener, batchPolicy, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordSequenceListener = new SecondaryBatchRecordSequenceListener<>(batchRecordSequenceListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(eventLoop, secondaryBatchRecordSequenceListener, batchPolicy, list);
                    return null;
                };
                val primaryBatchRecordSequenceListener = new PrimaryBatchRecordSequenceListener<>(batchRecordSequenceListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().operate(eventLoop, primaryBatchRecordSequenceListener, batchPolicy, list);
                return null;
            }
        });
    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        val batchResults = asClientResolver.primaryWriteASClient().operate(batchPolicy, batchWritePolicy, keys, operations);
        if (batchResults.status) {
            val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
            dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
                @Override
                public Void visit(final DualWriteMode dualWriteMode) {
                    try {
                        asClientResolver.secondaryWriteASClient(dualWriteMode).operate(batchPolicy, batchWritePolicy, keys, operations);
                    }
                    catch (Exception e){
                        if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                            throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to do " +
                                    "operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                        }
                    }
                    return null;
                }
            });
        }
        return batchResults;
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().operate(eventLoop, batchRecordArrayListener, batchPolicy, batchWritePolicy, keys, operations);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordArrayListener = new SecondaryBatchRecordArrayListener<>(batchRecordArrayListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(eventLoop, secondaryBatchRecordArrayListener, batchPolicy, batchWritePolicy, keys, operations);
                    return null;
                };
                val primaryBatchRecordArrayListener = new PrimaryBatchRecordArrayListener<>(batchRecordArrayListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().operate(eventLoop, primaryBatchRecordArrayListener, batchPolicy, batchWritePolicy, keys, operations);
                return null;
            }
        });
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchWritePolicy batchWritePolicy, Key[] keys, Operation... operations) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().operate(eventLoop, batchRecordSequenceListener, batchPolicy, batchWritePolicy, keys, operations);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordSequenceListener = new SecondaryBatchRecordSequenceListener<>(batchRecordSequenceListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).operate(eventLoop, secondaryBatchRecordSequenceListener, batchPolicy, batchWritePolicy, keys, operations);
                    return null;
                };
                val primaryBatchRecordSequenceListener = new PrimaryBatchRecordSequenceListener<>(batchRecordSequenceListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().operate(eventLoop, primaryBatchRecordSequenceListener, batchPolicy, batchWritePolicy, keys, operations);
                return null;
            }
        });
    }

    @Override
    public void scanAll(ScanPolicy scanPolicy, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanAll(scanPolicy, s, s1, scanCallback, strings);
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, String s, String s1, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanAll(eventLoop, recordSequenceListener, scanPolicy, s, s1, strings);
    }

    @Override
    public void scanNode(ScanPolicy scanPolicy, String s, String s1, String s2, ScanCallback scanCallback, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanNode(scanPolicy, s, s1, s2, scanCallback, strings);
    }

    @Override
    public void scanNode(ScanPolicy scanPolicy, Node node, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanNode(scanPolicy, node, s, s1, scanCallback, strings);
    }

    @Override
    public void scanPartitions(ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanPartitions(scanPolicy, partitionFilter, s, s1, scanCallback, strings);
    }

    @Override
    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().scanPartitions(eventLoop, recordSequenceListener, scanPolicy, partitionFilter, s, s1, strings);
    }

    @Override
    public RegisterTask register(Policy policy, String s, String s1, Language language) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<RegisterTask>() {
            @Override
            public RegisterTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().register(policy, s, s1, language);
            }

            @Override
            public RegisterTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "register operation is not supported in dual mode");
            }
        });
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader classLoader, String s, String s1, Language language) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<RegisterTask>() {
            @Override
            public RegisterTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().register(policy, classLoader, s, s1, language);
            }

            @Override
            public RegisterTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "register operation is not supported in dual mode");
            }
        });
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String s, String s1, Language language) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<RegisterTask>() {
            @Override
            public RegisterTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().register(policy, s, s1, language);
            }

            @Override
            public RegisterTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "register operation is not supported in dual mode");
            }
        });
    }

    @Override
    public void removeUdf(InfoPolicy infoPolicy, String s) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().removeUdf(infoPolicy, s);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "removeUdf operation is not supported in dual mode");
            }
        });
    }

    @Override
    public Object execute(WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {
        val result = asClientResolver.primaryWriteASClient().execute(writePolicy, key, s, s1, values);
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                try {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).execute(writePolicy, key, s, s1, values);
                }
                catch (Exception e){
                    if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                        throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to do " +
                                "operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                    }
                }
                return null;
            }
        });
        return result;
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener executeListener, WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().execute(eventLoop, executeListener, writePolicy, key, s, s1, values);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryExecuteListener = new SecondaryExecuteListener<>(executeListener, dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).execute(eventLoop, secondaryExecuteListener, writePolicy, key, s, s1, values);
                    return null;
                };
                val primaryExecuteListener = new PrimaryExecuteListener<>(executeListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().execute(eventLoop, primaryExecuteListener, writePolicy, key, s, s1, values);
                return null;
            }
        });
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        val batchResults = asClientResolver.primaryWriteASClient().execute(batchPolicy, batchUDFPolicy, keys, s, s1, values);
        if (batchResults.status) {
            val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
            dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeVisitorAdaptor<Void>() {
                @Override
                public Void visit(final DualWriteMode dualWriteMode) {
                    try {
                        asClientResolver.secondaryWriteASClient(dualWriteMode).execute(batchPolicy, batchUDFPolicy, keys, s, s1, values);
                    }
                    catch (Exception e){
                        if (dualWriteMode.isErrorOnSecondaryWriteFailure()) {
                            throw new AerospikeBundleException(ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "Failed to do " +
                                    "delete operation on secondary cluster with id : " + dualWriteMode.getSecondaryClusterId());
                        }
                    }
                    return null;
                }
            });
        }
        return batchResults;
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener batchRecordArrayListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().execute(eventLoop, batchRecordArrayListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordArrayListener = new SecondaryBatchRecordArrayListener<>(batchRecordArrayListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).execute(eventLoop, secondaryBatchRecordArrayListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                    return null;
                };
                val primaryBatchRecordArrayListener = new PrimaryBatchRecordArrayListener<>(batchRecordArrayListener, supplier,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().execute(eventLoop, primaryBatchRecordArrayListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                return null;
            }
        });
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordSequenceListener batchRecordSequenceListener, BatchPolicy batchPolicy, BatchUDFPolicy batchUDFPolicy, Key[] keys, String s, String s1, Value... values) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().execute(eventLoop, batchRecordSequenceListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                val secondaryBatchRecordSequenceListener = new SecondaryBatchRecordSequenceListener<>(batchRecordSequenceListener,
                        dualWriteMode.isErrorOnSecondaryWriteFailure());
                final Supplier<Void> supplier = () -> {
                    asClientResolver.secondaryWriteASClient(dualWriteMode).execute(eventLoop, secondaryBatchRecordSequenceListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                    return null;
                };
                val primaryBatchRecordSequenceListener = new PrimaryBatchRecordSequenceListener<>(batchRecordSequenceListener, supplier, dualWriteMode.isErrorOnSecondaryWriteFailure());
                asClientResolver.primaryWriteASClient().execute(eventLoop, primaryBatchRecordSequenceListener, batchPolicy, batchUDFPolicy, keys, s, s1, values);
                return null;
            }
        });
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, String s, String s1, Value... values) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<ExecuteTask>() {
            @Override
            public ExecuteTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().execute(writePolicy, statement, s, s1, values);
            }

            @Override
            public ExecuteTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async execute operation is not supported in dual mode");
            }
        });
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, Operation... operations) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<ExecuteTask>() {
            @Override
            public ExecuteTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().execute(writePolicy, statement, operations);
            }

            @Override
            public ExecuteTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async execute operation is not supported in dual mode");
            }
        });
    }

    @Override
    public RecordSet query(QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        return asClientResolver.primaryASClient().query(queryPolicy, statement);
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        asClientResolver.primaryASClient().query(eventLoop, recordSequenceListener, queryPolicy, statement);
    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, QueryListener queryListener) throws AerospikeException {
        asClientResolver.primaryASClient().query(queryPolicy, statement, queryListener);
    }

    @Override
    public void query(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter, QueryListener queryListener) throws AerospikeException {
        asClientResolver.primaryASClient().query(queryPolicy, statement, partitionFilter, queryListener);
    }

    @Override
    public RecordSet queryNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return asClientResolver.primaryASClient().queryNode(queryPolicy, statement, node);
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        return asClientResolver.primaryASClient().queryPartitions(queryPolicy, statement, partitionFilter);
    }

    @Override
    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        asClientResolver.primaryASClient().queryPartitions(eventLoop, recordSequenceListener, queryPolicy, statement, partitionFilter);
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy queryPolicy, Statement statement, String s, String s1, Value... values) throws AerospikeException {
        return asClientResolver.primaryASClient().queryAggregate(queryPolicy, statement, s, s1, values);
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy queryPolicy, Statement statement) throws AerospikeException {
        return asClientResolver.primaryASClient().queryAggregate(queryPolicy, statement);
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return asClientResolver.primaryASClient().queryAggregateNode(queryPolicy, statement, node);
    }

    @Override
    public IndexTask createIndex(Policy policy, String s, String s1, String s2, String s3, IndexType indexType) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<IndexTask>() {
            @Override
            public IndexTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().createIndex(policy, s, s1, s2, s3, indexType);
            }

            @Override
            public IndexTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async createIndex operation is not supported in dual mode");
            }
        });
    }

    @Override
    public IndexTask createIndex(Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<IndexTask>() {
            @Override
            public IndexTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().createIndex(policy, s, s1, s2, s3, indexType, indexCollectionType, ctxes);
            }

            @Override
            public IndexTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async createIndex operation is not supported in dual mode");
            }
        });
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctxes) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().createIndex(eventLoop, indexListener, policy, s, s1, s2, s3, indexType, indexCollectionType, ctxes);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async createIndex operation is not supported in dual mode");
            }
        });
    }

    @Override
    public IndexTask dropIndex(Policy policy, String s, String s1, String s2) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        return dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<IndexTask>() {
            @Override
            public IndexTask visit(SingleSourceWriteMode singleSourceWriteMode) {
                return asClientResolver.primaryWriteASClient().dropIndex(policy, s, s1, s2);
            }

            @Override
            public IndexTask visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async dropIndex operation is not supported in dual mode");
            }
        });
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().dropIndex(eventLoop, indexListener, policy, s, s1, s2);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "async dropIndex operation is not supported in dual mode");
            }
        });
    }

    @Override
    public void info(EventLoop eventLoop, InfoListener infoListener, InfoPolicy infoPolicy, Node node, String... strings) throws AerospikeException {
        asClientResolver.primaryASClient().info(eventLoop, infoListener, infoPolicy, node, strings);
    }

    @Override
    public void setXDRFilter(InfoPolicy infoPolicy, String s, String s1, Expression expression) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().setXDRFilter(infoPolicy, s, s1, expression);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation setXDRFilter is not supported in dual mode");
            }
        });
    }

    @Override
    public void createUser(AdminPolicy adminPolicy, String s, String s1, List<String> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().createUser(adminPolicy, s, s1, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation createUser is not supported in dual mode");
            }
        });
    }

    @Override
    public void dropUser(AdminPolicy adminPolicy, String s) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().dropUser(adminPolicy, s);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation dropUser is not supported in dual mode");
            }
        });
    }

    @Override
    public void changePassword(AdminPolicy adminPolicy, String s, String s1) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().changePassword(adminPolicy, s, s1);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation changePassword is not supported in dual mode");
            }
        });
    }

    @Override
    public void grantRoles(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().grantRoles(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation grantRoles is not supported in dual mode");
            }
        });
    }

    @Override
    public void revokeRoles(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().revokeRoles(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation revokeRoles is not supported in dual mode");
            }
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().createRole(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation createRole is not supported in dual mode");
            }
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().createRole(adminPolicy, s, list, list1);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation createRole is not supported in dual mode");
            }
        });
    }

    @Override
    public void createRole(AdminPolicy adminPolicy, String s, List<Privilege> list, List<String> list1, int i, int i1) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().createRole(adminPolicy, s, list, list1, i, i1);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation createRole is not supported in dual mode");
            }
        });
    }

    @Override
    public void dropRole(AdminPolicy adminPolicy, String s) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().dropRole(adminPolicy, s);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation dropRole is not supported in dual mode");
            }
        });
    }

    @Override
    public void grantPrivileges(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().grantPrivileges(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation grantPrivileges is not supported in dual mode");
            }
        });
    }

    @Override
    public void revokePrivileges(AdminPolicy adminPolicy, String s, List<Privilege> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().revokePrivileges(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation revokePrivileges is not supported in dual mode");
            }
        });
    }

    @Override
    public void setWhitelist(AdminPolicy adminPolicy, String s, List<String> list) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().setWhitelist(adminPolicy, s, list);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation setWhitelist is not supported in dual mode");
            }
        });
    }

    @Override
    public void setQuotas(AdminPolicy adminPolicy, String s, int i, int i1) throws AerospikeException {
        val dualModeASReadWriteConfig = asClientResolver.getDualModeASReadWriteConfig();
        dualModeASReadWriteConfig.getWriteMode().accept(new WriteModeConfigVisitor<Void>() {
            @Override
            public Void visit(SingleSourceWriteMode singleSourceWriteMode) {
                asClientResolver.primaryWriteASClient().setQuotas(adminPolicy, s, i, i1);
                return null;
            }

            @Override
            public Void visit(final DualWriteMode dualWriteMode) {
                throw new AerospikeBundleException(ResponseCode.AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE,
                        "operation setQuotas is not supported in dual mode");
            }
        });
    }

    @Override
    public User queryUser(AdminPolicy adminPolicy, String s) throws AerospikeException {
        return asClientResolver.primaryASClient().queryUser(adminPolicy, s);
    }

    @Override
    public List<User> queryUsers(AdminPolicy adminPolicy) throws AerospikeException {
        return asClientResolver.primaryASClient().queryUsers(adminPolicy);
    }

    @Override
    public Role queryRole(AdminPolicy adminPolicy, String s) throws AerospikeException {
        return asClientResolver.primaryASClient().queryRole(adminPolicy, s);
    }

    @Override
    public List<Role> queryRoles(AdminPolicy adminPolicy) throws AerospikeException {
        return asClientResolver.primaryASClient().queryRoles(adminPolicy);
    }
}
