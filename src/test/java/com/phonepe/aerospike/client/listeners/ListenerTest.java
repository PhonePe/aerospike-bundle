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

package com.phonepe.aerospike.client.listeners;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.*;
import com.phonepe.aerospike.client.listeners.batch.*;
import com.phonepe.aerospike.client.listeners.delete.PrimaryDeleteListener;
import com.phonepe.aerospike.client.listeners.delete.SecondaryDeleteListener;
import com.phonepe.aerospike.client.listeners.execute.PrimaryExecuteListener;
import com.phonepe.aerospike.client.listeners.execute.SecondaryExecuteListener;
import com.phonepe.aerospike.client.listeners.write.PrimaryRecordListener;
import com.phonepe.aerospike.client.listeners.write.PrimaryWriteListener;
import com.phonepe.aerospike.client.listeners.write.SecondaryRecordListener;
import com.phonepe.aerospike.client.listeners.write.SecondaryWriteListener;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.Mockito.*;

public class ListenerTest {

    private final Key testKey = new Key("ns", "set", "k1");
    private final AerospikeException testException = new AerospikeException("test error");

    // --- PrimaryWriteListener ---

    @Test
    public void testPrimaryWriteListenerOnSuccessWithErrorOnSecondaryFalse() {
        WriteListener original = mock(WriteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryWriteListener<Void> listener = new PrimaryWriteListener<>(original, supplier, false);
        listener.onSuccess(testKey);
        verify(original).onSuccess(testKey);
        verify(supplier).get();
    }

    @Test
    public void testPrimaryWriteListenerOnSuccessWithErrorOnSecondaryTrue() {
        WriteListener original = mock(WriteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryWriteListener<Void> listener = new PrimaryWriteListener<>(original, supplier, true);
        listener.onSuccess(testKey);
        verify(original, never()).onSuccess(any());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryWriteListenerOnFailure() {
        WriteListener original = mock(WriteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryWriteListener<Void> listener = new PrimaryWriteListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testPrimaryWriteListenerSupplierInvokedOnce() {
        WriteListener original = mock(WriteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryWriteListener<Void> listener = new PrimaryWriteListener<>(original, supplier, false);
        listener.onSuccess(testKey);
        listener.onSuccess(testKey); // second call
        verify(supplier, times(1)).get(); // only invoked once
    }

    // --- SecondaryWriteListener ---

    @Test
    public void testSecondaryWriteListenerOnSuccessWithErrorTrue() {
        WriteListener original = mock(WriteListener.class);
        SecondaryWriteListener<Void> listener = new SecondaryWriteListener<>(original, true);
        listener.onSuccess(testKey);
        verify(original).onSuccess(testKey);
    }

    @Test
    public void testSecondaryWriteListenerOnSuccessWithErrorFalse() {
        WriteListener original = mock(WriteListener.class);
        SecondaryWriteListener<Void> listener = new SecondaryWriteListener<>(original, false);
        listener.onSuccess(testKey);
        verify(original, never()).onSuccess(any());
    }

    @Test
    public void testSecondaryWriteListenerOnFailureWithErrorTrue() {
        WriteListener original = mock(WriteListener.class);
        SecondaryWriteListener<Void> listener = new SecondaryWriteListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryWriteListenerOnFailureWithErrorFalse() {
        WriteListener original = mock(WriteListener.class);
        SecondaryWriteListener<Void> listener = new SecondaryWriteListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }

    // --- PrimaryRecordListener ---

    @Test
    public void testPrimaryRecordListenerOnSuccessWithErrorFalse() {
        RecordListener original = mock(RecordListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryRecordListener<Void> listener = new PrimaryRecordListener<>(original, supplier, false);
        Record testRecord = new Record(null, 1, 1);
        listener.onSuccess(testKey, testRecord);
        verify(original).onSuccess(testKey, testRecord);
        verify(supplier).get();
    }

    @Test
    public void testPrimaryRecordListenerOnSuccessWithErrorTrue() {
        RecordListener original = mock(RecordListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryRecordListener<Void> listener = new PrimaryRecordListener<>(original, supplier, true);
        Record testRecord = new Record(null, 1, 1);
        listener.onSuccess(testKey, testRecord);
        verify(original, never()).onSuccess(any(), any());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryRecordListenerOnFailure() {
        RecordListener original = mock(RecordListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryRecordListener<Void> listener = new PrimaryRecordListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    // --- SecondaryRecordListener ---

    @Test
    public void testSecondaryRecordListenerOnSuccessWithErrorTrue() {
        RecordListener original = mock(RecordListener.class);
        SecondaryRecordListener<Void> listener = new SecondaryRecordListener<>(original, true);
        Record testRecord = new Record(null, 1, 1);
        listener.onSuccess(testKey, testRecord);
        verify(original).onSuccess(testKey, testRecord);
    }

    @Test
    public void testSecondaryRecordListenerOnSuccessWithErrorFalse() {
        RecordListener original = mock(RecordListener.class);
        SecondaryRecordListener<Void> listener = new SecondaryRecordListener<>(original, false);
        listener.onSuccess(testKey, new Record(null, 1, 1));
        verify(original, never()).onSuccess(any(), any());
    }

    @Test
    public void testSecondaryRecordListenerOnFailureWithErrorTrue() {
        RecordListener original = mock(RecordListener.class);
        SecondaryRecordListener<Void> listener = new SecondaryRecordListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryRecordListenerOnFailureWithErrorFalse() {
        RecordListener original = mock(RecordListener.class);
        SecondaryRecordListener<Void> listener = new SecondaryRecordListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }

    // --- PrimaryDeleteListener ---

    @Test
    public void testPrimaryDeleteListenerOnSuccessWithErrorFalse() {
        DeleteListener original = mock(DeleteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryDeleteListener<Void> listener = new PrimaryDeleteListener<>(original, supplier, false);
        listener.onSuccess(testKey, true);
        verify(original).onSuccess(testKey, true);
        verify(supplier).get();
    }

    @Test
    public void testPrimaryDeleteListenerOnSuccessWithErrorTrue() {
        DeleteListener original = mock(DeleteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryDeleteListener<Void> listener = new PrimaryDeleteListener<>(original, supplier, true);
        listener.onSuccess(testKey, true);
        verify(original, never()).onSuccess(any(), anyBoolean());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryDeleteListenerOnFailure() {
        DeleteListener original = mock(DeleteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryDeleteListener<Void> listener = new PrimaryDeleteListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    // --- SecondaryDeleteListener ---

    @Test
    public void testSecondaryDeleteListenerOnSuccessWithErrorTrue() {
        DeleteListener original = mock(DeleteListener.class);
        SecondaryDeleteListener<Void> listener = new SecondaryDeleteListener<>(original, true);
        listener.onSuccess(testKey, true);
        verify(original).onSuccess(testKey, true);
    }

    @Test
    public void testSecondaryDeleteListenerOnSuccessWithErrorFalse() {
        DeleteListener original = mock(DeleteListener.class);
        SecondaryDeleteListener<Void> listener = new SecondaryDeleteListener<>(original, false);
        listener.onSuccess(testKey, true);
        verify(original, never()).onSuccess(any(), anyBoolean());
    }

    @Test
    public void testSecondaryDeleteListenerOnFailureWithErrorTrue() {
        DeleteListener original = mock(DeleteListener.class);
        SecondaryDeleteListener<Void> listener = new SecondaryDeleteListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryDeleteListenerOnFailureWithErrorFalse() {
        DeleteListener original = mock(DeleteListener.class);
        SecondaryDeleteListener<Void> listener = new SecondaryDeleteListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }

    // --- PrimaryExecuteListener ---

    @Test
    public void testPrimaryExecuteListenerOnSuccessWithErrorFalse() {
        ExecuteListener original = mock(ExecuteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryExecuteListener<Void> listener = new PrimaryExecuteListener<>(original, supplier, false);
        listener.onSuccess(testKey, "result");
        verify(original).onSuccess(testKey, "result");
        verify(supplier).get();
    }

    @Test
    public void testPrimaryExecuteListenerOnSuccessWithErrorTrue() {
        ExecuteListener original = mock(ExecuteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryExecuteListener<Void> listener = new PrimaryExecuteListener<>(original, supplier, true);
        listener.onSuccess(testKey, "result");
        verify(original, never()).onSuccess(any(), any());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryExecuteListenerOnFailure() {
        ExecuteListener original = mock(ExecuteListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryExecuteListener<Void> listener = new PrimaryExecuteListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    // --- SecondaryExecuteListener ---

    @Test
    public void testSecondaryExecuteListenerOnSuccessWithErrorTrue() {
        ExecuteListener original = mock(ExecuteListener.class);
        SecondaryExecuteListener<Void> listener = new SecondaryExecuteListener<>(original, true);
        listener.onSuccess(testKey, "result");
        verify(original).onSuccess(testKey, "result");
    }

    @Test
    public void testSecondaryExecuteListenerOnSuccessWithErrorFalse() {
        ExecuteListener original = mock(ExecuteListener.class);
        SecondaryExecuteListener<Void> listener = new SecondaryExecuteListener<>(original, false);
        listener.onSuccess(testKey, "result");
        verify(original, never()).onSuccess(any(), any());
    }

    @Test
    public void testSecondaryExecuteListenerOnFailureWithErrorTrue() {
        ExecuteListener original = mock(ExecuteListener.class);
        SecondaryExecuteListener<Void> listener = new SecondaryExecuteListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryExecuteListenerOnFailureWithErrorFalse() {
        ExecuteListener original = mock(ExecuteListener.class);
        SecondaryExecuteListener<Void> listener = new SecondaryExecuteListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }

    // --- PrimaryBatchRecordArrayListener ---

    @Test
    public void testPrimaryBatchRecordArrayListenerOnSuccessWithErrorFalse() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordArrayListener<Void> listener = new PrimaryBatchRecordArrayListener<>(original, supplier, false);
        BatchRecord[] records = new BatchRecord[0];
        listener.onSuccess(records, true);
        verify(original).onSuccess(records, true);
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchRecordArrayListenerOnSuccessWithErrorTrue() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordArrayListener<Void> listener = new PrimaryBatchRecordArrayListener<>(original, supplier, true);
        BatchRecord[] records = new BatchRecord[0];
        listener.onSuccess(records, true);
        verify(original, never()).onSuccess(any(), anyBoolean());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchRecordArrayListenerOnFailure() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordArrayListener<Void> listener = new PrimaryBatchRecordArrayListener<>(original, supplier, true);
        listener.onFailure(new BatchRecord[0], testException);
        verify(original).onFailure(any(), eq(testException));
    }

    // --- SecondaryBatchRecordArrayListener ---

    @Test
    public void testSecondaryBatchRecordArrayListenerOnSuccessWithErrorTrue() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        SecondaryBatchRecordArrayListener<Void> listener = new SecondaryBatchRecordArrayListener<>(original, true);
        BatchRecord[] records = new BatchRecord[0];
        listener.onSuccess(records, true);
        verify(original).onSuccess(records, true);
    }

    @Test
    public void testSecondaryBatchRecordArrayListenerOnSuccessWithErrorFalse() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        SecondaryBatchRecordArrayListener<Void> listener = new SecondaryBatchRecordArrayListener<>(original, false);
        listener.onSuccess(new BatchRecord[0], true);
        verify(original, never()).onSuccess(any(), anyBoolean());
    }

    @Test
    public void testSecondaryBatchRecordArrayListenerOnFailureWithErrorTrue() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        SecondaryBatchRecordArrayListener<Void> listener = new SecondaryBatchRecordArrayListener<>(original, true);
        listener.onFailure(new BatchRecord[0], testException);
        verify(original).onFailure(any(), eq(testException));
    }

    @Test
    public void testSecondaryBatchRecordArrayListenerOnFailureWithErrorFalse() {
        BatchRecordArrayListener original = mock(BatchRecordArrayListener.class);
        SecondaryBatchRecordArrayListener<Void> listener = new SecondaryBatchRecordArrayListener<>(original, false);
        listener.onFailure(new BatchRecord[0], testException);
        verify(original, never()).onFailure(any(), any());
    }

    // --- PrimaryBatchRecordSequenceListener ---

    @Test
    public void testPrimaryBatchRecordSequenceListenerOnRecordWithErrorFalse() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordSequenceListener<Void> listener = new PrimaryBatchRecordSequenceListener<>(original, supplier, false);
        BatchRecord br = mock(BatchRecord.class);
        listener.onRecord(br, 0);
        verify(original).onRecord(br, 0);
    }

    @Test
    public void testPrimaryBatchRecordSequenceListenerOnSuccessWithErrorFalse() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordSequenceListener<Void> listener = new PrimaryBatchRecordSequenceListener<>(original, supplier, false);
        listener.onSuccess();
        verify(original).onSuccess();
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchRecordSequenceListenerOnSuccessWithErrorTrue() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordSequenceListener<Void> listener = new PrimaryBatchRecordSequenceListener<>(original, supplier, true);
        listener.onSuccess();
        verify(original, never()).onSuccess();
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchRecordSequenceListenerOnFailure() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchRecordSequenceListener<Void> listener = new PrimaryBatchRecordSequenceListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    // --- SecondaryBatchRecordSequenceListener ---

    @Test
    public void testSecondaryBatchRecordSequenceListenerOnRecordWithErrorTrue() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        SecondaryBatchRecordSequenceListener<Void> listener = new SecondaryBatchRecordSequenceListener<>(original, true);
        BatchRecord br = mock(BatchRecord.class);
        listener.onRecord(br, 0);
        // onRecord does nothing in secondary listener
        verify(original, never()).onRecord(any(), anyInt());
    }

    @Test
    public void testSecondaryBatchRecordSequenceListenerOnSuccessWithErrorTrue() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        SecondaryBatchRecordSequenceListener<Void> listener = new SecondaryBatchRecordSequenceListener<>(original, true);
        listener.onSuccess();
        verify(original).onSuccess();
    }

    @Test
    public void testSecondaryBatchRecordSequenceListenerOnSuccessWithErrorFalse() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        SecondaryBatchRecordSequenceListener<Void> listener = new SecondaryBatchRecordSequenceListener<>(original, false);
        listener.onSuccess();
        verify(original, never()).onSuccess();
    }

    @Test
    public void testSecondaryBatchRecordSequenceListenerOnFailureWithErrorTrue() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        SecondaryBatchRecordSequenceListener<Void> listener = new SecondaryBatchRecordSequenceListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryBatchRecordSequenceListenerOnFailureWithErrorFalse() {
        BatchRecordSequenceListener original = mock(BatchRecordSequenceListener.class);
        SecondaryBatchRecordSequenceListener<Void> listener = new SecondaryBatchRecordSequenceListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }

    // --- PrimaryBatchOperateListListener ---

    @Test
    public void testPrimaryBatchOperateListListenerOnSuccessWithErrorFalse() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchOperateListListener<Void> listener = new PrimaryBatchOperateListListener<>(original, supplier, false);
        List<BatchRecord> records = new ArrayList<>();
        listener.onSuccess(records, true);
        verify(original).onSuccess(records, true);
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchOperateListListenerOnSuccessWithErrorTrue() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchOperateListListener<Void> listener = new PrimaryBatchOperateListListener<>(original, supplier, true);
        listener.onSuccess(new ArrayList<>(), true);
        verify(original, never()).onSuccess(any(), anyBoolean());
        verify(supplier).get();
    }

    @Test
    public void testPrimaryBatchOperateListListenerOnFailure() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        Supplier<Void> supplier = mock(Supplier.class);
        PrimaryBatchOperateListListener<Void> listener = new PrimaryBatchOperateListListener<>(original, supplier, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    // --- SecondaryBatchOperateListListener ---

    @Test
    public void testSecondaryBatchOperateListListenerOnSuccessWithErrorTrue() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        SecondaryBatchOperateListListener<Void> listener = new SecondaryBatchOperateListListener<>(original, true);
        List<BatchRecord> records = new ArrayList<>();
        listener.onSuccess(records, true);
        verify(original).onSuccess(records, true);
    }

    @Test
    public void testSecondaryBatchOperateListListenerOnSuccessWithErrorFalse() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        SecondaryBatchOperateListListener<Void> listener = new SecondaryBatchOperateListListener<>(original, false);
        listener.onSuccess(new ArrayList<>(), true);
        verify(original, never()).onSuccess(any(), anyBoolean());
    }

    @Test
    public void testSecondaryBatchOperateListListenerOnFailureWithErrorTrue() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        SecondaryBatchOperateListListener<Void> listener = new SecondaryBatchOperateListListener<>(original, true);
        listener.onFailure(testException);
        verify(original).onFailure(testException);
    }

    @Test
    public void testSecondaryBatchOperateListListenerOnFailureWithErrorFalse() {
        BatchOperateListListener original = mock(BatchOperateListListener.class);
        SecondaryBatchOperateListListener<Void> listener = new SecondaryBatchOperateListListener<>(original, false);
        listener.onFailure(testException);
        verify(original, never()).onFailure(any());
    }
}
