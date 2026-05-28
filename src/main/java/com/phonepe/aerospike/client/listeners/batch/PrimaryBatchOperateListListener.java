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

package com.phonepe.aerospike.client.listeners.batch;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchOperateListListener;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class PrimaryBatchOperateListListener<T> implements BatchOperateListListener {

    private final BatchOperateListListener originalListener;
    private final Supplier<T> supplier;
    private boolean supplierInvoked;
    private final boolean errorOnSecondaryWriteFailure;

    public PrimaryBatchOperateListListener(final BatchOperateListListener originalListener,
                                           final Supplier<T> supplier,
                                           final boolean errorOnSecondaryWriteFailure) {
        this.originalListener = originalListener;
        this.supplier = supplier;
        this.supplierInvoked = false;
        this.errorOnSecondaryWriteFailure = errorOnSecondaryWriteFailure;

    }

    @Override
    public void onSuccess(List<BatchRecord> list, boolean b) {
        try {
            if (!errorOnSecondaryWriteFailure) {
                originalListener.onSuccess(list, b);
            }
        } catch (Exception exception) {
            log.error("exception while invoking listener: error message " + exception.getMessage());
        } finally {
            if (!supplierInvoked) {
                supplier.get();
                supplierInvoked = true;
            }
        }
    }

    @Override
    public void onFailure(AerospikeException exception) {
        originalListener.onFailure(exception);
    }
}