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

package com.phonepe.aerospike.client.listeners.write;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class PrimaryWriteListener<T> implements WriteListener {

    private final WriteListener originalListener;
    private final Supplier<T> supplier;
    private boolean supplierInvoked;
    private final boolean errorOnSecondaryWriteFailure;

    public PrimaryWriteListener(final WriteListener originalListener,
                                final Supplier<T> supplier,
                                final boolean errorOnSecondaryWriteFailure) {
        this.originalListener = originalListener;
        this.supplier = supplier;
        this.supplierInvoked = false;
        this.errorOnSecondaryWriteFailure = errorOnSecondaryWriteFailure;
    }

    @Override
    public void onSuccess(Key key) {
        // if errorOnSecondaryWriteFailure is true
        // Then we wait for secondary AS operation to successfully complete and invoke success of originalListener in SecondaryOperationListener
        // else success of originalListener is invoked in PrimaryOperationLister
        try {
            if (!errorOnSecondaryWriteFailure) {
                originalListener.onSuccess(key);
            }
        } catch (Exception exception) {
            log.error("exception while invoking listener: error message " + exception.getMessage());
        } finally {
            if (!supplierInvoked) {
                supplier.get();
                // Mark the supplier as invoked
                supplierInvoked = true;
            }
        }
    }

    @Override
    public void onFailure(AerospikeException exception) {
        originalListener.onFailure(exception);
    }
}
