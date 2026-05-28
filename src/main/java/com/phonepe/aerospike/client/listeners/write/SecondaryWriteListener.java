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

@Slf4j
public class SecondaryWriteListener<T> implements WriteListener {

    private final WriteListener originalListener;
    private final boolean errorOnSecondaryWriteFailure;

    public SecondaryWriteListener(final WriteListener originalListener,
                                  final boolean errorOnSecondaryWriteFailure) {
        this.originalListener = originalListener;
        this.errorOnSecondaryWriteFailure = errorOnSecondaryWriteFailure;

    }

    @Override
    public void onSuccess(Key key) {
        if (!errorOnSecondaryWriteFailure) {
            return;
        }
        originalListener.onSuccess(key);
    }

    @Override
    public void onFailure(AerospikeException exception) {
        log.error("Secondary cluster write failed with message: " + exception.getMessage());
        if (!errorOnSecondaryWriteFailure) {
            return;
        }
        originalListener.onFailure(exception);
    }
}
