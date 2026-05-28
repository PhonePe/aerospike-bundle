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

package com.phonepe.aerospike.config.operation;

public abstract class ASConfigVisitorAdaptor<T> implements AerospikeBundleConfigVisitor<T> {

    private final T defaultValue;

    protected ASConfigVisitorAdaptor() {
        this(null);
    }

    protected ASConfigVisitorAdaptor(final T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public T visit(DefaultModeOfOperation modeOfOperation) {
        return this.defaultValue;
    }

    public T visit(DualModeOfOperation modeOfOperation) {
        return this.defaultValue;
    }

}