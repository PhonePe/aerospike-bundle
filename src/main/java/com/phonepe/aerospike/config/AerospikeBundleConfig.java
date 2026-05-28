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

package com.phonepe.aerospike.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.phonepe.aerospike.config.operation.AerospikeBundleConfigVisitor;
import com.phonepe.aerospike.config.operation.DefaultModeOfOperation;
import com.phonepe.aerospike.config.operation.DualModeOfOperation;
import com.phonepe.aerospike.config.operation.ModeOfOperationType;

import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "modeOfOperationType")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "DEFAULT_MODE", value = DefaultModeOfOperation.class),
        @JsonSubTypes.Type(name = "DUAL_MODE", value = DualModeOfOperation.class)
})
public abstract class AerospikeBundleConfig {
    private final ModeOfOperationType modeOfOperationType;

    protected AerospikeBundleConfig(final ModeOfOperationType modeOfOperationType) {
        this.modeOfOperationType = modeOfOperationType;
    }

    public abstract <T> T accept(final AerospikeBundleConfigVisitor<T> visitor);

}