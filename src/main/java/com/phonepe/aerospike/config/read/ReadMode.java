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

package com.phonepe.aerospike.config.read;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

import java.util.List;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "readModeType")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "SINGLE_SOURCE_READ_MODE", value = SingleSourceReadMode.class)
})
public abstract class ReadMode {
    private final ReadModeType readModeType;

    protected ReadMode(final ReadModeType readModeType) {
        this.readModeType = readModeType;
    }

    public abstract <T> T accept(final ReadModeConfigVisitor<T> visitor);

    public abstract String clusterId();

}