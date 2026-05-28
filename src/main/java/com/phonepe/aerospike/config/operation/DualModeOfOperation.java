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

import com.phonepe.aerospike.config.AerospikeBundleConfig;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DualModeOfOperation extends AerospikeBundleConfig {

    @Valid
    @NotNull
    private DualModeASReadWriteConfig asReadWriteConfig;

    @Valid
    @Min(1)
    @Max(2)
    private List<AerospikeConfiguration> aerospikeConfiguration;

    public DualModeOfOperation() {
        super(ModeOfOperationType.DUAL_MODE);
    }

    @Builder
    public DualModeOfOperation(final DualModeASReadWriteConfig asReadWriteConfig,
                               final List<AerospikeConfiguration> aerospikeConfiguration) {
        super(ModeOfOperationType.DUAL_MODE);
        this.asReadWriteConfig = asReadWriteConfig;
        this.aerospikeConfiguration = aerospikeConfiguration;
    }

    @Override
    public <T> T accept(AerospikeBundleConfigVisitor<T> visitor) {
        return visitor.visit(this);
    }

}