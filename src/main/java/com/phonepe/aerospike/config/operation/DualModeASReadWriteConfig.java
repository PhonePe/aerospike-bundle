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

import com.phonepe.aerospike.config.read.ReadMode;
import com.phonepe.aerospike.config.write.WriteMode;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
public class DualModeASReadWriteConfig {

    @NotNull(message = "readMode should not be null")
    private ReadMode readMode;

    @NotNull(message = "writeMode should not be null")
    private WriteMode writeMode;

    @Min(1)
    private long configRefreshInSeconds;

    @Builder
    public DualModeASReadWriteConfig(final ReadMode readMode,
                                     final WriteMode writeMode,
                                     final long configRefreshInSeconds) {
        this.readMode = readMode;
        this.writeMode = writeMode;
        this.configRefreshInSeconds = configRefreshInSeconds;
    }

}
