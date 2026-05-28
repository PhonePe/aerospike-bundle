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

package com.phonepe.aerospike.util;

import com.google.common.base.Strings;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.exception.AerospikeBundleException;
import com.phonepe.aerospike.exception.ResponseCode;
import lombok.NoArgsConstructor;
import lombok.val;

import javax.validation.Valid;
import java.util.HashSet;
import java.util.Set;

@NoArgsConstructor
public class AerospikeConfigValidationUtil {

    public static void configValidation(@Valid final DualModeASReadWriteConfig config,
                                        final Set<String> validClusterIds) {
        val readClusterId = config.getReadMode().clusterId();
        val writeClusterIds = config.getWriteMode().clusterIds();
        if (Strings.isNullOrEmpty(readClusterId)
                || !validClusterIds.contains(readClusterId)
                || !new HashSet<>(writeClusterIds).contains(readClusterId)) {
            throw new AerospikeBundleException(ResponseCode.VALIDATION_ERROR,
                    "read cluster config not valid, readClusterId: " + readClusterId + " writeClusterIds: "
                            + writeClusterIds + " validClusterIds: " + validClusterIds);
        }


        if (writeClusterIds.stream().anyMatch(Strings::isNullOrEmpty)
                || !validClusterIds.containsAll(writeClusterIds)
                || writeClusterIds.size() != new HashSet<>(writeClusterIds).size()) {
            throw new AerospikeBundleException(ResponseCode.VALIDATION_ERROR,
                    "write cluster config not valid, writeClusterIds: " + writeClusterIds + " readClusterId: "
                            + readClusterId + " validClusterIds: " + validClusterIds);
        }
    }

}
