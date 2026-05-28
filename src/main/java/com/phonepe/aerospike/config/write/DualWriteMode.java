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

package com.phonepe.aerospike.config.write;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DualWriteMode extends WriteMode {

    @NotNull(message = "primary clusterId should not be null")
    private String primaryClusterId;

    @NotNull(message = "secondary clusterId should not be null")
    private String secondaryClusterId;

    private boolean errorOnSecondaryWriteFailure = true;

    public DualWriteMode() {
        super(WriteModeType.DUAL_WRITE_MODE);
    }

    @Builder
    public DualWriteMode(final String primaryClusterId,
                         final String secondaryClusterId,
                         boolean errorOnSecondaryWriteFailure) {
        super(WriteModeType.DUAL_WRITE_MODE);
        this.primaryClusterId = primaryClusterId;
        this.secondaryClusterId = secondaryClusterId;
        this.errorOnSecondaryWriteFailure = errorOnSecondaryWriteFailure;
    }

    @Override
    public <T> T accept(WriteModeConfigVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<String> clusterIds() {
        return Lists.newArrayList(primaryClusterId, secondaryClusterId);
    }

}
