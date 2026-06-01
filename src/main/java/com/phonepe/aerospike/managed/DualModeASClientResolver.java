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

package com.phonepe.aerospike.managed;

import com.aerospike.client.IAerospikeClient;
import com.google.common.annotations.VisibleForTesting;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.config.read.SingleSourceReadMode;
import com.phonepe.aerospike.config.write.DualWriteMode;
import com.phonepe.aerospike.config.write.SingleSourceWriteMode;
import com.phonepe.aerospike.config.write.WriteModeConfigVisitor;
import com.phonepe.aerospike.util.AerospikeConfigValidationUtil;
import io.dropwizard.lifecycle.Managed;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class DualModeASClientResolver implements Managed {

    private final ScheduledExecutorService scheduler;

    private final AtomicReference<DualModeASReadWriteConfig> dualModeASReadWriteConfig;
    private Supplier<DualModeASReadWriteConfig> dualModeASReadWriteConfigSupplier;
    private final ConcurrentMap<String, IAerospikeClient> asClientMap;

    public DualModeASClientResolver(final Supplier<DualModeASReadWriteConfig> dualModeASReadWriteConfigSupplier,
                                    final ConcurrentMap<String, IAerospikeClient> asClientMap) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.dualModeASReadWriteConfig = new AtomicReference<>(dualModeASReadWriteConfigSupplier.get());
        this.dualModeASReadWriteConfigSupplier = dualModeASReadWriteConfigSupplier;
        this.asClientMap = asClientMap;
    }

    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(this::configRefresh,
                60,
                dualModeASReadWriteConfig.get().getConfigRefreshInSeconds(),
                TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduler.shutdown();
    }

    public IAerospikeClient primaryASClient() {
        return dualModeASReadWriteConfig.get().getReadMode().accept(
                (SingleSourceReadMode singleSourceReadMode) -> asClientMap.get(singleSourceReadMode.getClusterId()));
    }

    public IAerospikeClient primaryWriteASClient() {
        return dualModeASReadWriteConfig.get().getWriteMode().accept(new WriteModeConfigVisitor<IAerospikeClient>() {

            @Override
            public IAerospikeClient visit(final SingleSourceWriteMode singleSourceWriteMode) {
                return asClientMap.get(singleSourceWriteMode.getClusterId());
            }

            @Override
            public IAerospikeClient visit(final DualWriteMode dualWriteMode) {
                return asClientMap.get(dualWriteMode.getPrimaryClusterId());
            }
        });
    }

    public IAerospikeClient secondaryWriteASClient(final DualWriteMode dualWriteMode) {
        return asClientMap.get(dualWriteMode.getSecondaryClusterId());
    }

    public DualModeASReadWriteConfig getDualModeASReadWriteConfig() {
        return dualModeASReadWriteConfig.get();
    }

    public List<IAerospikeClient> getAllAerospikeClient() {
        return dualModeASReadWriteConfig.get().getWriteMode().clusterIds()
                .stream()
                .map(asClientMap::get)
                .toList();
    }

    @VisibleForTesting
    public void setDualModeASReadWriteConfigSupplier(final Supplier<DualModeASReadWriteConfig> newDualModeASReadWriteConfigSupplier) {
        dualModeASReadWriteConfigSupplier = newDualModeASReadWriteConfigSupplier;
    }

    public void configRefresh() {
        if (dualModeASReadWriteConfigSupplier == null || dualModeASReadWriteConfigSupplier.get() == null) {
            return;
        }

        val newDualModeASReadWriteConfig = dualModeASReadWriteConfigSupplier.get();
        boolean configRefreshedNeeded = !dualModeASReadWriteConfig.get().toString().equals(newDualModeASReadWriteConfig.toString());
        if (!configRefreshedNeeded) {
            return;
        }
        try {
            val validClusterIds = asClientMap.keySet().stream()
                    .filter(clientId -> asClientMap.get(clientId) != null)
                    .collect(Collectors.toSet());

            // config validation
            AerospikeConfigValidationUtil.configValidation(newDualModeASReadWriteConfig, validClusterIds);

            // setting new config as default config
            dualModeASReadWriteConfig.set(newDualModeASReadWriteConfig);
            log.info("config refreshed: new config set to: {}", dualModeASReadWriteConfig.get());
        } catch (Exception e) {
            log.error("exception while validating config, new config will not be used ", e);
        }
    }

    public Map<String, IAerospikeClient> getClientMap() {
        return Collections.unmodifiableMap(asClientMap);
    }

}