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

package com.phonepe.aerospike.bundle;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import com.phonepe.aerospike.client.AerospikeClientWithMetrics;
import com.phonepe.aerospike.client.DualModeAerospikeClient;
import com.phonepe.aerospike.config.AerospikeBundleConfig;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.config.AerospikeHost;
import com.phonepe.aerospike.config.operation.AerospikeBundleConfigVisitor;
import com.phonepe.aerospike.config.operation.DefaultModeOfOperation;
import com.phonepe.aerospike.config.operation.DualModeASReadWriteConfig;
import com.phonepe.aerospike.config.operation.DualModeOfOperation;
import com.phonepe.aerospike.healthcheck.AerospikeHealthCheck;
import com.phonepe.aerospike.interceptors.AerospikeInterceptor;
import com.phonepe.aerospike.interceptors.internal.TerminalOperationInterceptor;
import com.phonepe.aerospike.interceptors.internal.metrics.MetricInterceptor;
import com.phonepe.aerospike.managed.DualModeASClientResolver;
import com.phonepe.aerospike.models.XdrLagDetails;
import com.phonepe.aerospike.services.AerospikeXdrService;
import com.phonepe.aerospike.util.AerospikeConfigValidationUtil;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Slf4j
public abstract class AerospikeBundle<T extends Configuration> implements ConfiguredBundle<T> {

    public static final String AEROSPIKE_BUNDLE_NAME = "aerospike-bundle";
    @Getter
    private IAerospikeClient aerospikeClient;

    private AerospikeXdrService xdrService;

    private MetricRegistry metricRegistry;

    @Getter
    private final List<AerospikeInterceptor> interceptors = new ArrayList<>();

    @Override
    public void initialize(Bootstrap bootstrap) {
        this.metricRegistry = bootstrap.getMetricRegistry();
    }

    @Override
    public void run(T configuration, Environment environment) {
        val aerospikeBundleConfig = this.configuration(configuration);
        this.aerospikeClient = aerospikeBundleConfig.accept(new AerospikeBundleConfigVisitor<IAerospikeClient>() {
            @Override
            public IAerospikeClient visit(final DefaultModeOfOperation modeOfOperation) {
                val asConfiguration = modeOfOperation.getAerospikeConfiguration();
                return buildIAerospikeClient(configuration, asConfiguration, environment,
                    getHealthCheckName(asConfiguration.getId()));
            }

            @Override
            public IAerospikeClient visit(final DualModeOfOperation dualModeOfOperationConfig) {
                validateDualModeOfOperationASConfig(dualModeOfOperationConfig);
                val asClientMap = new ConcurrentHashMap<String, IAerospikeClient>();
                dualModeOfOperationConfig.getAerospikeConfiguration().forEach(asConfig -> {
                    val asClient = buildIAerospikeClient(configuration,
                            asConfig,
                            environment,
                        getHealthCheckName(asConfig.getId()));
                    asClientMap.put(asConfig.getId(), asClient);
                });
                val asClientResolver = new DualModeASClientResolver(refreshDualModeASReadWriteConfig(configuration),
                        asClientMap);
                environment.lifecycle().manage(asClientResolver);
                return new DualModeAerospikeClient(asClientResolver);
            }
        });

        this.xdrService = new AerospikeXdrService(this.aerospikeClient);
    }

    private String getHealthCheckName(final String asConfigId) {
        return AEROSPIKE_BUNDLE_NAME + "-" + asConfigId;
    }

    private IAerospikeClient buildIAerospikeClient(T configuration,
                                                   final AerospikeConfiguration asConfiguration,
                                                   final Environment environment,
                                                   final String healthCheckName) {

        val writePolicy = configureWritePolicy(asConfiguration, this.writePolicy(configuration));
        val readPolicy = configureReadPolicy(asConfiguration, this.readPolicy(configuration));
        val scanPolicy = configureScanPolicy(asConfiguration, this.scanPolicy(configuration));
        val queryPolicy = configureQueryPolicy(asConfiguration, this.queryPolicy(configuration));
        val batchPolicy = configureBatchPolicy(asConfiguration, this.batchPolicy(configuration));
        val clientPolicy = configureClientPolicy(asConfiguration, readPolicy, writePolicy, scanPolicy, queryPolicy,
                batchPolicy, this.threadPool(configuration));

        log.info("Connecting to remote aerospike: hosts = {}", asConfiguration.getHosts());
        val aerospikeHosts = hosts(asConfiguration.getHosts());
        val asClient = new AerospikeClient(clientPolicy, aerospikeHosts.toArray(new Host[0]));

        val sizeMetricsEnabled = asConfiguration.isSizeMetricsEnabled();
        val interceptorContextKeyPopulationEnabled = asConfiguration.isInterceptorContextKeyPopulationEnabled();

        this.aerospikeClient = new AerospikeClientWithMetrics(asClient,
                getRootInterceptor(asConfiguration, metricRegistry),
                metricRegistry, sizeMetricsEnabled, interceptorContextKeyPopulationEnabled);
        if (asConfiguration.isHealthcheckEnabled()) {
            environment.healthChecks().register(healthCheckName, new AerospikeHealthCheck(aerospikeClient));
        }
        environment.jersey().register(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() {
                aerospikeClient.close();
            }
        });

        return aerospikeClient;
    }

    public final void registerInterceptor(final AerospikeInterceptor interceptor) {
        if (null == interceptor) {
            return;
        }
        this.interceptors.add(interceptor);
        log.info("Registered interceptor: " + interceptor.getClass().getSimpleName());
    }

    public Map<String, List<XdrLagDetails>> getXdrReplicationLag() {
        if (this.xdrService == null) {
            return Collections.emptyMap();
        }
        return this.xdrService.getXdrLag();
    }

    protected abstract AerospikeBundleConfig configuration(T configuration);

    @SuppressWarnings("java:S1172")
    protected Supplier<DualModeASReadWriteConfig> refreshDualModeASReadWriteConfig(T configuration) {
        return () -> null;
    }

    @SuppressWarnings("java:S1172")
    protected WritePolicy writePolicy(T configuration) {
        return null;
    }

    @SuppressWarnings("java:S1172")
    protected Policy readPolicy(T configuration) {
        return null;
    }

    @SuppressWarnings("java:S1172")
    protected ScanPolicy scanPolicy(T configuration) {
        return null;
    }

    @SuppressWarnings("java:S1172")
    protected QueryPolicy queryPolicy(T configuration) {
        return null;
    }

    @SuppressWarnings("java:S1172")
    protected BatchPolicy batchPolicy(T configuration) {
        return null;
    }

    @SuppressWarnings("java:S1172")
    protected ExecutorService threadPool(T configuration) {
        return null;
    }

    AerospikeInterceptor getRootInterceptor(final AerospikeConfiguration aerospikeConfiguration,
                                            final MetricRegistry metricRegistry) {
        AerospikeInterceptor rootInterceptor = new TerminalOperationInterceptor();
        for (AerospikeInterceptor interceptor : interceptors) {
            rootInterceptor = interceptor.setNext(rootInterceptor);
        }
        // Keeping metricInterceptor as the root
        rootInterceptor = new MetricInterceptor(aerospikeConfiguration, metricRegistry).setNext(rootInterceptor);
        return rootInterceptor;
    }

    private ClientPolicy configureClientPolicy(AerospikeConfiguration configuration,
                                               Policy readPolicy,
                                               WritePolicy writePolicy,
                                               ScanPolicy scanPolicy,
                                               QueryPolicy queryPolicy,
                                               BatchPolicy batchPolicy,
                                               ExecutorService providedThreadPool) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = configuration.getUser();
        clientPolicy.password = configuration.getPassword();
        clientPolicy.maxConnsPerNode = configuration.getMaxConnectionsPerNode();
        clientPolicy.failIfNotConnected = true;

        if (providedThreadPool == null) {
            clientPolicy.threadPool = configuration.getThreadPoolSize() > 0
                    ? Executors.newFixedThreadPool(configuration.getThreadPoolSize())
                    : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
        } else {
            clientPolicy.threadPool = providedThreadPool;
        }

        clientPolicy.readPolicyDefault = readPolicy;
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.scanPolicyDefault = scanPolicy;
        clientPolicy.queryPolicyDefault = queryPolicy;
        clientPolicy.batchPolicyDefault = batchPolicy;

        boolean isTlsNameProvided = !configuration.getHosts().stream()
                .allMatch(host -> Strings.isNullOrEmpty(host.getTlsName()));
        final Set<String> tlsProtocols = configuration.getTlsProtocols();
        if (isTlsNameProvided && !Strings.isNullOrEmpty(configuration.getUser())
                && !Strings.isNullOrEmpty(configuration.getPassword())) {
            clientPolicy.tlsPolicy = new TlsPolicy();
        }
        if (tlsProtocols != null && !tlsProtocols.isEmpty()) {
            clientPolicy.tlsPolicy.protocols = tlsProtocols.toArray(new String[0]);
        }

        clientPolicy.rackId = configuration.getRackId();
        clientPolicy.rackAware = configuration.isRackAware();

        if (configuration.getMaxSocketIdle() != null) {
            clientPolicy.maxSocketIdle = configuration.getMaxSocketIdle();
        }
        return clientPolicy;
    }

    private WritePolicy configureWritePolicy(AerospikeConfiguration aerospikeConfiguration,
                                             WritePolicy providedPolicy) {
        if (providedPolicy != null) {
            return providedPolicy;
        }

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.maxRetries = aerospikeConfiguration.getRetries();
        writePolicy.replica = aerospikeConfiguration.getReplica();
        writePolicy.sleepBetweenRetries = aerospikeConfiguration.getSleepBetweenRetries();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.socketTimeout = aerospikeConfiguration.getSocketTimeout();
        writePolicy.totalTimeout = aerospikeConfiguration.getTotalTimeout();
        writePolicy.sendKey = true;
        writePolicy.expiration = 0;
        return writePolicy;
    }

    private Policy configureReadPolicy(AerospikeConfiguration aerospikeConfiguration,
                                       Policy providedPolicy) {
        if (providedPolicy != null) {
            return providedPolicy;
        }

        Policy readPolicy = new Policy();
        readPolicy.maxRetries = aerospikeConfiguration.getRetries();
        readPolicy.replica = aerospikeConfiguration.getReplica();
        readPolicy.sleepBetweenRetries = aerospikeConfiguration.getSleepBetweenRetries();
        readPolicy.socketTimeout = aerospikeConfiguration.getSocketTimeout();
        readPolicy.totalTimeout = aerospikeConfiguration.getTotalTimeout();
        readPolicy.sendKey = true;
        readPolicy.readModeSC = aerospikeConfiguration.getReadModeSC();
        return readPolicy;
    }

    private ScanPolicy configureScanPolicy(AerospikeConfiguration aerospikeConfiguration,
                                           ScanPolicy providedPolicy) {
        if (providedPolicy != null) {
            return providedPolicy;
        }

        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = aerospikeConfiguration.getScanMaxConcurrentNodes();
        scanPolicy.includeBinData = true;
        return scanPolicy;
    }

    private QueryPolicy configureQueryPolicy(AerospikeConfiguration aerospikeConfiguration,
                                            QueryPolicy providedPolicy) {
        if (providedPolicy != null) {
            return providedPolicy;
        }
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.shortQuery = aerospikeConfiguration.isShortQuery();
        queryPolicy.maxConcurrentNodes = aerospikeConfiguration.getQueryMaxConcurrentNodes();
        queryPolicy.recordQueueSize = aerospikeConfiguration.getQueryRecordQueueSize();
        queryPolicy.includeBinData = true;
        return queryPolicy;
    }

    private BatchPolicy configureBatchPolicy(AerospikeConfiguration aerospikeConfiguration,
                                             BatchPolicy providedPolicy) {
        if (providedPolicy != null) {
            return providedPolicy;
        }

        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.maxConcurrentThreads = aerospikeConfiguration.getBatchMaxConcurrentThreads();
        batchPolicy.allowInline = true;
        batchPolicy.readModeSC = aerospikeConfiguration.getReadModeSC();
        return batchPolicy;
    }

    private List<Host> hosts(List<AerospikeHost> connections) {
        return connections.stream()
                .map(connection -> new Host(connection.getHost(), connection.getTlsName(), connection.getPort()))
                .toList();
    }

    private void validateDualModeOfOperationASConfig(@Valid final DualModeOfOperation config) {
        final Set<String> validClusterIds = new HashSet<>();

        config.getAerospikeConfiguration().forEach(aerospikeConfig -> {
            val clusterId = aerospikeConfig.getId();
            if (!validClusterIds.add(clusterId)) {
                throw new IllegalArgumentException("Duplicate cluster ID found: " + clusterId);
            }
        });

        AerospikeConfigValidationUtil.configValidation(config.getAsReadWriteConfig(), validClusterIds);
    }

}
