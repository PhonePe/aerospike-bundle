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

import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.Replica;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AerospikeConfiguration {

    @NotNull(message = "id should not be null")
    private String id;

    @NotNull
    @Size(max = 256)
    private List<AerospikeHost> hosts;

    private String user;

    private String password;

    private Set<String> tlsProtocols;

    @Min(0)
    @Builder.Default
    private int retries = 5;

    @Min(10)
    @Max(10000)
    @Builder.Default
    private int sleepBetweenRetries = 100;

    @Min(1)
    @Builder.Default
    private int maxConnectionsPerNode = 5;

    @Min(0)
    private int socketTimeout;

    @Min(0)
    private int totalTimeout;

    @Min(0)
    @Max(1000)
    private int threadPoolSize = 0;

    // QueryPolicy configurations
    @Min(0)
    @Max(100)
    private int queryMaxConcurrentNodes = 0;
    private boolean shortQuery = true;
    private int queryRecordQueueSize = 5000;

    @Min(0)
    @Max(100)
    private int scanMaxConcurrentNodes = 0;

    @Min(0)
    @Max(100)
    private int batchMaxConcurrentThreads = 1;

    private boolean healthcheckEnabled;

    @Builder.Default
    private boolean metricsEnabled = false;

    @Builder.Default
    private boolean sizeMetricsEnabled = false;

    // Set to true if you need the Value[] of the Key[] populated in the interceptor context
    @Builder.Default
    private boolean interceptorContextKeyPopulationEnabled = false;

    @Builder.Default
    private ReadModeSC readModeSC = ReadModeSC.SESSION;

    // Set to Replica.MASTER when readModeSC is LINEARIZE.
    @Builder.Default
    private Replica replica = Replica.MASTER_PROLES;

    @Builder.Default
    @Valid
    private List<Namespace> namespaces = new LinkedList<>();

    @Builder.Default
    private int rackId = 0;

    @Builder.Default
    private boolean rackAware = false;

    private Integer maxSocketIdle;

    public Optional<Namespace> getNamespaceFor(String tag) {
        return getNamespaces().stream().filter(n -> n.getTags().contains(tag)).findFirst();
    }

    // "when using @ValidationMethod, the method must begin with is"
    @ValidationMethod(message = "duplicate tags are not allowed across namespaces")
    // Tag across all the listed namespaces required to be unique
    public boolean isAllTagUniqueAcrossAllNamespaces() {
        List<String> tags = getNamespaces().stream().flatMap(n -> n.getTags().stream()).collect(Collectors.toList());
        return tags.size() == new HashSet<>(tags).size();
    }
}
