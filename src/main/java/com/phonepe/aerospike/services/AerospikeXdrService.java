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

package com.phonepe.aerospike.services;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.phonepe.aerospike.client.DualModeAerospikeClient;
import com.phonepe.aerospike.models.XdrLagDetails;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class AerospikeXdrService {

    private static final String DEFAULT_CLUSTER_ID = "default";
    private static final String CMD_GET_XDR_CONFIG = "get-config:context=xdr";
    private static final String CMD_GET_XDR_STATS_PREFIX = "get-stats:context=xdr;dc=";
    private static final String KEY_DCS = "dcs";
    private static final String KEY_LAG = "lag";
    private static final String KEY_STATE = "state";
    private static final String VAL_ZERO = "0";
    private static final String VAL_UNKNOWN = "UNKNOWN";
    private static final String DELIMITER_COMMA = ",";
    private static final String DELIMITER_SEMICOLON = ";";
    private static final String DELIMITER_EQUALS = "=";
    private static final String DELIMITER_COLON = ":";

    private final IAerospikeClient aerospikeClient;

    public AerospikeXdrService(IAerospikeClient aerospikeClient) {
        this.aerospikeClient = aerospikeClient;
    }

    public Map<String, List<XdrLagDetails>> getXdrLag() {
        Map<String, List<XdrLagDetails>> result = new HashMap<>();

        if (aerospikeClient instanceof DualModeAerospikeClient dualClient) {
            val clientMap = dualClient.getAsClientResolver().getClientMap();

            clientMap.forEach((clusterId, client) -> {
                if (client != null && client.isConnected()) {
                    result.put(clusterId, fetchLagForClient(clusterId, client));
                }
            });
        } else {
            if (aerospikeClient != null && aerospikeClient.isConnected()) {
                result.put(DEFAULT_CLUSTER_ID, fetchLagForClient(DEFAULT_CLUSTER_ID, aerospikeClient));
            }
        }
        return result;
    }

    private List<XdrLagDetails> fetchLagForClient(final String clusterId, final IAerospikeClient client) {
        List<XdrLagDetails> details = new ArrayList<>();
        try {
            for (val node : client.getNodes()) {
                details.addAll(fetchNodeXdrStats(clusterId, node));
            }
        } catch (Exception e) {
            log.error("Failed to fetch XDR stats for cluster: {}", clusterId, e);
        }
        return details;
    }

    private List<XdrLagDetails> fetchNodeXdrStats(final String clusterId, final Node node) {
        List<XdrLagDetails> nodeStats = new ArrayList<>();
        try {
            //sample config: dcs=centralplatform_mh6;src-id=0;trace-sample=0
            val xdrConfig = Info.request(node, CMD_GET_XDR_CONFIG);

            if (xdrConfig == null || xdrConfig.isEmpty()) {
                return Collections.emptyList();
            }

            val configMap = parseInfoString(xdrConfig);
            val dcs = configMap.get(KEY_DCS);

            if (dcs == null || dcs.isEmpty()) {
                return Collections.emptyList();
            }

            for (val dc : dcs.split(DELIMITER_COMMA)) {
                try {
                    //sample stats: lag=0;in_queue=0;in_progress=0;success=695;abandoned=0;not_found=0;filtered_out=0;retry_no_node=0;retry_conn_reset=0;retry_dest=0;recoveries=2048;recoveries_pending=0;hot_keys=593;bytes_shipped=676929;uncompressed_pct=1.222;compression_ratio=0.769;nodes=2;throughput=0;latency_ms=2;lap_us=1983
                    val statsStr = Info.request(node, CMD_GET_XDR_STATS_PREFIX + dc);
                    val statsMap = parseInfoString(statsStr);

                    val lag = Long.parseLong(statsMap.getOrDefault(KEY_LAG, VAL_ZERO));
                    val status = statsMap.getOrDefault(KEY_STATE, VAL_UNKNOWN);

                    nodeStats.add(XdrLagDetails.builder()
                        .clusterId(clusterId)
                        .nodeHost(node.getHost().name + DELIMITER_COLON + node.getHost().port)
                        .targetDc(dc)
                        .lag(lag)
                        .status(status)
                        .build());
                } catch (Exception e) {
                    log.warn("Error fetching stats for DC {} on node {}", dc, node.getName());
                }
            }
        } catch (Exception e) {
            log.error("Error fetching XDR info for node {}", node.getName(), e);
        }
        return nodeStats;
    }

    private Map<String, String> parseInfoString(final String info) {
        if (info == null || info.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> resultMap = new HashMap<>();

        for (val pair : info.split(DELIMITER_SEMICOLON)) {
            val kv = pair.split(DELIMITER_EQUALS);
            if (kv.length == 2) {
                resultMap.put(kv[0], kv[1]);
            }
        }
        return resultMap;
    }
}