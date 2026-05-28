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

package com.phonepe.aerospike;

import com.aerospike.client.policy.ReadModeSC;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class AerospikeConfigurationTest {

    private AerospikeConfiguration readYaml(String name) throws ConfigurationException, IOException {
        final ObjectMapper objectMapper = Jackson.newObjectMapper();
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        final YamlConfigurationFactory<AerospikeConfiguration> factory = new YamlConfigurationFactory<>(AerospikeConfiguration.class, validator, objectMapper, "dw");
        final File yaml = new File(Thread.currentThread().getContextClassLoader().getResource(name).getPath());
        return factory.build(yaml);
    }

    @Test
    public void testValidNsConfig() throws ConfigurationException, IOException {
        AerospikeConfiguration configuration  = readYaml("with_namespaces.yaml");
        Assert.assertEquals("ns1", configuration.getNamespaceFor("default_ns").get().getName());
        Assert.assertEquals("ns2_ap", configuration.getNamespaceFor("usecase_ap_1").get().getName());
        Assert.assertEquals("ns3_sc", configuration.getNamespaceFor("usecase_sc_1").get().getName());
        Assert.assertEquals(ReadModeSC.SESSION, configuration.getReadModeSC());
    }

    @Test
    public void testValidConfigWithoutNs() throws ConfigurationException, IOException {
        AerospikeConfiguration configuration  = readYaml("without_namespaces.yaml");
        Assert.assertEquals(Optional.empty(), configuration.getNamespaceFor("default_ns"));
    }

    @Test(expected = ConfigurationValidationException.class)
    public void testInValidNsConfig() throws ConfigurationException, IOException {
       readYaml("invalid_namespaces.yaml");
    }
}
