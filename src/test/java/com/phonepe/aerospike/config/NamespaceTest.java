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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NamespaceTest {

    @Test
    public void testBuilderAndGetters() {
        List<String> tags = Arrays.asList("tag1", "tag2");
        Namespace ns = Namespace.builder()
                .consistency(Namespace.CONSISTENCY.AP)
                .replicationFactor(2)
                .storage(Namespace.STORAGE.IN_MEMORY)
                .name("test-ns")
                .tags(tags)
                .build();

        Assert.assertEquals(Namespace.CONSISTENCY.AP, ns.getConsistency());
        Assert.assertEquals(2, ns.getReplicationFactor());
        Assert.assertEquals(Namespace.STORAGE.IN_MEMORY, ns.getStorage());
        Assert.assertEquals("test-ns", ns.getName());
        Assert.assertEquals(tags, ns.getTags());
    }

    @Test
    public void testConsistencyEnum() {
        Assert.assertEquals(2, Namespace.CONSISTENCY.values().length);
        Assert.assertNotNull(Namespace.CONSISTENCY.valueOf("AP"));
        Assert.assertNotNull(Namespace.CONSISTENCY.valueOf("SC"));
    }

    @Test
    public void testStorageEnum() {
        Assert.assertEquals(3, Namespace.STORAGE.values().length);
        Assert.assertNotNull(Namespace.STORAGE.valueOf("IN_MEMORY"));
        Assert.assertNotNull(Namespace.STORAGE.valueOf("FILE"));
        Assert.assertNotNull(Namespace.STORAGE.valueOf("RAW_DEVICE"));
    }

    @Test
    public void testNoArgsConstructorAndSetters() {
        Namespace ns = new Namespace();
        ns.setName("my-ns");
        ns.setConsistency(Namespace.CONSISTENCY.SC);
        ns.setReplicationFactor(3);
        ns.setStorage(Namespace.STORAGE.FILE);
        ns.setTags(Arrays.asList("t1"));

        Assert.assertEquals("my-ns", ns.getName());
        Assert.assertEquals(Namespace.CONSISTENCY.SC, ns.getConsistency());
        Assert.assertEquals(3, ns.getReplicationFactor());
        Assert.assertEquals(Namespace.STORAGE.FILE, ns.getStorage());
    }

    @Test
    public void testEqualsAndHashCode() {
        Namespace ns1 = Namespace.builder()
                .consistency(Namespace.CONSISTENCY.AP)
                .replicationFactor(2)
                .storage(Namespace.STORAGE.IN_MEMORY)
                .name("ns")
                .tags(Arrays.asList("t"))
                .build();
        Namespace ns2 = Namespace.builder()
                .consistency(Namespace.CONSISTENCY.AP)
                .replicationFactor(2)
                .storage(Namespace.STORAGE.IN_MEMORY)
                .name("ns")
                .tags(Arrays.asList("t"))
                .build();
        Assert.assertEquals(ns1, ns2);
        Assert.assertEquals(ns1.hashCode(), ns2.hashCode());
    }
}
