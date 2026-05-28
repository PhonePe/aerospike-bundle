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

package com.phonepe.aerospike.interceptors;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.Statement;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class AerospikeInterceptorContextTest {

    private static final String NAMESPACE = "namespace";
    private static final String SET = "set";
    private static final String KEY = "key";
    private static final AerospikeOperation OPERATION = AerospikeOperation.READ;
    private static final Key AEROSPIKE_KEY = new Key(NAMESPACE, SET, KEY);
    private static final Bin BIN = new Bin("binName", "binValue");

    @Test
    public void testGetInterceptorContextOperation() {
        val response = AerospikeInterceptorContext.getInterceptorContext(AerospikeOperation.READ);
        Assert.assertNull(response.getNamespace());
        Assert.assertNull(response.getSetName());
        Assert.assertEquals(AerospikeOperation.READ, response.getOperation());
        Assert.assertNull(response.getRequestInfo().getKeys());
        Assert.assertNull(response.getRequestInfo().getBins());
    }

    @Test
    public void testGetInterceptorContextKeyOperation() {
        val response = AerospikeInterceptorContext.getInterceptorContext(AEROSPIKE_KEY, OPERATION);
        Assert.assertEquals(NAMESPACE, response.getNamespace());
        Assert.assertEquals(SET, response.getSetName());
        Assert.assertEquals(OPERATION, response.getOperation());
        Assert.assertEquals(1, response.getRequestInfo().getKeys().length);
        Assert.assertEquals(KEY, response.getRequestInfo().getKeys()[0]);
    }

    @Test
    public void testGetInterceptorContextKeyOperationBins() {
        val response = AerospikeInterceptorContext.getInterceptorContext(AEROSPIKE_KEY, OPERATION, BIN);
        Assert.assertEquals(NAMESPACE, response.getNamespace());
        Assert.assertEquals(SET, response.getSetName());
        Assert.assertEquals(OPERATION, response.getOperation());
        Assert.assertEquals(1, response.getRequestInfo().getKeys().length);
        Assert.assertEquals(KEY, response.getRequestInfo().getKeys()[0]);
        Assert.assertEquals(1, response.getRequestInfo().getBins().length);
        Assert.assertEquals(BIN, response.getRequestInfo().getBins()[0]);
    }

    @Test
    public void testGetInterceptorContextStatementOperation() {
        val response = AerospikeInterceptorContext.getInterceptorContext(new Statement(), OPERATION);
        Assert.assertNull(response.getNamespace());
        Assert.assertNull(response.getSetName());
        Assert.assertEquals(OPERATION, response.getOperation());
        Assert.assertNull(response.getRequestInfo().getKeys());
        Assert.assertNull(response.getRequestInfo().getBins());
    }

    @Test
    public void testGetInterceptorContextKeysOperation() {
        val response = AerospikeInterceptorContext.getInterceptorContext(new Key[]{AEROSPIKE_KEY}, OPERATION);
        Assert.assertEquals(NAMESPACE, response.getNamespace());
        Assert.assertEquals(SET, response.getSetName());
        Assert.assertEquals(OPERATION, response.getOperation());
        Assert.assertEquals(1, response.getRequestInfo().getKeys().length);
        Assert.assertEquals(KEY, response.getRequestInfo().getKeys()[0]);
    }

    @Test
    public void testGetInterceptorContextNamespaceSetOperation() {
        val response = AerospikeInterceptorContext.getInterceptorContext(NAMESPACE, SET, OPERATION);
        Assert.assertEquals(NAMESPACE, response.getNamespace());
        Assert.assertEquals(SET, response.getSetName());
        Assert.assertEquals(OPERATION, response.getOperation());
        Assert.assertNull(response.getRequestInfo().getKeys());
        Assert.assertNull(response.getRequestInfo().getBins());
    }
}