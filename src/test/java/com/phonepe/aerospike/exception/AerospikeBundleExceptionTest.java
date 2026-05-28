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

package com.phonepe.aerospike.exception;

import org.junit.Assert;
import org.junit.Test;

public class AerospikeBundleExceptionTest {

    @Test
    public void testExceptionCreation() {
        AerospikeBundleException ex = new AerospikeBundleException(
                ResponseCode.VALIDATION_ERROR, "test message");
        Assert.assertEquals(ResponseCode.VALIDATION_ERROR, ex.getResponseCode());
        Assert.assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testExceptionIsRuntimeException() {
        AerospikeBundleException ex = new AerospikeBundleException(
                ResponseCode.SECONDARY_CLUSTER_OP_FAILED, "op failed");
        Assert.assertTrue(ex instanceof RuntimeException);
    }

    @Test
    public void testAllResponseCodes() {
        for (ResponseCode code : ResponseCode.values()) {
            AerospikeBundleException ex = new AerospikeBundleException(code, code.name());
            Assert.assertEquals(code, ex.getResponseCode());
            Assert.assertEquals(code.name(), ex.getMessage());
        }
    }

    @Test
    public void testResponseCodeValues() {
        ResponseCode[] codes = ResponseCode.values();
        Assert.assertEquals(5, codes.length);
        Assert.assertNotNull(ResponseCode.valueOf("VALIDATION_ERROR"));
        Assert.assertNotNull(ResponseCode.valueOf("SECONDARY_CLUSTER_OP_FAILED"));
        Assert.assertNotNull(ResponseCode.valueOf("SECONDARY_CLUSTER_PUT_ATTEMPT_FAILED"));
        Assert.assertNotNull(ResponseCode.valueOf("SECONDARY_CLUSTER_DELETE_ATTEMPT_FAILED"));
        Assert.assertNotNull(ResponseCode.valueOf("AEROSPIKE_OPERATION_NOT_SUPPORTED_FOR_DUAL_MODE"));
    }
}
