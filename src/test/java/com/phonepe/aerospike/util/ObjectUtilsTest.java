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

import org.junit.Assert;
import org.junit.Test;

public class ObjectUtilsTest {

    @Test
    public void testGetOrDefaultReturnsCurrentWhenNotNull() {
        Assert.assertEquals("hello", ObjectUtils.getOrDefault("hello", "default"));
    }

    @Test
    public void testGetOrDefaultReturnsDefaultWhenNull() {
        Assert.assertEquals("default", ObjectUtils.getOrDefault(null, "default"));
    }

    @Test
    public void testGetOrDefaultWithIntegerValues() {
        Assert.assertEquals(Integer.valueOf(42), ObjectUtils.getOrDefault(42, 0));
        Assert.assertEquals(Integer.valueOf(0), ObjectUtils.getOrDefault(null, 0));
    }

    @Test
    public void testGetOrDefaultBothNull() {
        Assert.assertNull(ObjectUtils.getOrDefault(null, null));
    }
}
