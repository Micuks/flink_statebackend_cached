/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;

import com.esotericsoftware.kryo.Kryo;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests that Kryo reference tracking is configurable via system property. */
public class KryoSerializerReferencesTest {

    @Test
    public void testReferencesDisabledByDefault() {
        // Clear the system property to ensure default behavior
        System.clearProperty("flink.kryo.references.enabled");

        ExecutionConfig config = new ExecutionConfig();
        KryoSerializer<String> serializer = new KryoSerializer<>(String.class, config);

        // Force Kryo initialization via the @VisibleForTesting accessor
        Kryo kryo = serializer.getKryo();
        assertFalse("Kryo references should be disabled by default", kryo.getReferences());
    }

    @Test
    public void testReferencesCanBeEnabled() {
        System.setProperty("flink.kryo.references.enabled", "true");
        try {
            ExecutionConfig config = new ExecutionConfig();
            KryoSerializer<String> serializer = new KryoSerializer<>(String.class, config);

            Kryo kryo = serializer.getKryo();
            assertTrue(
                    "Kryo references should be enabled when property is set",
                    kryo.getReferences());
        } finally {
            System.clearProperty("flink.kryo.references.enabled");
        }
    }
}
