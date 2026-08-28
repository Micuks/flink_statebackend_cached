/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;

/**
 * Internal key-selector capability for a key view that is valid only until the next invocation.
 *
 * <p>The ordinary {@link KeySelector#getKey(Object)} contract returns a key that may be retained by
 * the runtime. Some generated table selectors therefore have to copy their reusable projection on
 * every record. A consumer that hashes or compares the key before returning can use this method to
 * avoid that copy, but it must never store the returned reference. Implementations must remain
 * deterministic and must produce a key equal to {@code getKey(value)}.
 */
@Internal
public interface TransientKeySelector<IN, KEY> extends KeySelector<IN, KEY> {

    /** Returns a non-retainable key view for immediate hash/equality use. */
    KEY getTransientKey(IN value) throws Exception;
}
