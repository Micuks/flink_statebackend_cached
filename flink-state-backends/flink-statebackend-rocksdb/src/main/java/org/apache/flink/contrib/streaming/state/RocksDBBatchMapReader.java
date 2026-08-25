/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state;

import java.util.List;
import org.apache.flink.annotation.Internal;

/** Internal capability exposed by RocksDB {@code MapState} for exact user-key batch reads. */
@Internal
public interface RocksDBBatchMapReader<UK> {

    /**
     * Returns raw null-sensitive MapState values for the current outer key and namespace.
     *
     * <p>The returned list must preserve input order and size. A missing entry is represented by
     * {@code null}; a present value is the exact serialized RocksDB value, including MapState's
     * leading null marker.
     */
    List<byte[]> getSerializedValuesByUserKeys(List<UK> userKeys) throws Exception;
}
