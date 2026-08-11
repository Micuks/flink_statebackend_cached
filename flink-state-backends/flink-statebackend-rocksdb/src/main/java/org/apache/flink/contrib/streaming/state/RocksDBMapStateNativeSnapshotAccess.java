/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;

/** Narrow native access used to classify the current RocksDB MapState key/namespace prefix. */
@Internal
public interface RocksDBMapStateNativeSnapshotAccess {

    byte[] serializeCurrentKeyNamespacePrefix();

    long getDbNativeHandle();

    long getColumnFamilyNativeHandle();

    long getReadOptionsNativeHandle();

    int getKeyGroupPrefixBytes();
}
