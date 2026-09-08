/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import java.io.Serializable;
import java.util.Objects;

/** Configuration for the standalone JNI MapState snapshot cache. */
public final class NativeMapSnapshotOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final NativeMapSnapshotOptions DISABLED =
            new NativeMapSnapshotOptions(false, false, false, "", false);

    private final boolean enabled;
    private final boolean classifierEnabled;
    private final boolean removeHintEnabled;
    private final String libraryPath;
    private final boolean ownedKeyReuseEnabled;

    public NativeMapSnapshotOptions(
            boolean enabled,
            boolean classifierEnabled,
            boolean removeHintEnabled,
            String libraryPath) {
        this(enabled, classifierEnabled, removeHintEnabled, libraryPath, false);
    }

    public NativeMapSnapshotOptions(
            boolean enabled,
            boolean classifierEnabled,
            boolean removeHintEnabled,
            String libraryPath,
            boolean ownedKeyReuseEnabled) {
        if (classifierEnabled) {
            throw new IllegalArgumentException(
                    "Native snapshot classifier is unavailable without RocksDB backend changes");
        }
        this.enabled = enabled;
        this.classifierEnabled = classifierEnabled;
        this.removeHintEnabled = removeHintEnabled;
        this.libraryPath = Objects.requireNonNull(libraryPath, "libraryPath");
        this.ownedKeyReuseEnabled = ownedKeyReuseEnabled;
    }

    public static NativeMapSnapshotOptions disabled() {
        return DISABLED;
    }

    public boolean enabled() {
        return enabled;
    }

    public boolean classifierEnabled() {
        return classifierEnabled;
    }

    public boolean removeHintEnabled() {
        return removeHintEnabled;
    }

    public String libraryPath() {
        return libraryPath;
    }

    public boolean ownedKeyReuseEnabled() {
        return ownedKeyReuseEnabled;
    }
}
