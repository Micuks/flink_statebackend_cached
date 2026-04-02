/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.HashMap;
import java.util.Map;

/**
 * Optimized Kryo serializer for HashMap that avoids writeClassAndObject overhead
 * for the common Map<String, Long> pattern. Falls back to entry-level class writing
 * for other map types.
 */
public class OptimizedHashMapKryoSerializer extends Serializer<HashMap> {

    // Type tags for fast-path serialization
    private static final byte TAG_STRING_LONG = 1;
    private static final byte TAG_GENERIC = 0;

    @Override
    public void write(Kryo kryo, Output output, HashMap map) {
        output.writeVarInt(map.size(), true);

        if (map.isEmpty()) {
            output.writeByte(TAG_GENERIC);
            return;
        }

        // Detect Map<String, Long> pattern from first entry
        Map.Entry<?, ?> firstEntry = (Map.Entry<?, ?>) map.entrySet().iterator().next();
        if (firstEntry.getKey() instanceof String && firstEntry.getValue() instanceof Long) {
            output.writeByte(TAG_STRING_LONG);
            for (Object entry : map.entrySet()) {
                Map.Entry<String, Long> e = (Map.Entry<String, Long>) entry;
                output.writeString(e.getKey());
                output.writeLong(e.getValue());
            }
        } else {
            output.writeByte(TAG_GENERIC);
            for (Object entry : map.entrySet()) {
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
                kryo.writeClassAndObject(output, e.getKey());
                kryo.writeClassAndObject(output, e.getValue());
            }
        }
    }

    @Override
    public HashMap read(Kryo kryo, Input input, Class<HashMap> type) {
        int size = input.readVarInt(true);
        HashMap map = new HashMap<>(Math.max((int) (size / 0.75f) + 1, 16));

        if (size == 0) {
            input.readByte(); // consume tag
            return map;
        }

        byte tag = input.readByte();
        if (tag == TAG_STRING_LONG) {
            for (int i = 0; i < size; i++) {
                String key = input.readString();
                long value = input.readLong();
                map.put(key, value);
            }
        } else {
            for (int i = 0; i < size; i++) {
                Object key = kryo.readClassAndObject(input);
                Object value = kryo.readClassAndObject(input);
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public HashMap copy(Kryo kryo, HashMap original) {
        return new HashMap<>(original);
    }
}
