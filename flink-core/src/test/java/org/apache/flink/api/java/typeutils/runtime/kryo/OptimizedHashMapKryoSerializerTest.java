package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import static org.junit.Assert.*;

public class OptimizedHashMapKryoSerializerTest {

    @Test
    public void testStringLongMapRoundTrip() {
        Kryo kryo = new Kryo();
        OptimizedHashMapKryoSerializer serializer = new OptimizedHashMapKryoSerializer();

        HashMap<String, Long> original = new HashMap<>();
        original.put("key1", 100L);
        original.put("key2", 200L);
        original.put("key3", Long.MAX_VALUE);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        serializer.write(kryo, output, original);
        output.close();

        Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
        HashMap result = serializer.read(kryo, input, HashMap.class);

        assertEquals(original, result);
    }

    @Test
    public void testGenericMapRoundTrip() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        OptimizedHashMapKryoSerializer serializer = new OptimizedHashMapKryoSerializer();

        HashMap<Integer, String> original = new HashMap<>();
        original.put(1, "one");
        original.put(2, "two");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        serializer.write(kryo, output, original);
        output.close();

        Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
        HashMap result = serializer.read(kryo, input, HashMap.class);

        assertEquals(original, result);
    }

    @Test
    public void testEmptyMapRoundTrip() {
        Kryo kryo = new Kryo();
        OptimizedHashMapKryoSerializer serializer = new OptimizedHashMapKryoSerializer();

        HashMap<String, Long> original = new HashMap<>();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        serializer.write(kryo, output, original);
        output.close();

        Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
        HashMap result = serializer.read(kryo, input, HashMap.class);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testCopy() {
        Kryo kryo = new Kryo();
        OptimizedHashMapKryoSerializer serializer = new OptimizedHashMapKryoSerializer();

        HashMap<String, Long> original = new HashMap<>();
        original.put("a", 1L);

        HashMap copy = serializer.copy(kryo, original);
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }
}
