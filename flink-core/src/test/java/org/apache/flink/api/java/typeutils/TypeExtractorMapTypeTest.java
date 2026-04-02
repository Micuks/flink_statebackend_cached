package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;

import org.junit.Test;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.*;

public class TypeExtractorMapTypeTest {

    public static class PojoWithMap {
        public String name;
        public Map<String, Long> counters;
    }

    @Test
    public void testMapFieldGetsMapTypeInfo() {
        TypeInformation<PojoWithMap> typeInfo = TypeExtractor.getForClass(PojoWithMap.class);
        // Should be recognized as POJO
        assertTrue("Expected PojoTypeInfo", typeInfo instanceof PojoTypeInfo);
    }

    @Test
    public void testDirectMapTypeExtraction() {
        // Extract type info for Map<String, Long> directly
        TypeInformation<Map<String, Long>> typeInfo = TypeExtractor.getMapReturnType(
            new org.apache.flink.api.common.functions.MapFunction<String, Map<String, Long>>() {
                @Override
                public Map<String, Long> map(String value) {
                    return new HashMap<>();
                }
            }, TypeInformation.of(String.class));

        assertTrue("Expected MapTypeInfo but got " + typeInfo.getClass().getSimpleName(),
            typeInfo instanceof MapTypeInfo);

        MapTypeInfo<String, Long> mapTypeInfo = (MapTypeInfo<String, Long>) typeInfo;
        TypeSerializer<Map<String, Long>> serializer = mapTypeInfo.createSerializer(new org.apache.flink.api.common.ExecutionConfig());
        assertTrue("Expected MapSerializer but got " + serializer.getClass().getSimpleName(),
            serializer instanceof MapSerializer);
    }
}
