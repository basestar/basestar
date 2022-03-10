package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPropertyMapping {

    @Test
    void testSimpleMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleString"), "test")
                .build();

        final PropertyMapping<String> mapping = new PropertyMapping.Simple<>("simpleString");
        assertEquals("test", mapping.fromSQLValues(Name.of(), values));
        assertEquals(values, mapping.toSQLValues(Name.of(), "test"));
    }

    @Test
    void testFlattenedStructMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleStruct", "key"), "test1")
                .put(Name.of("simpleStruct", "value"), "test2")
                .build();

        final Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("key", "test1")
                .put("value", "test2")
                .build();

        final PropertyMapping<Map<String, Object>> mapping = new PropertyMapping.FlattenedStruct("simpleStruct", ImmutableList.of(
                new PropertyMapping.Simple<>("key"),
                new PropertyMapping.Simple<>("value")));
        assertEquals(expected, mapping.fromSQLValues(Name.of(), values));
        assertEquals(values, mapping.toSQLValues(Name.of(), expected));
    }

    @Test
    void testFlattenedRefMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleRef", "id"), "test1")
                .put(Name.of("simpleRef", "version"), 1L)
                .build();

        final Map<String, Object> expected = ImmutableMap.<String, Object>builder()
                .put("id", "test1")
                .put("version", 1L)
                .build();

        final PropertyMapping<Map<String, Object>> mapping = new PropertyMapping.FlattenedStruct("simpleRef", ImmutableList.of(
                new PropertyMapping.Simple<>("id"),
                new PropertyMapping.Simple<>("version")));
        assertEquals(expected, mapping.fromSQLValues(Name.of(), values));
        assertEquals(values, mapping.toSQLValues(Name.of(), expected));
    }
}
